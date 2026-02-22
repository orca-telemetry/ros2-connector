const std = @import("std");
const c = @import("c.zig").c;
const ah = @import("c.zig").ah;
const schema = @import("schema.zig");
const cb = @import("column_buffer.zig");
const tb = @import("topic_buffer.zig");

const FlatField = schema.FlatField;
const FieldType = schema.FieldType;
const TopicBuffer = tb.TopicBuffer;
const FLUSH_THRESHOLD = cb.FLUSH_THRESHOLD;
const IPCWriterError = error{ NanoarrowArrayViewInitFailed, NanoarrowArrayViewSetFailed };

/// One IPC stream writer per topic.
/// Owns the output file and nanoarrow writer state.
pub const IpcWriter = struct {
    file: std.fs.File,
    /// Nanoarrow IPC writer — wraps the file fd
    writer: c.ArrowIpcWriter,
    /// Nanoarrow schema — built once from FlatFields at init
    arrow_schema: c.ArrowSchema,
    /// Scratch buffers for drain — one per column, allocated at init
    drain_bufs: [][]u8,
    /// Number of columns
    n_cols: usize,
    allocator: std.mem.Allocator,

    /// Open an Arrow IPC stream file for the given topic.
    /// `fields` is the flattened schema from schema.zig.
    /// `out_dir` is the directory to write files into.
    /// All internal allocations use `allocator` (expected: startup arena).
    pub fn init(
        allocator: std.mem.Allocator,
        out_dir: std.fs.Dir,
        topic: *const TopicBuffer,
    ) !IpcWriter {
        // Sanitize topic name for use as filename: replace '/' with '_'
        const safe_name = try sanitizeTopicName(allocator, topic.topic_name);
        const filename = try std.fmt.allocPrint(allocator, "{s}.arrows", .{safe_name});

        const file = try out_dir.createFile(filename, .{ .truncate = true });
        errdefer file.close();

        // Build Arrow schema from flat fields
        var arrow_schema: c.ArrowSchema = undefined;
        try buildArrowSchema(&arrow_schema, topic.columns);

        // Init nanoarrow IPC writer
        var writer: c.ArrowIpcWriter = undefined;
        if (c.ArrowIpcWriterInit(&writer, null) != 0) {
            return error.NanoarrowWriterInitFailed;
        }
        errdefer _ = c.ArrowIpcWriterReset(&writer);

        // Wrap the fd in a FILE* — ArrowIpcOutputStreamInitFile expects a C FILE*
        // do_close=0 because we manage the file lifetime ourselves
        const file_ptr = c.fdopen(file.handle, "wb") orelse return error.FdopenFailed;
        var output: c.ArrowIpcOutputStream = undefined;
        if (c.ArrowIpcOutputStreamInitFile(&output, file_ptr, 0) != 0) {
            return error.NanoarrowOutputStreamInitFailed;
        }

        var err: c.ArrowError = undefined;
        if (c.ArrowIpcWriterStartFile(&writer, &err) != 0) {
            std.debug.print("ArrowIpcWriterStartFile error: {s}\n", .{err.message});
            return error.NanoarrowWriterStartFailed;
        }

        // Pre-allocate drain buffers — one per column, sized for FLUSH_THRESHOLD elements
        const drain_bufs = try allocator.alloc([]u8, topic.columns.len);
        for (topic.columns, 0..) |col, i| {
            const buf_size = FLUSH_THRESHOLD * col.field_type.stride();
            drain_bufs[i] = try allocator.alloc(u8, buf_size);
        }

        return .{
            .file = file,
            .writer = writer,
            .arrow_schema = arrow_schema,
            .drain_bufs = drain_bufs,
            .n_cols = topic.columns.len,
            .allocator = allocator,
        };
    }

    /// Drain the TopicBuffer and write one Arrow IPC record batch.
    pub fn flush(self: *IpcWriter, topic: *TopicBuffer) !void { // FIXME: use error type to robustly type this with the types defined at top of file
        const n_rows = try topic.drain(self.drain_bufs);
        if (n_rows == 0) return;

        // Build an ArrowArray (record batch) from drain buffers
        var array: c.ArrowArray = undefined;
        var err: c.ArrowError = undefined;

        if (c.ArrowArrayInitFromSchema(&array, &self.arrow_schema, &err) != 0) {
            std.debug.print("ArrowArrayInitFromSchema: {s}\n", .{err.message});
            return error.NanoarrowArrayInitFailed;
        }
        defer _ = c.ArrowArrayRelease(&array);

        // if (c.ArrowArrayStartAppending(&array) != 0) {
        //     return error.NanoarrowArrayStartFailed;
        // }

        // Populate each child array (column) from drain buffer
        for (topic.columns, 0..) |col, i| {
            const buf = self.drain_bufs[i][0 .. n_rows * col.field_type.stride()];

            if (col.field_type == .string) {
                try appendStringColumn(&array, buf, n_rows, col.field_type.stride(), &err);
            } else {
                try appendPrimitiveColumn(&array, buf, n_rows, col.field_type.stride(), &err);
            }
        }

        if (c.ArrowArrayFinishBuilding(&array, c.NANOARROW_VALIDATION_LEVEL_DEFAULT, &err) != 0) {
            std.debug.print("ArrowArrayFinishBuilding: {s}\n", .{err.message});
            return error.NanoarrowArrayFinishFailed;
        }

        array.length = @intCast(n_rows);

        // Build an ArrowArrayView from the finished ArrowArray
        var array_view: c.ArrowArrayView = undefined;
        if (c.ArrowArrayViewInitFromSchema(&array_view, &self.arrow_schema, &err) != 0) {
            std.debug.print("ArrowArrayViewInitFromSchema: {s}\n", .{err.message});
            return error.NanoarrowArrayViewInitFailed;
        }
        defer c.ArrowArrayViewReset(&array_view);

        if (c.ArrowArrayViewSetArray(&array_view, &array, &err) != 0) {
            std.debug.print("ArrowArrayViewSetArray: {s}\n", .{err.message});
            return error.NanoarrowArrayViewSetFailed;
        }

        if (c.ArrowIpcWriterWriteArrayView(&self.writer, &array_view, &err) != 0) {
            std.debug.print("ArrowIpcWriterWriteArrayView: {s}\n", .{err.message});
            return error.NanoarrowWriteBatchFailed;
        }
    }

    /// Write the IPC EOS marker and close the file.
    pub fn finish(self: *IpcWriter) void {
        var err: c.ArrowError = undefined;
        _ = c.ArrowIpcWriterFinalizeFile(&self.writer, &err);
        _ = c.ArrowIpcWriterReset(&self.writer);
        c.ArrowSchemaRelease(&self.arrow_schema);
        self.file.close();
    }
};

/// A pool of IpcWriters, one per topic.
pub const IpcWriterPool = struct {
    writers: []IpcWriter,

    pub fn init(
        allocator: std.mem.Allocator,
        out_dir: std.fs.Dir,
        topic_pool: *tb.TopicBufferPool,
    ) !IpcWriterPool {
        const writers = try allocator.alloc(IpcWriter, topic_pool.buffers.len);
        for (topic_pool.buffers, 0..) |*topic, i| {
            writers[i] = try IpcWriter.init(allocator, out_dir, topic);
        }
        return .{ .writers = writers };
    }

    pub fn flushAll(self: *IpcWriterPool, topic_pool: *tb.TopicBufferPool) !void {
        for (self.writers, topic_pool.buffers) |*writer, *topic| {
            if (topic.needsFlush()) {
                try writer.flush(topic);
            }
        }
    }

    /// Flush all topics regardless of threshold — used on shutdown to drain
    /// any partial batches that haven't hit FLUSH_THRESHOLD yet.
    pub fn forceFlushAll(self: *IpcWriterPool, topic_pool: *tb.TopicBufferPool) !void {
        for (self.writers, topic_pool.buffers) |*writer, *topic| {
            if (topic.len() > 0) {
                try writer.flush(topic);
            }
        }
    }

    pub fn finish(self: *IpcWriterPool) void {
        for (self.writers) |*w| w.finish();
    }
};

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn buildArrowSchema(out: *c.ArrowSchema, columns: []const cb.ColumnBuffer) !void {
    // Top-level schema is a struct with one child per column
    if (c.ArrowSchemaInitFromType(out, c.NANOARROW_TYPE_STRUCT) != 0) {
        return error.NanoarrowSchemaInitFailed;
    }
    if (c.ArrowSchemaAllocateChildren(out, @intCast(columns.len)) != 0) {
        return error.NanoarrowSchemaAllocFailed;
    }

    for (columns, 0..) |col, i| {
        const child = out.children[i];
        if (c.ArrowSchemaSetType(child, arrowTypeId(col.field_type)) != 0) {
            return error.NanoarrowSchemaSetTypeFailed;
        }
        if (c.ArrowSchemaSetName(child, col.name.ptr) != 0) {
            return error.NanoarrowSchemaSetNameFailed;
        }
    }
}

fn arrowTypeId(ft: FieldType) c.ArrowType {
    return switch (ft) {
        .int8 => c.NANOARROW_TYPE_INT8,
        .uint8, .byte => c.NANOARROW_TYPE_UINT8,
        .char => c.NANOARROW_TYPE_INT8,
        .boolean => c.NANOARROW_TYPE_BOOL,
        .int16 => c.NANOARROW_TYPE_INT16,
        .uint16 => c.NANOARROW_TYPE_UINT16,
        .int32 => c.NANOARROW_TYPE_INT32,
        .uint32 => c.NANOARROW_TYPE_UINT32,
        .float32 => c.NANOARROW_TYPE_FLOAT,
        .int64 => c.NANOARROW_TYPE_INT64,
        .uint64 => c.NANOARROW_TYPE_UINT64,
        .float64, .long_double => c.NANOARROW_TYPE_DOUBLE,
        .string => c.NANOARROW_TYPE_BINARY, // fixed-width binary slot
    };
}

fn appendPrimitiveColumn(
    child: *c.ArrowArray,
    buf: []const u8,
    n_rows: usize,
    stride: usize,
    err: *c.ArrowError,
) !void {
    // Get the data buffer and memcpy directly — no per-element append needed
    const data_buf = c.ArrowArrayBuffer(child, 1); // buffer 1 = data
    if (c.ArrowBufferResize(data_buf, @intCast(buf.len), 0) != 0) {
        return error.NanoarrowBufferResizeFailed;
    }
    @memcpy(data_buf.*.data[0..buf.len], buf);
    child.length = @intCast(n_rows);
    _ = stride;
    _ = err;
}

fn appendStringColumn(
    child: *c.ArrowArray,
    buf: []const u8,
    n_rows: usize,
    stride: usize,
    err: *c.ArrowError,
) !void {
    // For fixed-width binary slots we strip trailing zero padding per element
    // and append as variable-length binary via nanoarrow's append API
    var i: usize = 0;
    while (i < n_rows) : (i += 1) {
        const slot = buf[i * stride .. (i + 1) * stride];
        // Find actual string length (first null terminator)
        const actual_len = std.mem.indexOfScalar(u8, slot, 0) orelse stride;
        const view = c.ArrowBufferView{
            .data = .{ .as_uint8 = slot.ptr },
            .size_bytes = @intCast(actual_len),
        };
        if (c.arrow_array_append_bytes(child, view) != 0) {
            std.debug.print("ArrowArrayAppendBytes: {s}\n", .{err.message});
            return error.NanoarrowAppendFailed;
        }
    }
}

fn sanitizeTopicName(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const out = try allocator.dupe(u8, name);
    for (out) |*ch| {
        if (ch.* == '/') ch.* = '_';
    }
    // Strip leading underscore from leading slash
    return if (out[0] == '_') out[1..] else out;
}
