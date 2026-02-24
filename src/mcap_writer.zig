const std = @import("std");
const c = @import("c.zig").c;
const cb = @import("column_buffer.zig");
const tb = @import("topic_buffer.zig");

const TopicBuffer = tb.TopicBuffer;
const FLUSH_THRESHOLD = cb.FLUSH_THRESHOLD;

// ---------------------------------------------------------------------------
// C++ MCAP bridge — implemented in mcap_bridge.cpp
// ---------------------------------------------------------------------------
// We call into a thin C-linkage wrapper around mcap::McapWriter so Zig can
// remain C-ABI only.  The wrapper exposes:
//
//   void* mcap_writer_open(const char* path);
//   void  mcap_writer_close(void* w);
//   uint16_t mcap_writer_add_channel(void* w, const char* topic);
//   int  mcap_writer_write(void* w, uint16_t channel_id,
//                          uint64_t log_time_ns,
//                          const void* data, size_t len);
//
// Compile mcap_bridge.cpp with -DMCAP_IMPLEMENTATION and link it alongside
// this library.
extern "C" fn mcap_writer_open(path: [*:0]const u8) ?*anyopaque;
extern "C" fn mcap_writer_close(w: *anyopaque) void;
extern "C" fn mcap_writer_add_channel(w: *anyopaque, topic: [*:0]const u8) u16;
extern "C" fn mcap_writer_write(
    w: *anyopaque,
    channel_id: u16,
    log_time_ns: u64,
    data: [*]const u8,
    len: usize,
) c_int;

// ---------------------------------------------------------------------------
// McapWriter — one writer per topic
// ---------------------------------------------------------------------------

pub const McapWriter = struct {
    /// Opaque C++ McapWriter handle (shared across all channels in the file).
    /// The *pool* owns this; individual writers just borrow it.
    handle: *anyopaque,
    channel_id: u16,
    /// Scratch buffers for drain — one per column.
    drain_bufs: [][]u8,
    n_cols: usize,
    allocator: std.mem.Allocator,

    /// `handle` is the already-opened mcap writer handle (owned by the pool).
    pub fn init(
        allocator: std.mem.Allocator,
        handle: *anyopaque,
        topic: *const TopicBuffer,
    ) !McapWriter {
        // Register channel — topic name must be null-terminated.
        const topic_z = try allocator.dupeZ(u8, topic.topic_name);
        const channel_id = mcap_writer_add_channel(handle, topic_z.ptr);

        // Pre-allocate drain buffers.
        const drain_bufs = try allocator.alloc([]u8, topic.columns.len);
        for (topic.columns, 0..) |col, i| {
            const buf_size = FLUSH_THRESHOLD * col.field_type.stride();
            drain_bufs[i] = try allocator.alloc(u8, buf_size);
        }

        return .{
            .handle = handle,
            .channel_id = channel_id,
            .drain_bufs = drain_bufs,
            .n_cols = topic.columns.len,
            .allocator = allocator,
        };
    }

    /// Drain the TopicBuffer and write one MCAP message per batch.
    /// Each flush writes all columns concatenated as raw bytes in a single
    /// message.  Adjust the layout here if consumers expect a different
    /// framing (e.g. length-prefixed columns, FlatBuffers, etc.).
    pub fn flush(self: *McapWriter, topic: *TopicBuffer) !void {
        const n_rows = try topic.drain(self.drain_bufs);
        if (n_rows == 0) return;

        // Concatenate all column drain buffers into one payload.
        // Layout: [col0 data | col1 data | ... | colN data]
        // Each column's byte length is n_rows * col.field_type.stride().
        var total: usize = 0;
        for (topic.columns, 0..) |col, i| {
            _ = col;
            total += self.drain_bufs[i].len; // already sized to n_rows*stride by drain()
        }
        // Use a stack-fallback allocator for the scratch concat buffer.
        const payload = try self.allocator.alloc(u8, total);
        defer self.allocator.free(payload);

        var offset: usize = 0;
        for (topic.columns, 0..) |col, i| {
            const col_bytes = n_rows * col.field_type.stride();
            @memcpy(payload[offset .. offset + col_bytes], self.drain_bufs[i][0..col_bytes]);
            offset += col_bytes;
        }

        const log_time_ns = @as(u64, @intCast(std.time.nanoTimestamp()));
        if (mcap_writer_write(
            self.handle,
            self.channel_id,
            log_time_ns,
            payload.ptr,
            payload.len,
        ) != 0) {
            return error.McapWriteFailed;
        }
    }
};

// ---------------------------------------------------------------------------
// McapWriterPool — one McapWriter per topic, one shared file/handle
// ---------------------------------------------------------------------------

pub const McapWriterPool = struct {
    handle: *anyopaque,
    writers: []McapWriter,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        out_path: [:0]const u8,
        topic_pool: *tb.TopicBufferPool,
    ) !McapWriterPool {
        const handle = mcap_writer_open(out_path.ptr) orelse return error.McapOpenFailed;
        errdefer mcap_writer_close(handle);

        const writers = try allocator.alloc(McapWriter, topic_pool.buffers.len);
        for (topic_pool.buffers, 0..) |*topic, i| {
            writers[i] = try McapWriter.init(allocator, handle, topic);
        }

        return .{
            .handle = handle,
            .writers = writers,
            .allocator = allocator,
        };
    }

    pub fn flushAll(self: *McapWriterPool, topic_pool: *tb.TopicBufferPool) !void {
        for (self.writers, topic_pool.buffers) |*writer, *topic| {
            if (topic.needsFlush()) {
                try writer.flush(topic);
            }
        }
    }

    /// Drain any partial batches — call on shutdown.
    pub fn forceFlushAll(self: *McapWriterPool, topic_pool: *tb.TopicBufferPool) !void {
        for (self.writers, topic_pool.buffers) |*writer, *topic| {
            if (topic.len() > 0) {
                try writer.flush(topic);
            }
        }
    }

    pub fn finish(self: *McapWriterPool) void {
        mcap_writer_close(self.handle);
    }
};
