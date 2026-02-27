const std = @import("std");
const mcap_mod = @import("mcap.zig");
const tb = @import("topic_buffer.zig");

const MessageBuffer = tb.MessageBuffer;
const MessageBufferPool = tb.MessageBufferPool;

// ---------------------------------------------------------------------------
// McapFileWriter — wraps mcap.Writer (in-memory serializer) + std.fs.File
// ---------------------------------------------------------------------------
// Pattern: serialize a record into the Writer's ArrayList buffer, flush bytes
// to the file, and clear the buffer for reuse.

const McapFileWriter = struct {
    w: mcap_mod.Writer,
    file: std.fs.File,

    fn open(path: [:0]const u8) !McapFileWriter {
        const file = try std.fs.cwd().createFileZ(path, .{});
        return .{ .w = mcap_mod.Writer.init(), .file = file };
    }

    /// Flush the in-memory buffer to the file and clear for reuse.
    fn flush(self: *McapFileWriter) !void {
        const items = self.w.buf.items;
        if (items.len > 0) {
            try self.file.writeAll(items);
            self.w.buf.clearRetainingCapacity();
        }
    }

    fn writeMagic(self: *McapFileWriter, allocator: std.mem.Allocator) !void {
        try self.w.writeMagic(allocator);
        try self.flush();
    }

    fn writeHeader(self: *McapFileWriter, allocator: std.mem.Allocator, h: mcap_mod.Header) !void {
        try self.w.writeHeader(allocator, h);
        try self.flush();
    }

    fn writeSchema(self: *McapFileWriter, allocator: std.mem.Allocator, s: mcap_mod.Schema) !void {
        try self.w.writeSchema(allocator, s);
        try self.flush();
    }

    fn writeChannel(self: *McapFileWriter, allocator: std.mem.Allocator, c: mcap_mod.Channel) !void {
        try self.w.writeChannel(allocator, c);
        try self.flush();
    }

    fn writeMessage(self: *McapFileWriter, allocator: std.mem.Allocator, m: mcap_mod.Message) !void {
        try self.w.writeMessage(allocator, m);
        try self.flush();
    }

    fn writeMetadata(self: *McapFileWriter, allocator: std.mem.Allocator, m: mcap_mod.Metadata) !void {
        try self.w.writeMetadata(allocator, m);
        try self.flush();
    }

    fn writeDataEnd(self: *McapFileWriter, allocator: std.mem.Allocator, de: mcap_mod.DataEnd) !void {
        try self.w.writeDataEnd(allocator, de);
        try self.flush();
    }

    fn writeFooter(self: *McapFileWriter, allocator: std.mem.Allocator, f: mcap_mod.Footer) !void {
        try self.w.writeFooter(allocator, f);
        try self.flush();
    }
};

// ---------------------------------------------------------------------------
// McapWriter — one writer per topic (one schema + channel in the MCAP file)
// ---------------------------------------------------------------------------

pub const McapWriter = struct {
    channel_id: u16,
    schema_id: u16,
    sequence: u32,
    topic_name: []const u8,
    type_name: []const u8,
    topic_schema: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        fw: *McapFileWriter,
        topic: *const MessageBuffer,
        schema_id: u16,
        channel_id: u16,
    ) !McapWriter {
        try writeSchemaAndChannel(fw, allocator, topic.type_name, topic.topic_name, topic.topic_schema, schema_id, channel_id);

        return .{
            .channel_id = channel_id,
            .schema_id = schema_id,
            .sequence = 0,
            .topic_name = topic.topic_name,
            .type_name = topic.type_name,
            .topic_schema = topic.topic_schema,
        };
    }

    /// Re-register this writer's schema + channel on a new file after rotation.
    pub fn reregister(self: *McapWriter, allocator: std.mem.Allocator, fw: *McapFileWriter) !void {
        try writeSchemaAndChannel(fw, allocator, self.type_name, self.topic_name, self.topic_schema, self.schema_id, self.channel_id);
    }

    /// Write a single serialized message to the MCAP file.
    pub fn writeMessage(self: *McapWriter, fw: *McapFileWriter, allocator: std.mem.Allocator, log_time_ns: u64, data: []const u8) !void {
        self.sequence += 1;
        try fw.writeMessage(allocator, .{
            .channel_id = self.channel_id,
            .sequence = self.sequence,
            .log_time = log_time_ns,
            .publish_time = log_time_ns,
            .data = data,
        });
    }

    /// Drain all messages from the buffer and write each as a separate MCAP message.
    /// Returns the total bytes written.
    pub fn flush(self: *McapWriter, fw: *McapFileWriter, buf: *MessageBuffer, allocator: std.mem.Allocator) !u64 {
        const messages = buf.drainAll(allocator);
        if (messages.len == 0) return 0;
        defer allocator.free(messages);

        var bytes: u64 = 0;
        for (messages) |msg| {
            defer allocator.free(msg.data);
            try self.writeMessage(fw, allocator, msg.timestamp_ns, msg.data);
            bytes += msg.data.len;
        }
        return bytes;
    }
};

fn writeSchemaAndChannel(
    fw: *McapFileWriter,
    allocator: std.mem.Allocator,
    type_name: []const u8,
    topic_name: []const u8,
    topic_schema: []const u8,
    schema_id: u16,
    channel_id: u16,
) !void {
    try fw.writeSchema(allocator, .{
        .id = schema_id,
        .name = .{ .str = type_name },
        .encoding = .{ .str = "ros2msg" },
        .data = topic_schema,
    });

    var empty_entries = [_]mcap_mod.TupleStrStr{};
    try fw.writeChannel(allocator, .{
        .id = channel_id,
        .schema_id = schema_id,
        .topic = .{ .str = topic_name },
        .message_encoding = .{ .str = "cdr" },
        .metadata = .{ .entries = &empty_entries },
    });
}

// ---------------------------------------------------------------------------
// McapWriterPool — one shared MCAP file, one McapWriter per topic
// ---------------------------------------------------------------------------

pub const McapWriterPool = struct {
    fw: McapFileWriter,
    writers: []McapWriter,
    file_path: [:0]const u8,
    incomplete_path: [:0]const u8,
    out_dir: []const u8,
    robot_id: []const u8,
    software_version: []const u8,
    max_duration_ns: u64,
    max_size_bytes: u64,
    fsync_interval_ns: u64,
    bytes_written: u64,
    open_time_ns: u64,
    last_fsync_ns: u64,
    sequence: u32,

    pub fn init(
        allocator: std.mem.Allocator,
        out_dir: []const u8,
        robot_id: []const u8,
        software_version: []const u8,
        buf_pool: *tb.MessageBufferPool,
        max_duration_ns: u64,
        max_size_bytes: u64,
        fsync_interval_ns: u64,
    ) !McapWriterPool {
        const now_ns: u64 = @intCast(std.time.nanoTimestamp());

        const filename = try generateFilename(allocator, out_dir, robot_id, now_ns, 0);
        const incomplete = try std.fmt.allocPrintSentinel(
            allocator,
            "{s}.incomplete",
            .{filename},
            0,
        );

        // Open with .incomplete suffix
        var fw = try McapFileWriter.open(incomplete);
        errdefer {
            fw.file.close();
            fw.w.deinit(allocator);
        }

        // Write file preamble: magic + header + session metadata
        try fw.writeMagic(allocator);
        try fw.writeHeader(allocator, .{
            .profile = .{ .str = "ros2" },
            .library = .{ .str = "orca-collector" },
        });
        writeSessionMetadata(allocator, &fw, robot_id, software_version, now_ns, 0);

        // Register one schema + channel per topic
        const writers = try allocator.alloc(McapWriter, buf_pool.buffers.len);
        for (buf_pool.buffers, 0..) |*buf, i| {
            const id: u16 = @intCast(i + 1); // 1-indexed
            writers[i] = try McapWriter.init(allocator, &fw, buf, id, id);
        }

        return .{
            .fw = fw,
            .writers = writers,
            .file_path = filename,
            .incomplete_path = incomplete,
            .out_dir = out_dir,
            .robot_id = robot_id,
            .software_version = software_version,
            .max_duration_ns = max_duration_ns,
            .max_size_bytes = max_size_bytes,
            .fsync_interval_ns = fsync_interval_ns,
            .bytes_written = 0,
            .open_time_ns = now_ns,
            .last_fsync_ns = now_ns,
            .sequence = 0,
        };
    }

    pub fn flushAll(self: *McapWriterPool, buf_pool: *MessageBufferPool, allocator: std.mem.Allocator) !void {
        for (self.writers, buf_pool.buffers) |*writer, *buf| {
            if (buf.needsFlush()) {
                const bytes = try writer.flush(&self.fw, buf, allocator);
                self.bytes_written += bytes;
            }
        }
    }

    /// Drain any remaining messages — call on shutdown.
    pub fn forceFlushAll(self: *McapWriterPool, buf_pool: *MessageBufferPool, allocator: std.mem.Allocator) !void {
        for (self.writers, buf_pool.buffers) |*writer, *buf| {
            if (buf.len() > 0) {
                const bytes = try writer.flush(&self.fw, buf, allocator);
                self.bytes_written += bytes;
            }
        }
    }

    /// Check whether it's time to rotate to a new file.
    pub fn shouldRotate(self: *const McapWriterPool) bool {
        const now_ns: u64 = @intCast(std.time.nanoTimestamp());
        if (self.bytes_written >= self.max_size_bytes) return true;
        if (now_ns - self.open_time_ns >= self.max_duration_ns) return true;
        return false;
    }

    /// Rotate: close current file, open a new one, re-register all channels.
    pub fn rotate(self: *McapWriterPool, allocator: std.mem.Allocator) !void {
        // Close and finalize the current file
        self.closeAndFinalize(allocator);

        // Next sequence
        self.sequence += 1;
        const now_ns: u64 = @intCast(std.time.nanoTimestamp());

        const new_filename = try generateFilename(
            allocator,
            self.out_dir,
            self.robot_id,
            now_ns,
            self.sequence,
        );
        const new_incomplete = try std.fmt.allocPrintSentinel(
            allocator,
            "{s}.incomplete",
            .{new_filename},
            0,
        );

        // Open new file (reuse the Writer buffer)
        self.fw.file = try std.fs.cwd().createFileZ(new_incomplete, .{});

        // Write file preamble
        self.fw.writeMagic(allocator) catch |err| {
            std.log.err("Failed to write magic after rotation: {}", .{err});
            return err;
        };
        self.fw.writeHeader(allocator, .{
            .profile = .{ .str = "ros2" },
            .library = .{ .str = "orca-collector" },
        }) catch |err| {
            std.log.err("Failed to write header after rotation: {}", .{err});
            return err;
        };
        writeSessionMetadata(allocator, &self.fw, self.robot_id, self.software_version, now_ns, self.sequence);

        // Re-register all channels on the new file
        for (self.writers) |*writer| {
            try writer.reregister(allocator, &self.fw);
        }

        self.file_path = new_filename;
        self.incomplete_path = new_incomplete;
        self.bytes_written = 0;
        self.open_time_ns = now_ns;
        self.last_fsync_ns = now_ns;
    }

    /// Check and rotate if needed. Returns true if a rotation occurred.
    pub fn rotateIfNeeded(self: *McapWriterPool, allocator: std.mem.Allocator) !bool {
        if (self.shouldRotate()) {
            const old_path = self.file_path;
            try self.rotate(allocator);
            std.log.info("Rotated MCAP file: {s} -> {s}", .{ old_path, self.file_path });
            return true;
        }
        return false;
    }

    /// Fsync the current .incomplete file if the configured interval has elapsed.
    pub fn periodicFsync(self: *McapWriterPool) void {
        if (self.fsync_interval_ns == 0) return;

        const now_ns: u64 = @intCast(std.time.nanoTimestamp());
        if (now_ns - self.last_fsync_ns < self.fsync_interval_ns) return;

        self.fw.file.sync() catch |err| {
            std.log.err("periodic fsync failed: {}", .{err});
        };

        self.last_fsync_ns = now_ns;
    }

    /// Close the MCAP file, fsync, rename, and write checksum.
    pub fn finish(self: *McapWriterPool, allocator: std.mem.Allocator) void {
        self.closeAndFinalize(allocator);
    }

    fn closeAndFinalize(self: *McapWriterPool, allocator: std.mem.Allocator) void {
        // Write DataEnd record
        self.fw.writeDataEnd(allocator, .{ .data_section_crc32 = 0 }) catch |err| {
            std.log.err("Failed to write DataEnd: {}", .{err});
        };

        // --- Summary section: re-emit Schema + Channel for each topic ---
        const summary_start: u64 = self.fw.file.getPos() catch 0;

        for (self.writers) |writer| {
            self.fw.writeSchema(allocator, .{
                .id = writer.schema_id,
                .name = .{ .str = writer.type_name },
                .encoding = .{ .str = "ros2msg" },
                .data = writer.topic_schema,
            }) catch |err| {
                std.log.err("Failed to write summary Schema: {}", .{err});
            };
        }

        for (self.writers) |writer| {
            var empty_entries = [_]mcap_mod.TupleStrStr{};
            self.fw.writeChannel(allocator, .{
                .id = writer.channel_id,
                .schema_id = writer.schema_id,
                .topic = .{ .str = writer.topic_name },
                .message_encoding = .{ .str = "cdr" },
                .metadata = .{ .entries = &empty_entries },
            }) catch |err| {
                std.log.err("Failed to write summary Channel: {}", .{err});
            };
        }

        // Write Footer with summary offset
        self.fw.writeFooter(allocator, .{
            .ofs_summary_section = summary_start,
            .ofs_summary_offset_section = 0,
            .summary_crc32 = 0,
        }) catch |err| {
            std.log.err("Failed to write Footer: {}", .{err});
        };
        // Write trailing magic
        self.fw.writeMagic(allocator) catch |err| {
            std.log.err("Failed to write trailing magic: {}", .{err});
        };

        // Fsync and close
        self.fw.file.sync() catch |err| {
            std.log.err("fsync failed for {s}: {}", .{ self.incomplete_path, err });
        };
        self.fw.file.close();

        // Rename .incomplete → final .mcap
        std.fs.cwd().renameZ(self.incomplete_path, self.file_path) catch |err| {
            std.log.err("Failed to rename {s} -> {s}: {}", .{
                self.incomplete_path,
                self.file_path,
                err,
            });
            return;
        };

        // Write SHA-256 checksum file
        writeSha256File(allocator, self.file_path) catch |err| {
            std.log.err("Failed to write SHA-256 for {s}: {}", .{ self.file_path, err });
        };
    }
};

// ---------------------------------------------------------------------------
// Session metadata — written to each MCAP file on open
// ---------------------------------------------------------------------------

fn writeSessionMetadata(
    allocator: std.mem.Allocator,
    fw: *McapFileWriter,
    robot_id: []const u8,
    software_version: []const u8,
    session_ns: u64,
    sequence: u32,
) void {
    writeSessionMetadataImpl(allocator, fw, robot_id, software_version, session_ns, sequence) catch |err| {
        std.log.err("Failed to write session metadata: {}", .{err});
    };
}

fn writeSessionMetadataImpl(
    allocator: std.mem.Allocator,
    fw: *McapFileWriter,
    robot_id: []const u8,
    software_version: []const u8,
    session_ns: u64,
    sequence: u32,
) !void {
    const session_id_str = try std.fmt.allocPrint(allocator, "{d}", .{session_ns});
    const seq_str = try std.fmt.allocPrint(allocator, "{d}", .{sequence});

    var entries = [_]mcap_mod.TupleStrStr{
        .{ .key = .{ .str = "robot_id" }, .value = .{ .str = robot_id } },
        .{ .key = .{ .str = "session_id" }, .value = .{ .str = session_id_str } },
        .{ .key = .{ .str = "software_version" }, .value = .{ .str = software_version } },
        .{ .key = .{ .str = "file_sequence" }, .value = .{ .str = seq_str } },
    };

    try fw.writeMetadata(allocator, .{
        .name = .{ .str = "rdl_session" },
        .metadata = .{ .entries = &entries },
    });
}

// ---------------------------------------------------------------------------
// MCAP file recovery — replaces C++ mcap_recover
// ---------------------------------------------------------------------------

/// Recover an incomplete MCAP file by copying valid records to a new file.
/// Returns 0 for clean recovery, 1 for partial (some data lost), -1 for failure.
pub fn recover(allocator: std.mem.Allocator, src_path: [:0]const u8, dst_path: [:0]const u8) i32 {
    return recoverImpl(allocator, src_path, dst_path) catch -1;
}

fn recoverImpl(allocator: std.mem.Allocator, src_path: [:0]const u8, dst_path: [:0]const u8) !i32 {
    // Read entire source file into memory
    const src_file = try std.fs.cwd().openFileZ(src_path, .{});
    defer src_file.close();
    const file_stat = try src_file.stat();
    const data = try allocator.alloc(u8, file_stat.size);
    defer allocator.free(data);
    const bytes_read = try src_file.readAll(data);
    const file_data = data[0..bytes_read];

    // Validate leading magic
    mcap_mod.validateMagic(file_data) catch return -1;

    // Open destination file
    const dst_file = try std.fs.cwd().createFileZ(dst_path, .{});
    errdefer dst_file.close();

    // Write leading magic
    try dst_file.writeAll(mcap_mod.MAGIC);

    // Copy records from source, stopping at footer or truncation
    var offset: usize = mcap_mod.MAGIC_LEN;
    var clean: bool = true;

    while (offset < file_data.len) {
        const start = offset;
        const record = mcap_mod.Record.parseHeader(file_data, &offset) catch {
            clean = false;
            break;
        };

        if (record.op == .footer) break; // clean end
        if (record.op == .data_end) continue; // skip, we'll write our own

        // Write the raw record bytes (opcode + length + body) directly
        try dst_file.writeAll(file_data[start..offset]);
    }

    // Write finalization: DataEnd + Footer + trailing magic
    var w = mcap_mod.Writer.init();
    defer w.deinit(allocator);

    try w.writeDataEnd(allocator, .{ .data_section_crc32 = 0 });
    try w.writeFooter(allocator, .{
        .ofs_summary_section = 0,
        .ofs_summary_offset_section = 0,
        .summary_crc32 = 0,
    });
    try w.writeMagic(allocator);
    try dst_file.writeAll(w.buf.items);

    try dst_file.sync();
    dst_file.close();

    return if (clean) @as(i32, 0) else @as(i32, 1);
}

// ---------------------------------------------------------------------------
// ISO8601-ish timestamp formatting and filename generation
// ---------------------------------------------------------------------------

/// Format a nanosecond wall-clock timestamp as YYYYMMDD_HHMMSS.
fn formatTimestamp(allocator: std.mem.Allocator, ns: u64) ![:0]const u8 {
    const epoch_secs = std.time.epoch.EpochSeconds{ .secs = ns / std.time.ns_per_s };
    const day = epoch_secs.getEpochDay();
    const year_day = day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_secs = epoch_secs.getDaySeconds();

    return try std.fmt.allocPrintSentinel(
        allocator,
        "{d:0>4}{d:0>2}{d:0>2}_{d:0>2}{d:0>2}{d:0>2}",
        .{
            year_day.year,
            month_day.month.numeric(),
            month_day.day_index + 1,
            day_secs.getHoursIntoDay(),
            day_secs.getMinutesIntoHour(),
            day_secs.getSecondsIntoMinute(),
        },
        0,
    );
}

/// Generate a filename: <out_dir>/<robot_id>_<YYYYMMDD_HHMMSS>_<seq>.mcap
fn generateFilename(
    allocator: std.mem.Allocator,
    out_dir: []const u8,
    robot_id: []const u8,
    timestamp_ns: u64,
    sequence: u32,
) ![:0]const u8 {
    const ts = try formatTimestamp(allocator, timestamp_ns);
    return try std.fmt.allocPrintSentinel(
        allocator,
        "{s}/{s}_{s}_{d:0>4}.mcap",
        .{ out_dir, robot_id, ts, sequence },
        0,
    );
}

// ---------------------------------------------------------------------------
// SHA-256 checksum file — compatible with `sha256sum -c`
// ---------------------------------------------------------------------------

/// Compute SHA-256 of `path` and write `<hex>  <basename>\n` to `<path>.sha256`.
/// The .sha256 file is written atomically (write to .tmp, fsync, rename).
pub fn writeSha256File(allocator: std.mem.Allocator, path: [:0]const u8) !void {
    const file = try std.fs.cwd().openFileZ(path, .{});
    defer file.close();

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buf: [64 * 1024]u8 = undefined;
    while (true) {
        const n = try file.read(&buf);
        if (n == 0) break;
        hasher.update(buf[0..n]);
    }
    const digest = hasher.finalResult();

    // Format hex digest
    var hex: [64]u8 = undefined;
    for (digest, 0..) |byte, i| {
        const lo = "0123456789abcdef";
        hex[i * 2] = lo[byte >> 4];
        hex[i * 2 + 1] = lo[byte & 0x0f];
    }

    // Extract basename from path
    const path_slice: []const u8 = std.mem.sliceTo(path, 0);
    const basename = std.fs.path.basename(path_slice);

    // Write to .sha256.tmp, fsync, rename to .sha256
    const tmp_path = try std.fmt.allocPrintSentinel(allocator, "{s}.sha256.tmp", .{path_slice}, 0);
    defer allocator.free(tmp_path);
    const final_path = try std.fmt.allocPrintSentinel(allocator, "{s}.sha256", .{path_slice}, 0);
    defer allocator.free(final_path);

    const sha_file = try std.fs.cwd().createFileZ(tmp_path, .{});
    errdefer sha_file.close();
    // Write "<hex>  <basename>\n" — sha256sum -c compatible format
    try sha_file.writeAll(&hex);
    try sha_file.writeAll("  ");
    try sha_file.writeAll(basename);
    try sha_file.writeAll("\n");
    try sha_file.sync();
    sha_file.close();

    try std.fs.cwd().renameZ(tmp_path, final_path);
}
