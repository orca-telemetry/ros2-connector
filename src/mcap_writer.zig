const std = @import("std");
const tb = @import("topic_buffer.zig");

const MessageBuffer = tb.MessageBuffer;

// ---------------------------------------------------------------------------
// C++ MCAP bridge — implemented in mcap_bridge.cpp
// ---------------------------------------------------------------------------
// Thin C-linkage wrapper around mcap::McapWriter.
//
//   void* mcap_writer_open(const char* path);
//   void  mcap_writer_close(void* w);
//   uint16_t mcap_writer_add_channel(void* w, const char* topic,
//                                     const char* message_type,
//                                     const char* schema_encoding,
//                                     const char* schema_data);
//   int  mcap_writer_write(void* w, uint16_t channel_id,
//                          uint64_t log_time_ns,
//                          const void* data, size_t len);
//
extern "C" fn mcap_writer_open(path: [*:0]const u8) ?*anyopaque;
extern "C" fn mcap_writer_close(w: *anyopaque) void;
extern "C" fn mcap_writer_add_channel(
    w: *anyopaque,
    topic: [*:0]const u8,
    message_type: [*:0]const u8,
    schema_encoding: [*:0]const u8,
    schema_data: [*:0]const u8,
) u16;
extern "C" fn mcap_writer_write(
    w: *anyopaque,
    channel_id: u16,
    log_time_ns: u64,
    data: [*]const u8,
    len: usize,
) c_int;
extern "C" fn mcap_writer_write_metadata(
    w: *anyopaque,
    name: [*:0]const u8,
    keys: [*]const [*:0]const u8,
    values: [*]const [*:0]const u8,
    count: usize,
) void;

// ---------------------------------------------------------------------------
// McapWriter — one writer per topic (one channel in the MCAP file)
// ---------------------------------------------------------------------------

pub const McapWriter = struct {
    handle: *anyopaque,
    channel_id: u16,
    topic_name: [*:0]const u8,
    type_name: [*:0]const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        handle: *anyopaque,
        topic: *const MessageBuffer,
    ) !McapWriter {
        const topic_z = try allocator.dupeZ(u8, topic.topic_name);
        const type_z = try allocator.dupeZ(u8, topic.type_name);

        const channel_id = mcap_writer_add_channel(
            handle,
            topic_z.ptr,
            type_z.ptr,
            "ros2msg",
            "", // schema_data: empty for now, Phase 2 can populate
        );

        return .{
            .handle = handle,
            .channel_id = channel_id,
            .topic_name = topic_z.ptr,
            .type_name = type_z.ptr,
        };
    }

    /// Re-register this writer's channel on a new MCAP handle after rotation.
    pub fn reregister(self: *McapWriter, new_handle: *anyopaque) void {
        self.handle = new_handle;
        self.channel_id = mcap_writer_add_channel(
            new_handle,
            self.topic_name,
            self.type_name,
            "ros2msg",
            "",
        );
    }

    /// Write a single serialized message to the MCAP file.
    pub fn writeMessage(self: *McapWriter, log_time_ns: u64, data: []const u8) !void {
        if (mcap_writer_write(
            self.handle,
            self.channel_id,
            log_time_ns,
            data.ptr,
            data.len,
        ) != 0) {
            return error.McapWriteFailed;
        }
    }

    /// Drain all messages from the buffer and write each as a separate MCAP message.
    /// Returns the total bytes written.
    pub fn flush(self: *McapWriter, buf: *MessageBuffer, allocator: std.mem.Allocator) !u64 {
        const messages = buf.drainAll();
        if (messages.len == 0) return 0;
        defer allocator.free(messages);

        var bytes: u64 = 0;
        for (messages) |msg| {
            defer allocator.free(msg.data);
            try self.writeMessage(msg.timestamp_ns, msg.data);
            bytes += msg.data.len;
        }
        return bytes;
    }
};

// ---------------------------------------------------------------------------
// McapWriterPool — one shared MCAP file, one McapWriter per topic
// ---------------------------------------------------------------------------

pub const McapWriterPool = struct {
    handle: *anyopaque,
    writers: []McapWriter,
    allocator: std.mem.Allocator,
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
        const handle = mcap_writer_open(incomplete.ptr) orelse return error.McapOpenFailed;
        errdefer mcap_writer_close(handle);

        // Write session metadata
        writeSessionMetadata(allocator, handle, robot_id, software_version, now_ns, 0);

        const writers = try allocator.alloc(McapWriter, buf_pool.buffers.len);
        for (buf_pool.buffers, 0..) |*buf, i| {
            writers[i] = try McapWriter.init(allocator, handle, buf);
        }

        return .{
            .handle = handle,
            .writers = writers,
            .allocator = allocator,
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

    pub fn flushAll(self: *McapWriterPool, buf_pool: *tb.MessageBufferPool) !void {
        for (self.writers, buf_pool.buffers) |*writer, *buf| {
            if (buf.needsFlush()) {
                const bytes = try writer.flush(buf, buf.allocator);
                self.bytes_written += bytes;
            }
        }
    }

    /// Drain any remaining messages — call on shutdown.
    pub fn forceFlushAll(self: *McapWriterPool, buf_pool: *tb.MessageBufferPool) !void {
        for (self.writers, buf_pool.buffers) |*writer, *buf| {
            if (buf.len() > 0) {
                const bytes = try writer.flush(buf, buf.allocator);
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
    pub fn rotate(self: *McapWriterPool) !void {
        // Close and finalize the current file
        self.closeAndFinalize();

        // Next sequence
        self.sequence += 1;
        const now_ns: u64 = @intCast(std.time.nanoTimestamp());

        const new_filename = try generateFilename(
            self.allocator,
            self.out_dir,
            self.robot_id,
            now_ns,
            self.sequence,
        );
        const new_incomplete = try std.fmt.allocPrintSentinel(
            self.allocator,
            "{s}.incomplete",
            .{new_filename},
            0,
        );

        const new_handle = mcap_writer_open(new_incomplete.ptr) orelse
            return error.McapOpenFailed;

        // Write session metadata for the new file
        writeSessionMetadata(self.allocator, new_handle, self.robot_id, self.software_version, now_ns, self.sequence);

        // Re-register all channels on the new handle
        for (self.writers) |*writer| {
            writer.reregister(new_handle);
        }

        self.handle = new_handle;
        self.file_path = new_filename;
        self.incomplete_path = new_incomplete;
        self.bytes_written = 0;
        self.open_time_ns = now_ns;
        self.last_fsync_ns = now_ns;
    }

    /// Check and rotate if needed. Returns true if a rotation occurred.
    pub fn rotateIfNeeded(self: *McapWriterPool) !bool {
        if (self.shouldRotate()) {
            const old_path = self.file_path;
            try self.rotate();
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

        if (std.fs.cwd().openFileZ(self.incomplete_path, .{})) |file| {
            file.sync() catch |err| {
                std.log.err("periodic fsync failed for {s}: {}", .{ self.incomplete_path, err });
            };
            file.close();
        } else |err| {
            std.log.err("periodic fsync: failed to open {s}: {}", .{ self.incomplete_path, err });
        }

        self.last_fsync_ns = now_ns;
    }

    /// Close the MCAP file, fsync, rename, and write checksum.
    pub fn finish(self: *McapWriterPool) void {
        self.closeAndFinalize();
    }

    fn closeAndFinalize(self: *McapWriterPool) void {
        mcap_writer_close(self.handle);

        // Re-open the .incomplete file and fsync to ensure data is on disk
        if (std.fs.cwd().openFileZ(self.incomplete_path, .{})) |file| {
            file.sync() catch |err| {
                std.log.err("fsync failed for {s}: {}", .{ self.incomplete_path, err });
            };
            file.close();
        } else |err| {
            std.log.err("Failed to re-open for fsync {s}: {}", .{ self.incomplete_path, err });
        }

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
        writeSha256File(self.allocator, self.file_path) catch |err| {
            std.log.err("Failed to write SHA-256 for {s}: {}", .{ self.file_path, err });
        };
    }
};

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
// Session metadata — written to each MCAP file on open
// ---------------------------------------------------------------------------

fn writeSessionMetadata(
    allocator: std.mem.Allocator,
    handle: *anyopaque,
    robot_id: []const u8,
    software_version: []const u8,
    session_ns: u64,
    sequence: u32,
) void {
    // Format session_id and file_sequence as null-terminated strings
    const session_id_z = std.fmt.allocPrintSentinel(allocator, "{d}", .{session_ns}, 0) catch return;
    const seq_z = std.fmt.allocPrintSentinel(allocator, "{d}", .{sequence}, 0) catch return;
    const robot_id_z = allocator.dupeZ(u8, robot_id) catch return;
    const version_z = allocator.dupeZ(u8, software_version) catch return;

    const keys = [_][*:0]const u8{ "robot_id", "session_id", "software_version", "file_sequence" };
    const values = [_][*:0]const u8{ robot_id_z.ptr, session_id_z.ptr, version_z.ptr, seq_z.ptr };

    mcap_writer_write_metadata(handle, "rdl_session", &keys, &values, keys.len);
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
    const final_path = try std.fmt.allocPrintSentinel(allocator, "{s}.sha256", .{path_slice}, 0);

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
