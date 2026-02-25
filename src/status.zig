const std = @import("std");
const config = @import("configure/config.zig");
const constants = @import("configure/constants.zig");
const storage = @import("storage.zig");
const tb = @import("topic_buffer.zig");
const mcap = @import("mcap_writer.zig");
const http = std.http;

const log = std.log.scoped(.status);

// ---------------------------------------------------------------------------
// Payload structs — JSON-serializable
// ---------------------------------------------------------------------------

const TopicStatus = struct {
    name: []const u8,
    type_name: []const u8,
    messages_received: u64,
    bytes_received: u64,
    drop_count: u64,
};

const StatusPayload = struct {
    robot_id: []const u8,
    software_version: []const u8,
    status: []const u8,
    uptime_s: u64,
    timestamp_ns: u64,
    current_file: []const u8,
    file_sequence: u32,
    bytes_written: u64,
    total_bytes_written: u64,
    disk_usage_pct: u32,
    disk_free_mb: u64,
    topics: []const TopicStatus,
};

// ---------------------------------------------------------------------------
// StatusReporter
// ---------------------------------------------------------------------------

pub const StatusReporter = struct {
    allocator: std.mem.Allocator,
    robot_id: []const u8,
    software_version: []const u8,
    log_dir_z: [*:0]const u8,
    interval_ns: u64,
    last_report_ns: u64,
    start_ns: u64,
    total_bytes_written: u64,
    enabled: bool,

    pub fn init(
        allocator: std.mem.Allocator,
        software_version: []const u8,
        log_dir_z: [*:0]const u8,
        status_interval_s: u32,
    ) StatusReporter {
        const now_ns: u64 = @intCast(std.time.nanoTimestamp());

        if (status_interval_s == 0) {
            return .{
                .allocator = allocator,
                .robot_id = "",
                .software_version = software_version,
                .log_dir_z = log_dir_z,
                .interval_ns = 0,
                .last_report_ns = now_ns,
                .start_ns = now_ns,
                .total_bytes_written = 0,
                .enabled = false,
            };
        }

        // Try to get robot_id from provisioning config
        const robot_id = config.ConfigStorage.getRobotId(allocator) catch {
            log.warn("Orca cloud status reporting disabled: robot not provisioned", .{});
            return .{
                .allocator = allocator,
                .robot_id = "",
                .software_version = software_version,
                .log_dir_z = log_dir_z,
                .interval_ns = 0,
                .last_report_ns = now_ns,
                .start_ns = now_ns,
                .total_bytes_written = 0,
                .enabled = false,
            };
        };

        const interval_ns: u64 = @as(u64, status_interval_s) * std.time.ns_per_s;

        log.info("Status reporting enabled: every {d}s to {s}", .{ status_interval_s, constants.status_base_url });

        return .{
            .allocator = allocator,
            .robot_id = robot_id,
            .software_version = software_version,
            .log_dir_z = log_dir_z,
            .interval_ns = interval_ns,
            .last_report_ns = now_ns,
            .start_ns = now_ns,
            .total_bytes_written = 0,
            .enabled = true,
        };
    }

    /// Called after each file rotation to accumulate total bytes across files.
    pub fn addBytesWritten(self: *StatusReporter, bytes: u64) void {
        self.total_bytes_written += bytes;
    }

    /// Called from the main loop. Sends a status report if the interval has elapsed.
    /// Never propagates errors — logs warnings instead.
    pub fn maybeSend(
        self: *StatusReporter,
        writer_pool: *const mcap.McapWriterPool,
        buf_pool: *const tb.MessageBufferPool,
    ) void {
        if (!self.enabled) return;

        const now_ns: u64 = @intCast(std.time.nanoTimestamp());
        if (now_ns - self.last_report_ns < self.interval_ns) return;

        self.last_report_ns = now_ns;
        self.sendStatus(writer_pool, buf_pool, now_ns) catch |err| {
            log.warn("Status report failed: {}", .{err});
        };
    }

    fn sendStatus(
        self: *StatusReporter,
        writer_pool: *const mcap.McapWriterPool,
        buf_pool: *const tb.MessageBufferPool,
        now_ns: u64,
    ) !void {
        const base64_encoder = std.base64.standard.Encoder;

        // Build per-topic stats
        const topic_stats = try self.allocator.alloc(TopicStatus, buf_pool.buffers.len);
        defer self.allocator.free(topic_stats);

        for (buf_pool.buffers, 0..) |*buf, i| {
            topic_stats[i] = .{
                .name = buf.topic_name,
                .type_name = buf.type_name,
                .messages_received = buf.messages_received,
                .bytes_received = buf.bytes_received,
                .drop_count = buf.drop_count,
            };
        }

        // Get disk usage
        const disk = storage.getUsage(self.log_dir_z) catch storage.DiskUsage{ .used_pct = 0, .free_mb = 0 };

        // Extract current filename (basename only)
        const file_path_slice: []const u8 = std.mem.sliceTo(writer_pool.file_path, 0);
        const current_file = std.fs.path.basename(file_path_slice);

        const uptime_s = (now_ns - self.start_ns) / std.time.ns_per_s;

        const payload = StatusPayload{
            .robot_id = self.robot_id,
            .software_version = self.software_version,
            .status = "recording",
            .uptime_s = uptime_s,
            .timestamp_ns = now_ns,
            .current_file = current_file,
            .file_sequence = writer_pool.sequence,
            .bytes_written = writer_pool.bytes_written,
            .total_bytes_written = self.total_bytes_written + writer_pool.bytes_written,
            .disk_usage_pct = disk.used_pct,
            .disk_free_mb = disk.free_mb,
            .topics = topic_stats,
        };

        // Serialize to JSON
        var body_payload: std.Io.Writer.Allocating = .init(self.allocator);
        defer body_payload.deinit();
        try body_payload.writer.print("{f}", .{std.json.fmt(payload, .{})});
        const json_bytes = body_payload.written();

        // Sign payload
        const sig_bytes = try config.ConfigStorage.signPayload(self.allocator, json_bytes);
        var sig_b64: [base64_encoder.calcSize(64)]u8 = undefined;
        _ = base64_encoder.encode(&sig_b64, &sig_bytes);

        // HTTP POST
        var client = http.Client{ .allocator = self.allocator };
        defer client.deinit();

        var response_body: std.Io.Writer.Allocating = .init(self.allocator);
        defer response_body.deinit();

        const url = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ constants.status_base_url, self.robot_id });
        defer self.allocator.free(url);

        const result = try client.fetch(.{
            .method = .POST,
            .location = .{ .url = url },
            .payload = json_bytes,
            .response_writer = &response_body.writer,
            .headers = .{
                .content_type = .{ .override = "application/json" },
                .connection = .{ .override = "close" },
            },
            .extra_headers = &.{
                .{ .name = "X-Signature", .value = &sig_b64 },
            },
        });

        if (result.status != .ok) {
            log.warn("Status POST returned {d}", .{result.status});
        }
    }
};
