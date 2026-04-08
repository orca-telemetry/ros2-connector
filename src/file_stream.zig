const std = @import("std");
const http = std.http;
const config = @import("config.zig");
const constants = @import("configure/constants.zig");

const log = std.log.scoped(.stream);

/// Frequency at which we should search the directory for mcap files to stream
/// to the bucket.
const POLL_INTERVAL_NS = 5 * std.time.ns_per_s;

/// The size of the file read buffer used when streaming chunks to the bucket.
const FILE_BUFFER_SIZE: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// File discovery
// ---------------------------------------------------------------------------

/// Scans log_dir for files where both "name.mcap" and "name.mcap.sha256" exist.
/// Returns a sorted (lexicographic = chronological) list of owned name slices.
/// Caller must free each name and call deinit on the list.
fn collectCompletedFiles(
    allocator: std.mem.Allocator,
    log_dir: []const u8,
) !std.ArrayListUnmanaged([]const u8) {
    var candidates: std.ArrayListUnmanaged([]const u8) = .empty;

    errdefer {
        for (candidates.items) |name| allocator.free(name);
        candidates.deinit(allocator);
    }

    var dir = try std.fs.cwd().openDir(log_dir, .{ .iterate = true });
    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".mcap")) continue;
        if (std.mem.endsWith(u8, entry.name, ".incomplete")) continue;
        if (std.mem.endsWith(u8, entry.name, ".recovering")) continue;
        if (std.mem.endsWith(u8, entry.name, ".unrecoverable")) continue;

        // confirm that companion .sha256 exists
        var buf: [std.fs.max_name_bytes + ".sha256".len]u8 = undefined;
        const sha_name = try std.fmt.bufPrint(&buf, "{s}.sha256", .{entry.name});
        dir.access(sha_name, .{}) catch continue;

        try candidates.append(allocator, try allocator.dupe(u8, entry.name));
    }

    // Sort lexicographically — YYYYMMDD_HHMMSS_seq.mcap names are chronological
    std.mem.sort([]const u8, candidates.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lt);

    return candidates;
}

/// Requests a presigned upload URL from the Orca app for the given file.
/// Returns an allocator-owned URL string. Caller must free.
fn requestPresignedUrl(
    allocator: std.mem.Allocator,
    robot_id: []const u8,
    file_name: []const u8,
) ![]u8 {
    const url = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ constants.upload_base_url, robot_id });
    defer allocator.free(url);

    const path = try std.fmt.allocPrint(allocator, "/api/robot/upload/{s}", .{robot_id});
    defer allocator.free(path);

    // Sign: POST:/api/robot/upload/{robot_id}:{timestamp_ms}
    const timestamp = std.time.milliTimestamp();
    const timestamp_str = try std.fmt.allocPrint(allocator, "{d}", .{timestamp});
    defer allocator.free(timestamp_str);

    const sign_payload = try std.fmt.allocPrint(allocator, "POST:{s}:{s}", .{ path, timestamp_str });
    defer allocator.free(sign_payload);

    const sig_bytes = try config.ConfigStorage.signPayload(allocator, sign_payload);
    const base64_encoder = std.base64.standard.Encoder;
    var sig_b64: [base64_encoder.calcSize(64)]u8 = undefined;
    _ = base64_encoder.encode(&sig_b64, &sig_bytes);

    // JSON body: {"file_name":"..."}
    const body = try std.fmt.allocPrint(allocator, "{{\"file_name\":\"{s}\"}}", .{file_name});
    defer allocator.free(body);

    var client = http.Client{ .allocator = allocator };
    defer client.deinit();

    var response_body: std.Io.Writer.Allocating = .init(allocator);
    defer response_body.deinit();

    const result = try client.fetch(.{
        .method = .POST,
        .location = .{ .url = url },
        .payload = body,
        .response_writer = &response_body.writer,
        .headers = .{
            .content_type = .{ .override = "application/json" },
            .connection = .{ .override = "close" },
        },
        .extra_headers = &.{
            .{ .name = "X-Signature", .value = &sig_b64 },
            .{ .name = "X-Timestamp", .value = timestamp_str },
        },
    });

    if (result.status != .ok) {
        log.warn("Presigned URL request failed (HTTP {d}) for {s}", .{ @intFromEnum(result.status), file_name });
        return error.PresignedUrlRequestFailed;
    }

    const UrlResponse = struct {
        url: []const u8 = "",
    };

    const parsed = try std.json.parseFromSlice(UrlResponse, allocator, response_body.written(), .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    if (parsed.value.url.len == 0) return error.EmptyPresignedUrl;

    return allocator.dupe(u8, parsed.value.url);
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

/// Uploads a file to a presigned URL via HTTP PUT.
/// Returns `error.UploadExpired` on 403/410 so the caller can re-request and retry.
fn uploadFile(
    allocator: std.mem.Allocator,
    file_dir: []const u8,
    file_name: []const u8,
    presigned_url: []const u8,
) !void {
    var path_buf: [std.fs.max_path_bytes + std.fs.max_name_bytes]u8 = undefined;
    var file_read_buffer: [FILE_BUFFER_SIZE]u8 = undefined;

    const file_path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ file_dir, file_name });
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();
    const file_size = (try file.stat()).size;

    var file_reader = file.reader(&file_read_buffer);
    const file_reader_interface = &file_reader.interface;

    var client = http.Client{ .allocator = allocator };
    defer client.deinit();

    var req = try client.request(
        .PUT,
        try std.Uri.parse(presigned_url),
        .{
            .headers = .{
                .content_type = .{ .override = "application/octet-stream" },
            },
        },
    );
    req.transfer_encoding = .chunked;
    defer req.deinit();

    try req.sendBodiless();

    const n_chunks = file_size / FILE_BUFFER_SIZE;
    const remainder = file_size % FILE_BUFFER_SIZE;
    var in_flight_buffer: [FILE_BUFFER_SIZE]u8 = undefined;
    var body_writer = try req.sendBody(&in_flight_buffer);

    for (0..n_chunks) |_| {
        try file_reader_interface.readSliceAll(&in_flight_buffer);
        try body_writer.flush();
    }

    if (remainder > 0) {
        try file_reader_interface.readSliceAll(in_flight_buffer[0..remainder]);
        try body_writer.flush();
    }

    var redirect_buf: [4096]u8 = undefined;
    const response = try req.receiveHead(&redirect_buf);
    const status = response.head.status;
    const status_code = @intFromEnum(status);

    if (status == .forbidden or status == .gone) {
        return error.UploadExpired;
    }
    if (status_code / 100 != 2) {
        log.warn("Upload of {s} rejected with HTTP {d}", .{ file_name, status_code });
        return error.UploadFailed;
    }
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

/// Delete both the .mcap and .mcap.sha256 files. Logs errors but does not fail.
fn deleteCompletedPair(
    allocator: std.mem.Allocator,
    log_dir: []const u8,
    mcap_name: []const u8,
) void {
    const mcap_path = std.fmt.allocPrint(allocator, "{s}/{s}", .{ log_dir, mcap_name }) catch return;
    defer allocator.free(mcap_path);
    std.fs.cwd().deleteFile(mcap_path) catch |err| {
        log.err("Failed to delete {s}: {}", .{ mcap_name, err });
    };

    const sha_path = std.fmt.allocPrint(allocator, "{s}/{s}.sha256", .{ log_dir, mcap_name }) catch return;
    defer allocator.free(sha_path);
    std.fs.cwd().deleteFile(sha_path) catch |err| {
        log.warn("Failed to delete {s}.sha256: {}", .{ mcap_name, err });
    };
}

// ---------------------------------------------------------------------------
// StreamWorker
// ---------------------------------------------------------------------------

pub const StreamWorker = struct {
    allocator: std.mem.Allocator,
    log_dir: []const u8,
    robot_id: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        log_dir: []const u8,
        robot_id: []const u8,
    ) StreamWorker {
        return .{
            .allocator = allocator,
            .log_dir = log_dir,
            .robot_id = robot_id,
        };
    }

    /// Run the streaming loop until `running` becomes false.
    pub fn run(self: *StreamWorker, running: *std.atomic.Value(bool)) void {
        log.info("Stream worker started. Watching '{s}'", .{self.log_dir});

        while (running.load(.acquire)) {
            self.processOneBatch();

            // sleep in 200ms increments for responsive shutdown
            var slept_ns: u64 = 0;
            while (slept_ns < POLL_INTERVAL_NS and running.load(.acquire)) {
                std.Thread.sleep(200 * std.time.ns_per_ms);
                slept_ns += 200 * std.time.ns_per_ms;
            }
        }

        log.info("Stream worker stopped.", .{});
    }

    fn processOneBatch(self: *StreamWorker) void {
        var files = collectCompletedFiles(self.allocator, self.log_dir) catch |err| {
            log.warn("Directory scan failed: {}", .{err});
            return;
        };
        defer {
            for (files.items) |name| self.allocator.free(name);
            files.deinit(self.allocator);
        }

        if (files.items.len == 0) return;

        log.info("Found {d} file(s) ready for upload", .{files.items.len});

        for (files.items) |mcap_name| {
            // request a fresh presigned URL for each file
            var presigned_url = requestPresignedUrl(self.allocator, self.robot_id, mcap_name) catch |err| {
                log.warn("Could not get presigned URL for {s}: {}", .{ mcap_name, err });
                continue;
            };
            defer self.allocator.free(presigned_url);

            uploadFile(self.allocator, self.log_dir, mcap_name, presigned_url) catch |err| {
                if (err == error.UploadExpired) {
                    // URL expired between request and upload — re-request once and retry
                    log.info("Presigned URL expired for {s}, re-requesting", .{mcap_name});
                    self.allocator.free(presigned_url);
                    presigned_url = requestPresignedUrl(self.allocator, self.robot_id, mcap_name) catch |rerr| {
                        log.warn("Re-request failed for {s}: {}", .{ mcap_name, rerr });
                        presigned_url = &.{}; // prevent double-free in defer
                        continue;
                    };
                    uploadFile(self.allocator, self.log_dir, mcap_name, presigned_url) catch |retry_err| {
                        log.warn("Upload retry failed for {s}: {}", .{ mcap_name, retry_err });
                        continue;
                    };
                } else {
                    log.warn("Upload skipped for {s}: {}", .{ mcap_name, err });
                    continue;
                }
            };

            log.info("Uploaded {s}", .{mcap_name});
            deleteCompletedPair(self.allocator, self.log_dir, mcap_name);
        }
    }
};
