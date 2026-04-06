const std = @import("std");
const http = std.http;

const log = std.log.scoped(.stream);

const POLL_INTERVAL_NS = 5 * std.time.ns_per_s;
/// the size of the file read buffer. this amount will be consumed in memory and used
/// to consume chunks of the file and then send over to the bucket via HTTP
const FILE_BUFFER_SIZE: usize = 1024 * 1024;

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

/// uploads an mcap file and the mcap.sha265 to the bucket endpoint
fn uploadFile(
    allocator: std.mem.Allocator,
    file_dir: []const u8,
    file_name: []const u8,
    bucket_url: []const u8,
    bucket_token: []const u8,
) !void {
    // initialise buffers
    var path_buf: [std.fs.max_path_bytes + std.fs.max_name_bytes]u8 = undefined; // file path name buf
    var url_buf: [std.fs.max_path_bytes]u8 = undefined; // url name buf
    var bearer_buf: [512]u8 = undefined; // bearer token buf
    var file_read_buffer: [FILE_BUFFER_SIZE]u8 = undefined; // buffer used to read the file

    // open the file and extract metrics
    const file_path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ file_dir, file_name });
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();
    const file_size = (try file.stat()).size;

    // get the file reader and a ptr to the interface
    var file_reader = file.reader(&file_read_buffer);
    const file_reader_interface = &file_reader.interface;

    // set up the request
    var client = http.Client{ .allocator = allocator };
    defer client.deinit();
    var req = try client.request(
        .POST,
        try std.Uri.parse(
            try std.fmt.bufPrint(&url_buf, "{s}{s}", .{ bucket_url, file_name }),
        ),
        .{
            .headers = .{
                .content_type = .{ .override = "application/octet-stream" },
            },
            .extra_headers = &.{
                .{ .name = "Authorization", .value = try std.fmt.bufPrint(&bearer_buf, "Bearer {s}", .{bucket_token}) },
            },
        },
    );

    // important for single-request uploads
    // mandated by GCP
    // TODO: confirm if AWS is different
    req.transfer_encoding = .chunked;

    defer req.deinit();

    // send just the header
    try req.sendBodiless();

    // go through the file and write buffer chunks
    // this calculates the exact chunks
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
}

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

pub const StreamWorker = struct {
    allocator: std.mem.Allocator,
    log_dir: []const u8,
    bucket_url: []const u8,
    bucket_token: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        log_dir: []const u8,
        bucket_url: []const u8,
        bucket_token: []const u8,
    ) StreamWorker {
        return .{
            .allocator = allocator,
            .log_dir = log_dir,
            .bucket_url = bucket_url,
            .bucket_token = bucket_token,
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
            uploadFile(
                self.allocator,
                self.log_dir,
                mcap_name,
                self.bucket_url,
                self.bucket_token,
            ) catch |err| {
                log.warn("Upload skipped for {s}: {}", .{ mcap_name, err });
                continue;
            };

            deleteCompletedPair(self.allocator, self.log_dir, mcap_name);
        }
    }
};

pub fn main() !void {
    const dir: []u8 = "./data";
    const bucket_url: []u8 = "?"
    const allocator, const is_debug = gpa: {
        break :gpa switch (builtin.mode) {
            .Debug, .ReleaseSafe => .{ debug_allocator.allocator(), true },
            .ReleaseFast, .ReleaseSmall => .{ std.heap.smp_allocator, false },
        };
    };

    StreamWorker.init(allocator, log_dir: dir, bucket_url: , bucket_token: []const u8)

}
