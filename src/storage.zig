const std = @import("std");
const c = @import("c.zig").c;

pub const DiskUsage = struct {
    used_pct: u32,
    free_mb: u64,
};

/// Query filesystem usage for the given directory.
pub fn getUsage(log_dir: [*:0]const u8) !DiskUsage {
    var buf: c.struct_statvfs = undefined;
    if (c.statvfs(log_dir, &buf) != 0) {
        return error.StatvfsFailed;
    }

    const frsize: u64 = buf.f_frsize;
    const total_blocks: u64 = buf.f_blocks;
    const avail_blocks: u64 = buf.f_bavail;

    const total_bytes = total_blocks * frsize;
    const avail_bytes = avail_blocks * frsize;
    const free_mb = avail_bytes / (1024 * 1024);

    const used_pct: u32 = if (total_bytes > 0)
        @intCast(((total_bytes - avail_bytes) * 100) / total_bytes)
    else
        0;

    return .{ .used_pct = used_pct, .free_mb = free_mb };
}

/// Sum the sizes of all completed .mcap files in the log directory (bytes).
fn logDirSizeBytes(dir_path: []const u8) !u64 {
    var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
    defer dir.close();

    var total: u64 = 0;
    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".mcap")) continue;
        const stat = dir.statFile(entry.name) catch continue;
        total += stat.size;
    }
    return total;
}

/// Delete oldest completed .mcap files (and companion .sha256) until the total
/// size of .mcap files in the log directory is within max_log_dir_size_mb.
/// Returns the number of files deleted.
/// Never deletes the most recent file (always keeps at least one).
pub fn cleanupOldFiles(
    allocator: std.mem.Allocator,
    log_dir: [*:0]const u8,
    max_log_dir_size_mb: u32,
) !u32 {
    const dir_path: []const u8 = std.mem.sliceTo(log_dir, 0);
    const limit_bytes: u64 = @as(u64, max_log_dir_size_mb) * 1024 * 1024;

    // Check if we're already within the limit
    if (try logDirSizeBytes(dir_path) <= limit_bytes) {
        return 0;
    }

    // Collect completed .mcap filenames (not .incomplete, .recovering, .unrecoverable)
    var names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }

    var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".mcap")) continue;
        try names.append(allocator, try allocator.dupe(u8, entry.name));
    }

    if (names.items.len <= 1) return 0; // never delete the only file

    // Sort lexicographically — YYYYMMDD_HHMMSS_seq format sorts chronologically
    std.mem.sort([]const u8, names.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);

    var deleted: u32 = 0;
    // Delete oldest first, but always keep the last (most recent) file
    for (names.items[0 .. names.items.len - 1]) |name| {
        const mcap_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir_path, name });
        defer allocator.free(mcap_path);

        std.fs.cwd().deleteFile(mcap_path) catch |err| {
            std.log.err("Failed to delete {s}: {}", .{ name, err });
            continue;
        };
        std.log.info("Deleted old recording: {s}", .{name});

        // Delete companion .sha256 if it exists
        const sha_path = try std.fmt.allocPrint(allocator, "{s}/{s}.sha256", .{ dir_path, name });
        defer allocator.free(sha_path);
        std.fs.cwd().deleteFile(sha_path) catch {};

        deleted += 1;

        // Re-check size after each deletion
        const current_size = logDirSizeBytes(dir_path) catch break;
        if (current_size <= limit_bytes) break;
    }

    return deleted;
}
