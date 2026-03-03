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

/// Delete oldest completed .mcap files (and companion .sha256) until disk usage
/// is within limits. Returns the number of files deleted.
/// Never deletes the most recent file (always keeps at least one).
pub fn cleanupOldFiles(
    allocator: std.mem.Allocator,
    log_dir: [*:0]const u8,
    disk_usage_limit_pct: u32,
    min_free_disk_mb: u32,
) !u32 {
    // Check if we're already within limits
    const usage = try getUsage(log_dir);
    if (usage.used_pct <= disk_usage_limit_pct and usage.free_mb >= min_free_disk_mb) {
        return 0;
    }

    // Convert sentinel pointer to slice for std.fs operations
    const dir_path: []const u8 = std.mem.sliceTo(log_dir, 0);

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
        // Skip files with extra suffixes (.incomplete, .recovering, .unrecoverable)
        // Those would end with e.g. ".mcap.incomplete", not ".mcap"
        // Since we check endsWith(".mcap"), bare ".mcap" files are already correct.
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
        // Delete the .mcap file
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

        // Re-check usage after each deletion
        const current = getUsage(log_dir) catch break;
        if (current.used_pct <= disk_usage_limit_pct and current.free_mb >= min_free_disk_mb) {
            break;
        }
    }

    return deleted;
}
