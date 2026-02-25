const std = @import("std");

pub const Config = struct {
    robot_id: []const u8 = "robot_001",
    log_directory: []const u8 = "/data/logs",
    max_bag_duration_s: u32 = 60,
    max_bag_size_mb: u32 = 512,
    write_buffer_mb: u32 = 8,
    software_version: []const u8 = "0.1.0",
    disk_usage_limit_pct: u32 = 80,
    min_free_disk_mb: u32 = 500,
    fsync_interval_s: u32 = 5,
    status_interval_s: u32 = 30,
    topics: []const TopicEntry,

    pub const TopicEntry = struct {
        type_name: []const u8,
        topic_name: []const u8,
    };
};

pub fn load(allocator: std.mem.Allocator, path: []const u8) !std.json.Parsed(Config) {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const contents = try file.readToEndAlloc(allocator, 1024 * 1024);
    defer allocator.free(contents);

    return try std.json.parseFromSlice(Config, allocator, contents, .{
        .ignore_unknown_fields = true,
    });
}

/// Apply environment variable overrides. Returns a new Config with overridden values.
/// The returned Config borrows from both the original parsed config and env var strings,
/// so the caller must keep both alive.
pub fn resolve(cfg: Config) Config {
    var result = cfg;
    if (std.posix.getenv("RDL_ROBOT_ID")) |val| {
        result.robot_id = val;
    }
    if (std.posix.getenv("RDL_LOG_DIRECTORY")) |val| {
        result.log_directory = val;
    }
    if (std.posix.getenv("RDL_DISK_USAGE_LIMIT_PCT")) |val| {
        result.disk_usage_limit_pct = std.fmt.parseInt(u32, val, 10) catch result.disk_usage_limit_pct;
    }
    if (std.posix.getenv("RDL_MIN_FREE_DISK_MB")) |val| {
        result.min_free_disk_mb = std.fmt.parseInt(u32, val, 10) catch result.min_free_disk_mb;
    }
    if (std.posix.getenv("RDL_FSYNC_INTERVAL_S")) |val| {
        result.fsync_interval_s = std.fmt.parseInt(u32, val, 10) catch result.fsync_interval_s;
    }
    if (std.posix.getenv("RDL_STATUS_INTERVAL_S")) |val| {
        result.status_interval_s = std.fmt.parseInt(u32, val, 10) catch result.status_interval_s;
    }
    return result;
}

/// Validate config and ensure log directory exists.
pub fn validate(cfg: *const Config) !void {
    if (cfg.topics.len == 0) {
        std.debug.print("Error: config must specify at least one topic\n", .{});
        return error.NoTopics;
    }

    // Ensure log directory exists (create if needed)
    std.fs.makeDirAbsolute(cfg.log_directory) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => {
            std.debug.print("Error: cannot create log directory '{s}': {}\n", .{ cfg.log_directory, err });
            return err;
        },
    };
}
