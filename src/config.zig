const std = @import("std");
const crypto = std.crypto;

pub const Config = struct {
    robot_id: []const u8 = "robot_001",
    log_directory: []const u8 = "",
    max_bag_duration_s: u32 = 60,
    max_bag_size_mb: u32 = 512,
    write_buffer_mb: u32 = 8,
    software_version: []const u8 = "0.1.0",
    disk_usage_limit_pct: u32 = 80,
    min_free_disk_mb: u32 = 200,
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
        .allocate = .alloc_always,
    });
}

/// Apply environment variable overrides. Returns a new Config with overridden values.
/// The returned Config borrows from both the original parsed config and env var strings,
/// so the caller must keep both alive.
pub fn resolve(allocator: std.mem.Allocator, cfg: Config) !Config {
    var result = cfg;
    if (std.posix.getenv("ORCA_ROBOT_ID")) |val| {
        result.robot_id = val;
    }
    if (std.posix.getenv("ORCA_LOG_DIRECTORY")) |val| {
        result.log_directory = val;
    }
    if (std.posix.getenv("ORCA_DISK_USAGE_LIMIT_PCT")) |val| {
        result.disk_usage_limit_pct = std.fmt.parseInt(u32, val, 10) catch result.disk_usage_limit_pct;
    }
    if (std.posix.getenv("ORCA_MIN_FREE_DISK_MB")) |val| {
        result.min_free_disk_mb = std.fmt.parseInt(u32, val, 10) catch result.min_free_disk_mb;
    }
    if (std.posix.getenv("ORCA_FSYNC_INTERVAL_S")) |val| {
        result.fsync_interval_s = std.fmt.parseInt(u32, val, 10) catch result.fsync_interval_s;
    }
    if (std.posix.getenv("ORCA_STATUS_INTERVAL_S")) |val| {
        result.status_interval_s = std.fmt.parseInt(u32, val, 10) catch result.status_interval_s;
    }

    // if log_directory is still empty (default), resolve to ~/.orca/logs
    if (result.log_directory.len == 0) {
        const home = std.posix.getenv("HOME") orelse "/tmp";
        result.log_directory = try std.fs.path.join(allocator, &.{ home, ".orca", "logs" });
        // Caller owns this allocation — see main.zig defer at cfg.log_directory free
    }

    return result;
}

/// Validate config and ensure log directory exists.
pub fn validate(cfg: *const Config) !void {
    if (cfg.topics.len == 0) {
        std.debug.print("Error: config must specify at least one topic\n", .{});
        return error.NoTopics;
    }

    // Ensure log directory exists (create nested directories if needed)
    std.fs.cwd().makePath(cfg.log_directory) catch |err| {
        std.debug.print("Error: cannot create log directory '{s}': {}\n", .{ cfg.log_directory, err });
        return err;
    };
}

// ---------------------------------------------------------------------------
// Robot identity & storage (provisioning, signing)
// ---------------------------------------------------------------------------

pub const RobotConfig = struct {
    id: []u8,
    cloud_available: bool,
};

pub const ConfigStorage = struct {
    const dir_name = ".orca";
    pub const pub_key_file = "id_ed25519.pub";
    pub const priv_key_file = "id_ed25519";
    pub const robot_config_file = "config.json"; // RobotConfig
    pub const collector_config_file = "collector.json";

    pub fn getStoragePath(allocator: std.mem.Allocator) ![]u8 {
        const home_owned = std.process.getEnvVarOwned(allocator, "HOME") catch null;
        defer if (home_owned) |h| allocator.free(h);
        const home = home_owned orelse "/tmp";
        return std.fs.path.join(allocator, &.{ home, dir_name });
    }

    pub fn getRobotId(allocator: std.mem.Allocator) ![]u8 {
        const storage_path = try getStoragePath(allocator);
        defer allocator.free(storage_path);

        var dir = try std.fs.openDirAbsolute(storage_path, .{});
        defer dir.close();

        const file_contents = try dir.readFileAlloc(allocator, "config.json", 1024 * 1024);
        defer allocator.free(file_contents);

        const parsed = try std.json.parseFromSlice(RobotConfig, allocator, file_contents, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();

        return try allocator.dupe(u8, parsed.value.id);
    }

    /// retrieve from the config whether the asset is configured to
    /// stream to the cloud
    pub fn getCloudAvailabilityStatus(allocator: std.mem.Allocator) !bool {
        const storage_path = try getStoragePath(allocator);
        defer allocator.free(storage_path);

        var dir = try std.fs.openDirAbsolute(storage_path, .{});
        defer dir.close();

        const file_contents = try dir.readFileAlloc(allocator, "config.json", 1024 * 1024);
        defer allocator.free(file_contents);

        const parsed = try std.json.parseFromSlice(RobotConfig, allocator, file_contents, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();

        return parsed.value.cloud_available;
    }

    pub fn setCloudAvailabilityStatus(allocator: std.mem.Allocator, cloud_availability: bool) !void {
        const storage_path = try getStoragePath(allocator);
        defer allocator.free(storage_path);

        var dir = try std.fs.openDirAbsolute(storage_path, .{});
        defer dir.close();

        const file_contents = try dir.readFileAlloc(allocator, "config.json", 1024 * 1024);
        defer allocator.free(file_contents);

        const parsed: std.json.Parsed(RobotConfig) = try std.json.parseFromSlice(RobotConfig, allocator, file_contents, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();

        // copy the config so we can mutate it
        var updated = parsed.value;
        updated.cloud_available = cloud_availability;

        // write it back
        const file = try dir.createFile("config.json", .{});
        defer file.close();

        var writer: std.Io.Writer.Allocating = .init(allocator);
        defer writer.deinit();

        try writer.writer.print("{f}", .{std.json.fmt(updated, .{})});
        try file.writeAll(writer.written());
    }

    pub fn signPayload(allocator: std.mem.Allocator, message: []const u8) ![64]u8 { // Note: Ed25519 signature is 64 bytes
        const storage_path = try getStoragePath(allocator);
        defer allocator.free(storage_path);

        var dir = try std.fs.openDirAbsolute(storage_path, .{});
        defer dir.close();

        // 1. Ed25519 SecretKey is 64 bytes. If stored as HEX, the file is 128 bytes.
        var hex_buffer: [crypto.sign.Ed25519.SecretKey.encoded_length * 2]u8 = undefined;
        const file = try dir.openFile(priv_key_file, .{});
        defer file.close();

        const amt = try file.readAll(&hex_buffer);
        if (amt < hex_buffer.len) return error.InvalidKeyLength;

        // 2. Decode the Hex string into actual bytes
        var secret_key_bytes: [crypto.sign.Ed25519.SecretKey.encoded_length]u8 = undefined;
        _ = try std.fmt.hexToBytes(&secret_key_bytes, &hex_buffer);

        // 3. Now this will work!
        const secretKey = try crypto.sign.Ed25519.SecretKey.fromBytes(secret_key_bytes);
        const keypair = try crypto.sign.Ed25519.KeyPair.fromSecretKey(secretKey);

        const sig = try keypair.sign(message, null);
        return sig.toBytes();
    }
};
