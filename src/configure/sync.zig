const std = @import("std");
const config = @import("../config.zig");
const constants = @import("constants.zig");
const http = std.http;

const log = std.log.scoped(.sync);

pub const SyncError = error{ SyncFailed, SyncTooEarly, NotProvisioned };

/// Fetch topic configuration from Orca cloud and write it to the local config file.
pub fn syncConfig(allocator: std.mem.Allocator) !void {
    const robot_id = config.ConfigStorage.getRobotId(allocator) catch {
        std.debug.print("Error: robot not provisioned. Run `orca provision --token <T>` first.\n", .{});
        return error.NotProvisioned;
    };
    defer allocator.free(robot_id);

    const storage_path = try config.ConfigStorage.getStoragePath(allocator);
    defer allocator.free(storage_path);

    // Build URL
    const url = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ constants.sync_base_url, robot_id });
    defer allocator.free(url);

    const path = try std.fmt.allocPrint(allocator, "/api/robot/sync/{s}", .{robot_id});
    defer allocator.free(path);

    // Build signature payload: GET:/path:timestamp
    const timestamp = std.time.milliTimestamp();
    const timestamp_str = try std.fmt.allocPrint(allocator, "{d}", .{timestamp});
    defer allocator.free(timestamp_str);

    const sign_payload = try std.fmt.allocPrint(allocator, "GET:{s}:{s}", .{ path, timestamp_str });
    defer allocator.free(sign_payload);

    const sig_bytes = try config.ConfigStorage.signPayload(allocator, sign_payload);
    const base64_encoder = std.base64.standard.Encoder;
    var sig_b64: [base64_encoder.calcSize(64)]u8 = undefined;
    _ = base64_encoder.encode(&sig_b64, &sig_bytes);

    // HTTP GET
    var client = http.Client{ .allocator = allocator };
    defer client.deinit();

    var response_body: std.Io.Writer.Allocating = .init(allocator);
    defer response_body.deinit();

    const result = try client.fetch(.{
        .method = .GET,
        .location = .{ .url = url },
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

    if (result.status == .service_unavailable) {
        // reserved by the server to denote that sync is not ready yet
        return error.SyncTooEarly;
    }

    if (result.status != .ok) {
        const body = response_body.written();
        std.debug.print("Sync failed (HTTP {d}): {s}\n", .{ @intFromEnum(result.status), body });
        return error.SyncFailed;
    }

    const body = response_body.written();

    // parse response - top-level object with topics array and optional bucket config
    const RemoteTopic = struct {
        name: []const u8,
        type_name: []const u8,
    };

    const SyncResponse = struct {
        topics: []const RemoteTopic = &.{},
        cloud_available: bool = false,
    };

    const parsed: std.json.Parsed(SyncResponse) = std.json.parseFromSlice(SyncResponse, allocator, body, .{
        .ignore_unknown_fields = true,
    }) catch |err| {
        std.debug.print("Error: failed to parse sync response: {}\n", .{err});
        return error.InvalidResponse;
    };
    defer parsed.deinit();

    // load existing config (or use defaults) and update topics + bucket config
    const config_path = try std.fs.path.join(allocator, &.{ storage_path, config.ConfigStorage.collector_config_file });
    defer allocator.free(config_path);

    var existing = loadOrDefault(allocator, config_path);
    defer if (existing.parsed) |*p| p.deinit();

    // build topic entries from remote data
    const topics = try allocator.alloc(config.Config.TopicEntry, parsed.value.topics.len);
    for (parsed.value.topics, 0..) |remote, i| {
        topics[i] = .{
            .topic_name = remote.name,
            .type_name = remote.type_name,
        };
    }
    existing.value.topics = topics;
    defer allocator.free(topics);

    // update the cloud availability status
    try config.ConfigStorage.setCloudAvailabilityStatus(allocator, parsed.value.cloud_available);

    // write updated config
    try writeConfig(allocator, config_path, &existing.value);

    std.debug.print("Synced {d} topic(s) from Orca cloud.\n", .{parsed.value.topics.len});
    for (parsed.value.topics) |topic| {
        std.debug.print("  {s} [{s}]\n", .{ topic.name, topic.type_name });
    }
}

const LoadResult = struct {
    value: config.Config,
    parsed: ?std.json.Parsed(config.Config),
};

/// Try to load existing config; if it doesn't exist, return defaults.
/// Caller must deinit the returned .parsed if non-null.
fn loadOrDefault(allocator: std.mem.Allocator, path: []const u8) LoadResult {
    const parsed = config.load(allocator, path) catch {
        return .{
            .value = .{ .topics = &.{} },
            .parsed = null,
        };
    };
    return .{
        .value = parsed.value,
        .parsed = parsed,
    };
}

/// Serialise config to JSON and write to disk.
fn writeConfig(allocator: std.mem.Allocator, path: []const u8, cfg: *const config.Config) !void {
    // Build a serializable struct that matches what load() expects
    const Serializable = struct {
        robot_id: []const u8,
        log_directory: []const u8,
        max_bag_duration_s: u32,
        max_bag_size_mb: u32,
        write_buffer_mb: u32,
        software_version: []const u8,
        disk_usage_limit_pct: u32,
        min_free_disk_mb: u32,
        fsync_interval_s: u32,
        status_interval_s: u32,
        topics: []const config.Config.TopicEntry,
    };

    const out = Serializable{
        .robot_id = cfg.robot_id,
        .log_directory = cfg.log_directory,
        .max_bag_duration_s = cfg.max_bag_duration_s,
        .max_bag_size_mb = cfg.max_bag_size_mb,
        .write_buffer_mb = cfg.write_buffer_mb,
        .software_version = cfg.software_version,
        .disk_usage_limit_pct = cfg.disk_usage_limit_pct,
        .min_free_disk_mb = cfg.min_free_disk_mb,
        .fsync_interval_s = cfg.fsync_interval_s,
        .status_interval_s = cfg.status_interval_s,
        .topics = cfg.topics,
    };

    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    try buf.writer.print("{f}", .{std.json.fmt(out, .{ .whitespace = .indent_2 })});

    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();
    try file.writeAll(buf.written());
}
