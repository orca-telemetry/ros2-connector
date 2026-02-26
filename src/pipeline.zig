const std = @import("std");
const c = @import("c.zig").c;
const tb = @import("topic_buffer.zig");
const mcap = @import("mcap_writer.zig");
const storage = @import("storage.zig");
const status = @import("status.zig");
const Config = @import("config.zig").Config;

const TopicSpec = tb.TopicSpec;
const MessageBufferPool = tb.MessageBufferPool;
const McapWriterPool = mcap.McapWriterPool;

// ---------------------------------------------------------------------------
// Subscription — uses rcl_take_serialized_message instead of rcl_take
// ---------------------------------------------------------------------------

const Subscription = struct {
    sub: c.rcl_subscription_t,
    topic_name: []const u8,
    type_name: []const u8,
    buf: *tb.MessageBuffer,
    serialized_msg: c.rcutils_uint8_array_t,
};

fn initSubscription(
    allocator: std.mem.Allocator,
    node: *c.rcl_node_t,
    cfg: Config.TopicEntry,
    buf: *tb.MessageBuffer,
    type_support: *const c.rosidl_message_type_support_t,
) !Subscription {
    var sub = c.rcl_get_zero_initialized_subscription();
    const opts = c.rcl_subscription_get_default_options();

    const topic_z = try allocator.dupeZ(u8, cfg.topic_name);
    defer allocator.free(topic_z);

    const ret = c.rcl_subscription_init(&sub, node, type_support, topic_z.ptr, &opts);
    if (ret != c.RCL_RET_OK) return error.SubscriptionInitFailed;

    // Initialize a reusable serialized message buffer
    var serialized_msg = c.rcutils_get_zero_initialized_uint8_array();
    const alloc_ret = c.rcutils_uint8_array_init(&serialized_msg, 0, &c.rcutils_get_default_allocator());
    if (alloc_ret != c.RCUTILS_RET_OK) return error.SerializedMsgInitFailed;

    return .{
        .sub = sub,
        .topic_name = cfg.topic_name,
        .type_name = cfg.type_name,
        .buf = buf,
        .serialized_msg = serialized_msg,
    };
}

fn finiSubscription(sub: *Subscription, node: *c.rcl_node_t) void {
    _ = c.rcutils_uint8_array_fini(&sub.serialized_msg);
    _ = c.rcl_subscription_fini(&sub.sub, node);
}

// ---------------------------------------------------------------------------
// Type support loading via rosidl_typesupport_c
// ---------------------------------------------------------------------------

/// Load the generic type support for a ROS message type.
/// type_name format: "package_name/msg/MessageType"
fn loadTypeSupport(
    allocator: std.mem.Allocator,
    type_name: []const u8,
) !*const c.rosidl_message_type_support_t {
    // Parse "pkg/msg/Type" into components
    var parts = std.mem.splitScalar(u8, type_name, '/');
    const pkg = parts.next() orelse return error.InvalidTypeName;
    const msg_ns = parts.next() orelse return error.InvalidTypeName;
    const msg_type = parts.next() orelse return error.InvalidTypeName;

    _ = msg_ns; // always "msg"

    // Build the type support function name:
    // rosidl_typesupport_c__get_message_type_support_handle__<pkg>__msg__<type>
    const func_name = try std.fmt.allocPrintSentinel(
        allocator,
        "rosidl_typesupport_c__get_message_type_support_handle__{s}__msg__{s}",
        .{ pkg, msg_type },
        0,
    );

    // Load the shared library containing the type support
    const lib_name = try std.fmt.allocPrintSentinel(
        allocator,
        "lib{s}__rosidl_typesupport_c.so",
        .{pkg},
        0,
    );

    const handle = std.c.dlopen(lib_name.ptr, .{ .LAZY = true }) orelse
        return error.TypeSupportLibNotFound;

    const sym = std.c.dlsym(handle, func_name.ptr) orelse
        return error.TypeSupportSymNotFound;

    const get_ts: *const fn () callconv(.c) *const c.rosidl_message_type_support_t = @ptrCast(sym);
    return get_ts();
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

pub const Pipeline = struct {
    arena: std.heap.ArenaAllocator,
    node: *c.rcl_node_t,
    subs: []Subscription,
    buf_pool: MessageBufferPool,
    writer_pool: McapWriterPool,
    wait_set: c.rcl_wait_set_t,
    log_dir_z: [*:0]const u8,
    disk_usage_limit_pct: u32,
    min_free_disk_mb: u32,
    status_reporter: status.StatusReporter,

    pub fn init(
        backing: std.mem.Allocator,
        node: *c.rcl_node_t,
        context: *c.rcl_context_t,
        cfg: *const Config,
    ) !Pipeline {
        var arena = std.heap.ArenaAllocator.init(backing);
        const allocator = arena.allocator();

        const topics = cfg.topics;

        // 1. Build topic specs and load type supports
        const specs = try allocator.alloc(TopicSpec, topics.len);
        defer allocator.free(specs);
        const ts_handles = try allocator.alloc(*const c.rosidl_message_type_support_t, topics.len);
        defer allocator.free(ts_handles);

        for (topics, 0..) |topic, i| {
            ts_handles[i] = try loadTypeSupport(allocator, topic.type_name);
            specs[i] = .{
                .name = topic.topic_name,
                .type_name = topic.type_name,
            };
        }

        // 2. Init message buffer pool
        const write_buffer_bytes: usize = @as(usize, cfg.write_buffer_mb) * 1024 * 1024;
        var buf_pool = try MessageBufferPool.init(allocator, specs, write_buffer_bytes);

        // 3. Init MCAP writer pool
        const max_duration_ns: u64 = @as(u64, cfg.max_bag_duration_s) * std.time.ns_per_s;
        const max_size_bytes: u64 = @as(u64, cfg.max_bag_size_mb) * 1024 * 1024;
        const fsync_interval_ns: u64 = @as(u64, cfg.fsync_interval_s) * std.time.ns_per_s;
        const writer_pool = try McapWriterPool.init(
            allocator,
            cfg.log_directory,
            cfg.robot_id,
            cfg.software_version,
            &buf_pool,
            max_duration_ns,
            max_size_bytes,
            fsync_interval_ns,
        );

        // 4. Init subscriptions
        const subs = try allocator.alloc(Subscription, topics.len);
        for (topics, 0..) |topic, i| {
            const buf = buf_pool.get(topic.topic_name) orelse return error.TopicBufferNotFound;
            subs[i] = try initSubscription(
                allocator,
                node,
                topic,
                buf,
                ts_handles[i],
            );
        }

        // 6. Init wait set
        var wait_set = c.rcl_get_zero_initialized_wait_set();
        const ret = c.rcl_wait_set_init(
            &wait_set,
            subs.len,
            0,
            0,
            0,
            0,
            0,
            context,
            c.rcutils_get_default_allocator(),
        );
        if (ret != c.RCL_RET_OK) return error.WaitSetInitFailed;

        const log_dir_z = try allocator.dupeZ(u8, cfg.log_directory);

        const status_reporter = status.StatusReporter.init(
            allocator,
            cfg.software_version,
            log_dir_z.ptr,
            cfg.status_interval_s,
        );

        return .{
            .arena = arena,
            .node = node,
            .subs = subs,
            .buf_pool = buf_pool,
            .writer_pool = writer_pool,
            .wait_set = wait_set,
            .log_dir_z = log_dir_z.ptr,
            .disk_usage_limit_pct = cfg.disk_usage_limit_pct,
            .min_free_disk_mb = cfg.min_free_disk_mb,
            .status_reporter = status_reporter,
        };
    }

    pub fn deinit(self: *Pipeline) void {
        const allocator = self.arena.allocator();
        // Final flush — drain any remaining messages
        self.writer_pool.forceFlushAll(&self.buf_pool, allocator) catch |err| {
            std.debug.print("Warning: final flush failed: {}\n", .{err});
        };
        for (self.subs) |*sub| finiSubscription(sub, self.node);
        _ = c.rcl_wait_set_fini(&self.wait_set);
        self.writer_pool.finish(allocator);
        self.buf_pool.deinit(allocator);
        self.arena.deinit();
    }

    // ---------------------------------------------------------------------------
    // Main loop
    // ---------------------------------------------------------------------------

    pub fn runUntil(self: *Pipeline, running: *std.atomic.Value(bool)) !void {
        std.debug.print("Pipeline running, recording {} topics...\n", .{self.subs.len});

        while (running.load(.acquire)) {
            // Arm wait set
            _ = c.rcl_wait_set_clear(&self.wait_set);
            for (self.subs) |*sub| {
                _ = c.rcl_wait_set_add_subscription(&self.wait_set, &sub.sub, null);
            }

            // Block with 100ms timeout so we flush on quiet topics
            const ret = c.rcl_wait(&self.wait_set, 100 * std.time.ns_per_ms);
            if (ret == c.RCL_RET_TIMEOUT) {
                try self.writer_pool.flushAll(&self.buf_pool, self.arena.allocator());
                self.maybeRotate();
                self.writer_pool.periodicFsync();
                self.status_reporter.maybeSend(self.arena.allocator(), &self.writer_pool, &self.buf_pool);
                continue;
            }
            if (ret != c.RCL_RET_OK) return error.WaitFailed;

            // Receive from each ready subscription
            for (self.subs, 0..) |*sub, i| {
                if (self.wait_set.subscriptions[i] == null) continue;
                self.receiveOne(sub);
            }

            // Flush topics that have hit the message threshold
            try self.writer_pool.flushAll(&self.buf_pool, self.arena.allocator());

            // Check if file rotation is needed (duration or size limit)
            self.maybeRotate();
            self.writer_pool.periodicFsync();
            self.status_reporter.maybeSend(self.arena.allocator(), &self.writer_pool, &self.buf_pool);
        }
    }

    fn maybeRotate(self: *Pipeline) void {
        if (!self.writer_pool.shouldRotate()) return;

        // Capture bytes from the completed file before rotation resets the counter
        const old_bytes = self.writer_pool.bytes_written;
        const old_path = self.writer_pool.file_path;

        self.writer_pool.rotate(self.arena.allocator()) catch |err| {
            std.log.err("File rotation failed: {}", .{err});
            return;
        };
        std.log.info("Rotated MCAP file: {s} -> {s}", .{ old_path, self.writer_pool.file_path });

        self.status_reporter.addBytesWritten(old_bytes);
        self.runCleanup();
    }

    fn runCleanup(self: *Pipeline) void {
        const deleted = storage.cleanupOldFiles(
            self.arena.allocator(),
            self.log_dir_z,
            self.disk_usage_limit_pct,
            self.min_free_disk_mb,
        ) catch |err| {
            std.log.err("Disk cleanup failed: {}", .{err});
            return;
        };
        if (deleted > 0) {
            std.log.info("Disk cleanup: deleted {} old recording(s)", .{deleted});
        }
    }

    fn receiveOne(self: *Pipeline, sub: *Subscription) void {
        var msg_info: c.rmw_message_info_t = undefined;

        const ret = c.rcl_take_serialized_message(
            &sub.sub,
            &sub.serialized_msg,
            &msg_info,
            null,
        );
        if (ret == c.RCL_RET_SUBSCRIPTION_TAKE_FAILED) return;
        if (ret != c.RCL_RET_OK) {
            std.debug.print("Warning: rcl_take_serialized_message failed for {s}\n", .{sub.topic_name});
            return;
        }

        // Use source_timestamp from the publisher; fall back to received_timestamp
        // if the DDS implementation didn't fill it.
        const timestamp_ns: u64 = if (msg_info.source_timestamp > 0)
            @intCast(msg_info.source_timestamp)
        else if (msg_info.received_timestamp > 0)
            @intCast(msg_info.received_timestamp)
        else
            @intCast(std.time.nanoTimestamp());

        // Copy the serialized bytes into the message buffer
        const data = sub.serialized_msg.buffer[0..sub.serialized_msg.buffer_length];
        sub.buf.push(self.arena.allocator(), data, timestamp_ns) catch |err| {
            std.debug.print("Warning: buffer push failed for {s}: {}\n", .{ sub.topic_name, err });
        };
    }
};
