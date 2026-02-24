const std = @import("std");
const c = @import("c.zig").c;
const schema = @import("schema.zig");
const tb = @import("topic_buffer.zig");
const mcap = @import("mcap_writer.zig");

const FlatField = schema.FlatField;
const TopicSpec = tb.TopicSpec;
const TopicBufferPool = tb.TopicBufferPool;
const IpcWriterPool = mcap.IpcWriterPool;

// ---------------------------------------------------------------------------
// Config parsing
// ---------------------------------------------------------------------------

const TopicConfig = struct {
    type_name: []const u8,
    topic_name: []const u8,
};

fn parseConfig(allocator: std.mem.Allocator, path: []const u8) ![]TopicConfig {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const contents = try file.readToEndAlloc(allocator, 64 * 1024);

    var list: std.ArrayList(TopicConfig) = .empty;
    var lines = std.mem.splitScalar(u8, contents, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \r\t");
        if (trimmed.len == 0 or trimmed[0] == '#') continue;
        var parts = std.mem.splitScalar(u8, trimmed, ':');
        const type_name = parts.next() orelse continue;
        const topic_name = parts.next() orelse continue;
        try list.append(allocator, .{
            .type_name = try allocator.dupe(u8, type_name),
            .topic_name = try allocator.dupe(u8, topic_name),
        });
    }
    return list.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

const Subscription = struct {
    sub: c.rcl_subscription_t,
    topic_name: []const u8,
    /// Pointer into TopicBufferPool — no ownership
    buf: *tb.TopicBuffer,
    /// Type support handle — kept alive for the life of the subscription
    type_support: *const c.rosidl_message_type_support_t,
    /// Allocated message struct via type support init_function
    msg_buf: []u8,
    /// Size of the message struct in bytes (from introspection)
    msg_size: usize,
    /// Flat field layout — offsets and strides into msg_buf
    fields: []const FlatField,
};

fn initSubscription(
    allocator: std.mem.Allocator,
    node: *c.rcl_node_t,
    cfg: TopicConfig,
    buf: *tb.TopicBuffer,
    fields: []const FlatField,
    type_support: *const c.rosidl_message_type_support_t,
) !Subscription {
    // Allocate and zero-init the message struct using the type support
    // init_function — this is the same struct rcl_take will fill in
    const intro_ts: *const c.rosidl_typesupport_introspection_c__MessageMembers =
        @ptrCast(@alignCast(type_support.data));

    const msg_size = intro_ts.size_of_;
    const msg_buf = try allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(8), msg_size);
    @memset(msg_buf, 0);

    // Call the ROS-generated init function to set up any internal allocators
    // (e.g. for dynamic arrays / strings inside the struct)
    if (intro_ts.init_function) |init_fn| {
        _ = init_fn(msg_buf.ptr, c.ROSIDL_RUNTIME_C_MSG_INIT_DEFAULTS_ONLY);
    }

    var sub = c.rcl_get_zero_initialized_subscription();
    const opts = c.rcl_subscription_get_default_options();

    const topic_z = try allocator.dupeZ(u8, cfg.topic_name);
    defer allocator.free(topic_z);

    const ret = c.rcl_subscription_init(&sub, node, type_support, topic_z.ptr, &opts);
    if (ret != c.RCL_RET_OK) return error.SubscriptionInitFailed;

    return .{
        .sub = sub,
        .topic_name = cfg.topic_name,
        .buf = buf,
        .type_support = type_support,
        .msg_buf = msg_buf,
        .msg_size = msg_size,
        .fields = fields,
    };
}

fn finiSubscription(sub: *Subscription, node: *c.rcl_node_t) void {
    // Call fini_function to free any heap inside the message struct
    const intro_ts: *const c.rosidl_typesupport_introspection_c__MessageMembers =
        @ptrCast(@alignCast(sub.type_support.data));
    if (intro_ts.fini_function) |fini_fn| {
        _ = fini_fn(sub.msg_buf.ptr);
    }
    _ = c.rcl_subscription_fini(&sub.sub, node);
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

pub const Pipeline = struct {
    arena: std.heap.ArenaAllocator,
    node: *c.rcl_node_t,
    subs: []Subscription,
    buf_pool: TopicBufferPool,
    ipc_pool: IpcWriterPool,
    wait_set: c.rcl_wait_set_t,
    out_dir: std.fs.Dir,

    pub fn init(
        backing: std.mem.Allocator,
        node: *c.rcl_node_t,
        config_path: []const u8,
        out_path: []const u8,
    ) !Pipeline {
        var arena = std.heap.ArenaAllocator.init(backing);
        const allocator = arena.allocator();

        // 1. Parse config
        const configs = try parseConfig(allocator, config_path);

        // 2. Flatten schemas + load type supports
        const specs = try allocator.alloc(TopicSpec, configs.len);
        const ts_handles = try allocator.alloc(*const c.rosidl_message_type_support_t, configs.len);

        for (configs, 0..) |cfg, i| {
            var fields: std.ArrayList(FlatField) = .empty;
            // flattenMessageType now returns the type_support handle too
            const ts = try schema.flattenMessageType(allocator, cfg.type_name.ptr, "", &fields);
            ts_handles[i] = ts;
            specs[i] = .{
                .name = cfg.topic_name,
                .type_name = cfg.type_name,
                .fields = try fields.toOwnedSlice(allocator),
            };
        }

        // 3. Init topic buffer pool
        var buf_pool = try TopicBufferPool.init(allocator, specs);

        // 4. Open output dir, init IPC writer pool
        const out_dir = try std.fs.cwd().makeOpenPath(out_path, .{});
        const ipc_pool = try IpcWriterPool.init(allocator, out_dir, &buf_pool);

        // 5. Init subscriptions
        const subs = try allocator.alloc(Subscription, configs.len);
        for (configs, 0..) |cfg, i| {
            const buf = buf_pool.get(cfg.topic_name) orelse return error.TopicBufferNotFound;
            subs[i] = try initSubscription(
                allocator,
                node,
                cfg,
                buf,
                specs[i].fields,
                ts_handles[i],
            );
        }

        // 6. Init wait set
        var wait_set = c.rcl_get_zero_initialized_wait_set();
        const ctx = c.rcl_get_zero_initialized_context();
        const newCtx = @constCast(&ctx);
        const ret = c.rcl_wait_set_init(
            &wait_set,
            subs.len,
            0,
            0,
            0,
            0,
            0,
            newCtx,
            c.rcutils_get_default_allocator(),
        );
        if (ret != c.RCL_RET_OK) return error.WaitSetInitFailed;

        return .{
            .arena = arena,
            .node = node,
            .subs = subs,
            .buf_pool = buf_pool,
            .ipc_pool = ipc_pool,
            .wait_set = wait_set,
            .out_dir = out_dir,
        };
    }

    pub fn deinit(self: *Pipeline) void {
        // Final flush — drain any partial batches below the threshold
        self.ipc_pool.forceFlushAll(&self.buf_pool) catch |err| {
            std.debug.print("Warning: final flush failed: {}\n", .{err});
        };
        for (self.subs) |*sub| finiSubscription(sub, self.node);
        _ = c.rcl_wait_set_fini(&self.wait_set);
        self.ipc_pool.finish();
        self.buf_pool.deinit();
        self.out_dir.close();
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
                try self.ipc_pool.flushAll(&self.buf_pool);
                continue;
            }
            if (ret != c.RCL_RET_OK) return error.WaitFailed;

            // Receive from each ready subscription
            for (self.subs, 0..) |*sub, i| {
                if (self.wait_set.subscriptions[i] == null) continue;
                try self.receiveOne(sub);
            }

            // Flush topics that have hit the message threshold
            try self.ipc_pool.flushAll(&self.buf_pool);
        }
    }

    fn receiveOne(self: *Pipeline, sub: *Subscription) !void {
        _ = self;
        var msg_info: c.rmw_message_info_t = undefined;

        // rcl_take fills sub.msg_buf in-place — the C struct is already
        // allocated and init'd, rcl just overwrites its fields
        const ret = c.rcl_take(&sub.sub, sub.msg_buf.ptr, &msg_info, null);
        if (ret == c.RCL_RET_SUBSCRIPTION_TAKE_FAILED) return;
        if (ret != c.RCL_RET_OK) return error.TakeFailed;

        // Extract each flat field by offset directly from the C struct bytes
        for (sub.fields) |field| {
            const start = field.offset;
            const end = start + field.stride();
            if (end > sub.msg_buf.len) {
                std.debug.print("Warning: field '{s}' offset out of bounds, skipping\n", .{field.name});
                continue;
            }
            try sub.buf.columns[fieldIndex(sub, field)].push(sub.msg_buf[start..end]);
        }
        sub.buf.message_count += 1;
    }
};

/// Returns the column index for a field — fields and columns are built in the
/// same order from the same FlatField slice so this is a direct index lookup.
fn fieldIndex(sub: *const Subscription, field: FlatField) usize {
    for (sub.fields, 0..) |f, i| {
        if (f.offset == field.offset) return i;
    }
    unreachable; // field must exist — same slice
}
