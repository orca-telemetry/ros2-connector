const std = @import("std");
const c = @import("c.zig").c;
const Pipeline = @import("pipeline.zig").Pipeline;
const mcap = @import("mcap_writer.zig");
const cfg_mod = @import("config.zig");
const config = @import("configure/config.zig");
const provision = @import("configure/provision.zig");
const discovery = @import("configure/discovery.zig");

extern "C" fn mcap_recover(src: [*:0]const u8, dst: [*:0]const u8) c_int;

// ---------------------------------------------------------------------------
// Signal handling — graceful shutdown on SIGINT / SIGTERM
// ---------------------------------------------------------------------------

var g_running: std.atomic.Value(bool) = .init(true);

fn handleSignal(sig: c_int) callconv(.c) void {
    _ = sig;
    g_running.store(false, .release);
}

fn installSignalHandlers() void {
    var sa = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        printUsage();
        return;
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "provision")) {
        if (args.len < 4 or !std.mem.eql(u8, args[2], "--token")) {
            std.debug.print("Error: provision requires --token <value>\n", .{});
            return;
        }
        try provision.provisionRobot(allocator, args[3]);
    } else if (std.mem.eql(u8, command, "discover")) {
        // Initialize RCL
        var context = c.rcl_get_zero_initialized_context();
        var init_options = c.rcl_get_zero_initialized_init_options();

        const alloc = c.rcutils_get_default_allocator();
        var ret = c.rcl_init_options_init(&init_options, alloc);
        if (ret != c.RCL_RET_OK) {
            std.debug.print("Failed to initialize init_options\n", .{});
            return error.RclInitFailed;
        }
        defer _ = c.rcl_init_options_fini(&init_options);

        ret = c.rcl_init(0, null, &init_options, &context);
        if (ret != c.RCL_RET_OK) {
            std.debug.print("Failed to initialize rcl\n", .{});
            return error.RclInitFailed;
        }
        defer _ = c.rcl_shutdown(&context);
        defer _ = c.rcl_context_fini(&context);

        // Create node
        var node = c.rcl_get_zero_initialized_node();
        const node_name = "network_discovery_node_zig";
        const node_namespace = "";

        var node_options = c.rcl_node_get_default_options();
        ret = c.rcl_node_init(&node, node_name, node_namespace, &context, &node_options);
        if (ret != c.RCL_RET_OK) {
            std.debug.print("Failed to initialize node\n", .{});
            return error.NodeInitFailed;
        }
        defer _ = c.rcl_node_fini(&node);

        std.debug.print("Network Discovery Node (Zig) started\n", .{});

        // Wait for DDS discovery to settle
        std.debug.print("Waiting for DDS discovery...\n", .{});
        std.Thread.sleep(2 * std.time.ns_per_s);

        try discovery.runDiscovery(allocator, &node);
    } else if (std.mem.eql(u8, command, "listen")) {
        if (args.len < 3) {
            std.debug.print("Error: listen requires a config file path\n", .{});
            std.debug.print("Usage: orca listen <config.json>\n", .{});
            return;
        }

        // --- Load and validate config ---
        const parsed = cfg_mod.load(allocator, args[2]) catch |err| {
            std.debug.print("Error: failed to load config '{s}': {}\n", .{ args[2], err });
            return;
        };
        defer parsed.deinit();

        const cfg = cfg_mod.resolve(parsed.value);
        try cfg_mod.validate(&cfg);

        // Recover any .incomplete files from previous crashes
        recoverIncompleteFiles(allocator, cfg.log_directory);

        installSignalHandlers();

        // --- RCL init ---
        var rcl_ctx = c.rcl_get_zero_initialized_context();
        var init_opts = c.rcl_get_zero_initialized_init_options();

        var ret = c.rcl_init_options_init(&init_opts, c.rcutils_get_default_allocator());
        if (ret != c.RCL_RET_OK) return error.RclInitOptionsFailed;
        defer _ = c.rcl_init_options_fini(&init_opts);

        ret = c.rcl_init(0, null, &init_opts, &rcl_ctx);
        if (ret != c.RCL_RET_OK) return error.RclInitFailed;
        defer _ = c.rcl_shutdown(&rcl_ctx);
        defer _ = c.rcl_context_fini(&rcl_ctx);

        // --- Node init ---
        var node = c.rcl_get_zero_initialized_node();
        var node_opts = c.rcl_node_get_default_options();

        ret = c.rcl_node_init(&node, "ros_recorder", "", &rcl_ctx, &node_opts);
        if (ret != c.RCL_RET_OK) return error.RclNodeInitFailed;
        defer _ = c.rcl_node_fini(&node);

        std.debug.print("Node initialised. Loading config from '{s}'...\n", .{args[2]});

        // --- Pipeline init ---
        var pipeline = try Pipeline.init(allocator, &node, &cfg);
        defer pipeline.deinit();

        std.debug.print("Recording to MCAP in '{s}' (robot: {s})\n", .{ cfg.log_directory, cfg.robot_id });
        std.debug.print("Press Ctrl+C to stop.\n", .{});

        // --- Main loop — runs until SIGINT/SIGTERM ---
        try pipeline.runUntil(&g_running);

        std.debug.print("Shutdown signal received. Flushing and closing files...\n", .{});
    } else {
        printUsage();
    }
}

fn recoverIncompleteFiles(allocator: std.mem.Allocator, log_dir: []const u8) void {
    var dir = std.fs.cwd().openDir(log_dir, .{ .iterate = true }) catch |err| {
        std.log.warn("Cannot open log directory for recovery scan: {}", .{err});
        return;
    };
    defer dir.close();

    var iter = dir.iterate();
    while (iter.next() catch null) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".incomplete")) continue;

        const incomplete_path = std.fmt.allocPrintSentinel(
            allocator,
            "{s}/{s}",
            .{ log_dir, entry.name },
            0,
        ) catch continue;

        // Derive final .mcap name by stripping ".incomplete"
        const stem = entry.name[0 .. entry.name.len - ".incomplete".len];
        const final_path = std.fmt.allocPrintSentinel(
            allocator,
            "{s}/{s}",
            .{ log_dir, stem },
            0,
        ) catch continue;

        const tmp_path = std.fmt.allocPrintSentinel(
            allocator,
            "{s}/{s}.recovering",
            .{ log_dir, stem },
            0,
        ) catch continue;

        std.log.info("Recovering incomplete file: {s}", .{entry.name});

        const result = mcap_recover(incomplete_path.ptr, tmp_path.ptr);
        if (result >= 0) {
            // Success (0 = clean, 1 = partial but usable)
            if (result == 1) {
                std.log.warn("Partial recovery for {s} (some messages lost)", .{entry.name});
            }
            // Rename recovered file to final name
            std.fs.cwd().renameZ(tmp_path, final_path) catch |err| {
                std.log.err("Failed to rename recovered file: {}", .{err});
                continue;
            };
            // Delete the .incomplete file
            std.fs.cwd().deleteFileZ(incomplete_path) catch |err| {
                std.log.warn("Failed to delete {s}: {}", .{ incomplete_path, err });
            };
            // Write SHA-256 checksum
            mcap.writeSha256File(allocator, final_path) catch |err| {
                std.log.err("Failed to write SHA-256 for recovered file: {}", .{err});
            };
            std.log.info("Recovered: {s}", .{stem});
        } else {
            // Recovery failed — rename to .unrecoverable
            const unrec_path = std.fmt.allocPrintSentinel(
                allocator,
                "{s}/{s}.unrecoverable",
                .{ log_dir, entry.name },
                0,
            ) catch continue;
            std.fs.cwd().renameZ(incomplete_path, unrec_path) catch |err| {
                std.log.err("Failed to rename to .unrecoverable: {}", .{err});
            };
            // Clean up failed tmp file
            std.fs.cwd().deleteFileZ(tmp_path) catch {};
            std.log.err("Unrecoverable: {s}", .{entry.name});
        }
    }
}

fn printUsage() void {
    std.debug.print(
        \\Usage: orca <command> [options]
        \\
        \\Commands:
        \\  provision --token <T>   Generate keys and register with Orca
        \\  discover                Scan ROS 2 network and emit schema to Orca
        \\  listen <config.json>    Listen to ROS 2 topics and save data to .mcap files
        \\  sync                    Sync local robot config with Orca cloud
        \\
    , .{});
}

test {
    _ = @import("main_test.zig");
}
