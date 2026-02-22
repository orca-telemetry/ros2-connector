const std = @import("std");
const c = @import("c.zig").c;
const Pipeline = @import("pipeline.zig").Pipeline;

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

    // Parse CLI args: ros_recorder <config_path> <out_dir>
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        std.debug.print(
            "Usage: {s} <topics.conf> <output_dir>\n",
            .{args[0]},
        );
        return error.BadArgs;
    }
    const config_path = args[1];
    const out_path = args[2];

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

    std.debug.print("Node initialised. Loading config from '{s}'...\n", .{config_path});

    // --- Pipeline init ---
    var pipeline = try Pipeline.init(allocator, &node, config_path, out_path);
    defer pipeline.deinit();

    std.debug.print("Writing Arrow IPC streams to '{s}'\n", .{out_path});
    std.debug.print("Press Ctrl+C to stop.\n", .{});

    // --- Main loop — runs until SIGINT/SIGTERM ---
    try pipeline.runUntil(&g_running);

    std.debug.print("Shutdown signal received. Flushing and closing files...\n", .{});
    // pipeline.deinit() flushes and closes all IPC writers via defer above
}
