const std = @import("std");
const c = @cImport({
    @cInclude("rcl/rcl.h");
    @cInclude("rcl/error_handling.h");
    @cInclude("rcl/node.h");
    @cInclude("rcl/graph.h");
    @cInclude("rmw/rmw.h");
    @cInclude("rosidl_runtime_c/message_type_support_struct.h");
    @cInclude("rosidl_runtime_c/string.h");
    @cInclude("rosidl_runtime_c/primitives_sequence.h");
});

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize RCL
    var context = c.rcl_get_zero_initialized_context();
    var init_options = c.rcl_get_zero_initialized_init_options();

    var ret = c.rcl_init_options_init(&init_options, allocator);
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

    // Discovery loop
    var count: usize = 0;
    while (count < 10) : (count += 1) {
        std.time.sleep(5 * std.time.ns_per_s);

        try discoverNetwork(allocator, &node);
    }
}

fn discoverNetwork(allocator: std.mem.Allocator, node: *c.rcl_node_t) !void {
    std.debug.print("\n", .{});
    std.debug.print("================================================================================\n", .{});
    std.debug.print("ROS2 NETWORK DISCOVERY (Zig)\n", .{});
    std.debug.print("================================================================================\n", .{});

    // Print RMW implementation
    try printRmwInfo();

    // Discover nodes
    try discoverNodes(allocator, node);

    // Discover topics
    try discoverTopics(allocator, node);
}

fn printRmwInfo() !void {
    const rmw_impl = c.rmw_get_implementation_identifier();
    std.debug.print("\nRMW Implementation: {s}\n", .{rmw_impl});
}

fn discoverNodes(allocator: std.mem.Allocator, node: *c.rcl_node_t) !void {
    var node_names = c.rcl_get_zero_initialized_names_and_types();
    defer _ = c.rcl_names_and_types_fini(&node_names);

    const alloc = c.rcutils_get_default_allocator();
    const ret = c.rcl_get_node_names(node, alloc, &node_names);

    if (ret != c.RCL_RET_OK) {
        std.debug.print("Failed to get node names\n", .{});
        return;
    }

    std.debug.print("\nDiscovered {} nodes:\n", .{node_names.names.size});

    var i: usize = 0;
    while (i < node_names.names.size) : (i += 1) {
        const name = node_names.names.data[i];
        std.debug.print("  - {s}\n", .{name});
    }
}

fn discoverTopics(allocator: std.mem.Allocator, node: *c.rcl_node_t) !void {
    var topic_names_and_types = c.rcl_get_zero_initialized_names_and_types();
    defer _ = c.rcl_names_and_types_fini(&topic_names_and_types);

    const alloc = c.rcutils_get_default_allocator();
    const ret = c.rcl_get_topic_names_and_types(node, &alloc, false, &topic_names_and_types);

    if (ret != c.RCL_RET_OK) {
        std.debug.print("Failed to get topic names and types\n", .{});
        return;
    }

    std.debug.print("\n================================================================================\n", .{});
    std.debug.print("Discovered {} topics:\n", .{topic_names_and_types.names.size});
    std.debug.print("================================================================================\n\n", .{});

    var i: usize = 0;
    while (i < topic_names_and_types.names.size) : (i += 1) {
        const topic_name = topic_names_and_types.names.data[i];
        std.debug.print("Topic: {s}\n", .{topic_name});

        // Get types for this topic
        const types = &topic_names_and_types.types[i];
        var j: usize = 0;
        while (j < types.size) : (j += 1) {
            const type_name = types.data[j];
            std.debug.print("  Type: {s}\n", .{type_name});

            // Analyze the type
            analyzeMessageType(type_name);
        }

        // Get publishers and subscribers
        try getPublishersAndSubscribers(allocator, node, topic_name);

        std.debug.print("\n", .{});
    }
}

fn analyzeMessageType(type_name: [*c]const u8) void {
    // Parse type name to determine if it's POD
    // For this simplified version, we'll check against known primitive types
    const type_str = std.mem.span(type_name);

    const is_pod = isPodType(type_str);
    const has_nested = !is_pod and !isUnknown(type_str);

    std.debug.print("    Is POD: {}\n", .{is_pod});
    std.debug.print("    Has nested types: {}\n", .{has_nested});

    // Note: Full introspection of message fields requires type support introspection
    // which is more complex in C/Zig. For a complete implementation, you'd need to:
    // 1. Load the type support library dynamically
    // 2. Get the message type support structure
    // 3. Introspect the fields using rosidl_typesupport_introspection_c
    std.debug.print("    Field introspection: Requires rosidl_typesupport_introspection_c\n", .{});
}

fn isPodType(type_name: []const u8) bool {
    // Common POD types in ROS2
    const pod_types = [_][]const u8{
        "std_msgs/msg/String",
        "std_msgs/msg/Int32",
        "std_msgs/msg/Int64",
        "std_msgs/msg/UInt32",
        "std_msgs/msg/UInt64",
        "std_msgs/msg/Float32",
        "std_msgs/msg/Float64",
        "std_msgs/msg/Bool",
        "std_msgs/msg/Byte",
        "std_msgs/msg/Char",
    };

    for (pod_types) |pod_type| {
        if (std.mem.eql(u8, type_name, pod_type)) {
            return true;
        }
    }

    return false;
}

fn isUnknown(type_name: []const u8) bool {
    _ = type_name;
    return false;
}

fn getPublishersAndSubscribers(allocator: std.mem.Allocator, node: *c.rcl_node_t, topic_name: [*c]const u8) !void {
    _ = allocator;

    const alloc = c.rcutils_get_default_allocator();

    // Get publishers
    var pub_info = c.rcl_get_zero_initialized_topic_endpoint_info_array();
    defer _ = c.rcl_topic_endpoint_info_array_fini(&pub_info, &alloc);

    var ret = c.rcl_get_publishers_info_by_topic(node, &alloc, topic_name, false, &pub_info);
    if (ret == c.RCL_RET_OK) {
        std.debug.print("  Publishers: {}\n", .{pub_info.size});

        var i: usize = 0;
        while (i < pub_info.size) : (i += 1) {
            const info = &pub_info.info_array[i];
            std.debug.print("    - Node: {s}, Namespace: {s}\n", .{
                info.node_name,
                info.node_namespace,
            });
        }
    }

    // Get subscribers
    var sub_info = c.rcl_get_zero_initialized_topic_endpoint_info_array();
    defer _ = c.rcl_topic_endpoint_info_array_fini(&sub_info, &alloc);

    ret = c.rcl_get_subscriptions_info_by_topic(node, &alloc, topic_name, false, &sub_info);
    if (ret == c.RCL_RET_OK) {
        std.debug.print("  Subscribers: {}\n", .{sub_info.size});

        var i: usize = 0;
        while (i < sub_info.size) : (i += 1) {
            const info = &sub_info.info_array[i];
            std.debug.print("    - Node: {s}, Namespace: {s}\n", .{
                info.node_name,
                info.node_namespace,
            });
        }
    }
}
