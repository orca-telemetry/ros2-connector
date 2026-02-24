const std = @import("std");
const c = @import("../c.zig").c;
const config = @import("config.zig");
const constants = @import("constants.zig");
const http = std.http;
const crypto = std.crypto;

const JsonOutput = struct {
    robotId: []u8 = "", //  expect it to be filled in later
    topics: []TopicInfo,
    nodes: []NodeInfo,
    services: []ServiceInfo,
    rmw_implementation: []const u8,

    const TopicInfo = struct {
        name: []const u8,
        type_name: []const u8,
        schema: ?MessageSchema,
        publishers: []EndpointInfo,
        subscribers: []EndpointInfo,
    };

    const NodeInfo = struct {
        name: []const u8,
        namespace: []const u8,
    };

    const ServiceInfo = struct {
        name: []const u8,
        type_name: []const u8,
    };

    const EndpointInfo = struct {
        node_name: []const u8,
        node_namespace: []const u8,
    };

    const MessageSchema = struct {
        fields: []FieldInfo,
    };

    const FieldInfo = struct {
        name: []const u8,
        type_name: []const u8,
        is_array: bool,
        array_size: usize,
        is_upper_bound: bool,
    };
};

pub fn runDiscovery(allocator: std.mem.Allocator, node: *c.rcl_node_t) !void {
    // 1. Internal ROS 2 Discovery
    var discovery_data = try discoverNetworkToJson(allocator, node);
    defer freeJsonOutput(allocator, discovery_data);

    // 2. Initial stringify to create the signing bytes
    var sign_target: std.Io.Writer.Allocating = .init(allocator);
    defer sign_target.deinit();
    try sign_target.writer.print("{f}", .{std.json.fmt(discovery_data, .{})});

    // add in robot id
    const robotId = try config.ConfigStorage.getRobotId(allocator);
    defer allocator.free(robotId);
    discovery_data.robotId = robotId;

    // 6. Push to orca cloud
    try uploadDiscovery(allocator, discovery_data, robotId);

    // Optional: Also save a local copy for debugging
    const file = try std.fs.cwd().createFile("last_discovery_sent.json", .{});
    defer file.close();
    var debug_writer: std.Io.Writer.Allocating = .init(allocator);
    defer debug_writer.deinit();
    try debug_writer.writer.print("{f}", .{std.json.fmt(discovery_data, .{})});
    try file.writeAll(debug_writer.written());
}

fn discoverNetworkToJson(allocator: std.mem.Allocator, node: *c.rcl_node_t) !JsonOutput {
    const rmw_impl = c.rmw_get_implementation_identifier();

    // Discover nodes
    const nodes = try discoverNodes(allocator, node);

    // Discover topics
    const topics = try discoverTopicsWithSchemas(allocator, node);

    // Discover services
    const services = try discoverServicesInfo(allocator, node);

    return JsonOutput{
        .rmw_implementation = std.mem.span(rmw_impl),
        .nodes = nodes,
        .topics = topics,
        .services = services,
    };
}

fn discoverNodes(allocator: std.mem.Allocator, node: *c.rcl_node_t) ![]JsonOutput.NodeInfo {
    var node_names = c.rcutils_get_zero_initialized_string_array();
    var node_namespaces = c.rcutils_get_zero_initialized_string_array();
    defer {
        _ = c.rcutils_string_array_fini(&node_names);
        _ = c.rcutils_string_array_fini(&node_namespaces);
    }

    const alloc = c.rcutils_get_default_allocator();
    const ret = c.rcl_get_node_names(node, alloc, &node_names, &node_namespaces);

    if (ret != c.RCL_RET_OK) {
        return error.FailedToGetNodes;
    }

    var nodes = try allocator.alloc(JsonOutput.NodeInfo, node_names.size);

    var i: usize = 0;
    while (i < node_names.size) : (i += 1) {
        nodes[i] = .{
            .name = try allocator.dupe(u8, std.mem.span(node_names.data[i])),
            .namespace = try allocator.dupe(u8, std.mem.span(node_namespaces.data[i])),
        };
    }

    return nodes;
}

fn discoverTopicsWithSchemas(allocator: std.mem.Allocator, node: *c.rcl_node_t) ![]JsonOutput.TopicInfo {
    var topic_names_and_types = c.rcl_get_zero_initialized_names_and_types();
    defer _ = c.rcl_names_and_types_fini(&topic_names_and_types);

    var alloc = c.rcutils_get_default_allocator();
    const ret = c.rcl_get_topic_names_and_types(node, &alloc, false, &topic_names_and_types);

    if (ret != c.RCL_RET_OK) {
        return error.FailedToGetTopics;
    }

    var topics = try allocator.alloc(JsonOutput.TopicInfo, topic_names_and_types.names.size);

    var i: usize = 0;
    while (i < topic_names_and_types.names.size) : (i += 1) {
        const topic_name = topic_names_and_types.names.data[i];
        const types = &topic_names_and_types.types[i];
        const type_name: [*:0]const u8 = if (types.size > 0) types.data[0] else "";

        // Get publishers and subscribers
        const endpoints = try getTopicEndpoints(allocator, node, topic_name);

        // Introspect message schema
        const schema = introspectMessageType(allocator, type_name) catch null;

        topics[i] = .{
            .name = try allocator.dupe(u8, std.mem.span(topic_name)),
            .type_name = try allocator.dupe(u8, std.mem.span(type_name)),
            .schema = schema,
            .publishers = endpoints.publishers,
            .subscribers = endpoints.subscribers,
        };
    }

    return topics;
}

const TopicEndpoints = struct {
    publishers: []JsonOutput.EndpointInfo,
    subscribers: []JsonOutput.EndpointInfo,
};

fn getTopicEndpoints(allocator: std.mem.Allocator, node: *c.rcl_node_t, topic_name: [*c]const u8) !TopicEndpoints {
    var alloc = c.rcutils_get_default_allocator();

    // Get publishers
    var pub_info = c.rcl_get_zero_initialized_topic_endpoint_info_array();
    defer _ = c.rcl_topic_endpoint_info_array_fini(&pub_info, &alloc);

    var ret = c.rcl_get_publishers_info_by_topic(node, &alloc, topic_name, false, &pub_info);

    var publishers = try allocator.alloc(JsonOutput.EndpointInfo, if (ret == c.RCL_RET_OK) pub_info.size else 0);
    if (ret == c.RCL_RET_OK) {
        var i: usize = 0;
        while (i < pub_info.size) : (i += 1) {
            const info = &pub_info.info_array[i];
            publishers[i] = .{
                .node_name = try allocator.dupe(u8, std.mem.span(info.node_name)),
                .node_namespace = try allocator.dupe(u8, std.mem.span(info.node_namespace)),
            };
        }
    }

    // Get subscribers
    var sub_info = c.rcl_get_zero_initialized_topic_endpoint_info_array();
    defer _ = c.rcl_topic_endpoint_info_array_fini(&sub_info, &alloc);

    ret = c.rcl_get_subscriptions_info_by_topic(node, &alloc, topic_name, false, &sub_info);

    var subscribers = try allocator.alloc(JsonOutput.EndpointInfo, if (ret == c.RCL_RET_OK) sub_info.size else 0);
    if (ret == c.RCL_RET_OK) {
        var i: usize = 0;
        while (i < sub_info.size) : (i += 1) {
            const info = &sub_info.info_array[i];
            subscribers[i] = .{
                .node_name = try allocator.dupe(u8, std.mem.span(info.node_name)),
                .node_namespace = try allocator.dupe(u8, std.mem.span(info.node_namespace)),
            };
        }
    }

    return .{
        .publishers = publishers,
        .subscribers = subscribers,
    };
}

fn introspectMessageType(allocator: std.mem.Allocator, type_name_cstr: [*c]const u8) !JsonOutput.MessageSchema {
    const type_name = std.mem.span(type_name_cstr);

    // Parse type name: "package_name/msg/MessageName" -> package_name, MessageName
    var parts = std.mem.splitSequence(u8, type_name, "/");
    const package_name = parts.next() orelse return error.InvalidTypeName;
    _ = parts.next(); // Skip "msg"
    const message_name = parts.next() orelse return error.InvalidTypeName;

    // Build library name: libpackage_name__rosidl_typesupport_introspection_c.so
    const lib_name = try std.fmt.allocPrint(allocator, "lib{s}__rosidl_typesupport_introspection_c.so", .{package_name});
    defer allocator.free(lib_name);

    // Load type support library
    const handle = c.dlopen(lib_name.ptr, c.RTLD_LAZY);
    if (handle == null) {
        std.debug.print("Warning: Could not load type support for {s}: {s}\n", .{ type_name, c.dlerror() });
        return error.TypeSupportNotFound;
    }
    defer _ = c.dlclose(handle);

    // Get type support function: rosidl_typesupport_introspection_c__get_message_type_support_handle__package_name__msg__MessageName
    const func_name = try std.fmt.allocPrint(
        allocator,
        "rosidl_typesupport_introspection_c__get_message_type_support_handle__{s}__msg__{s}",
        .{ package_name, message_name },
    );
    defer allocator.free(func_name);

    const func_name_z = try allocator.dupeZ(u8, func_name);
    defer allocator.free(func_name_z);

    const get_ts_func = c.dlsym(handle, func_name_z.ptr);
    if (get_ts_func == null) {
        std.debug.print("Warning: Could not find type support function: {s}\n", .{func_name});
        return error.TypeSupportFunctionNotFound;
    }

    // Call the function to get type support
    const GetTypeSupportFunc = *const fn () callconv(.c) ?*const c.rosidl_message_type_support_t;
    const get_ts: GetTypeSupportFunc = @ptrCast(@alignCast(get_ts_func));
    const type_support = get_ts() orelse return error.TypeSupportNull;

    // Cast to introspection type support
    const intro_ts: *const c.rosidl_typesupport_introspection_c__MessageMembers = @ptrCast(@alignCast(type_support.data));

    // Extract field information
    var fields = try allocator.alloc(JsonOutput.FieldInfo, intro_ts.member_count_);

    var i: u32 = 0;
    while (i < intro_ts.member_count_) : (i += 1) {
        const member = intro_ts.members_ + i;
        fields[i] = .{
            .name = try allocator.dupe(u8, std.mem.span(member.*.name_)),
            .type_name = try getFieldTypeName(member),
            .is_array = member.*.is_array_,
            .array_size = member.*.array_size_,
            .is_upper_bound = member.*.is_upper_bound_,
        };
    }
    return JsonOutput.MessageSchema{
        .fields = fields,
    };
}

fn getFieldTypeName(member: *const c.rosidl_typesupport_introspection_c__MessageMember) ![]const u8 {
    return switch (member.type_id_) {
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_NESTED_TYPE => "nested_type",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT8 => "int8",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT8 => "uint8",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT16 => "int16",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT16 => "uint16",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT32 => "int32",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT32 => "uint32",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT64 => "int64",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT64 => "uint64",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FLOAT => "float",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_DOUBLE => "double",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_LONG_DOUBLE => "long_double",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_CHAR => "char",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_WCHAR => "wchar",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOOLEAN => "boolean",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BYTE => "byte",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_STRING => "string",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_WSTRING => "wstring",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FIXED_STRING => "fixed_string",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FIXED_WSTRING => "fixed_wstring",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOUNDED_STRING => "bounded_string",
        c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOUNDED_WSTRING => "bounded_wstring",
        else => "unknown",
    };
}

fn discoverServicesInfo(allocator: std.mem.Allocator, node: *c.rcl_node_t) ![]JsonOutput.ServiceInfo {
    var service_names_and_types = c.rcl_get_zero_initialized_names_and_types();
    defer _ = c.rcl_names_and_types_fini(&service_names_and_types);

    var alloc = c.rcutils_get_default_allocator();
    const ret = c.rcl_get_service_names_and_types(node, &alloc, &service_names_and_types);

    if (ret != c.RCL_RET_OK) {
        return error.FailedToGetServices;
    }

    var services = try allocator.alloc(JsonOutput.ServiceInfo, service_names_and_types.names.size);

    var i: usize = 0;
    while (i < service_names_and_types.names.size) : (i += 1) {
        const service_name = service_names_and_types.names.data[i];
        const types = &service_names_and_types.types[i];
        const type_name: [*:0]const u8 = if (types.size > 0) types.data[0] else "";

        services[i] = .{
            .name = try allocator.dupe(u8, std.mem.span(service_name)),
            .type_name = try allocator.dupe(u8, std.mem.span(type_name)),
        };
    }

    return services;
}

fn freeJsonOutput(allocator: std.mem.Allocator, output: JsonOutput) void {
    for (output.nodes) |node_info| {
        allocator.free(node_info.name);
        allocator.free(node_info.namespace);
    }
    allocator.free(output.nodes);

    for (output.topics) |topic| {
        allocator.free(topic.name);
        allocator.free(topic.type_name);
        if (topic.schema) |schema| {
            for (schema.fields) |field| {
                allocator.free(field.name);
            }
            allocator.free(schema.fields);
        }
        for (topic.publishers) |publisher| {
            allocator.free(publisher.node_name);
            allocator.free(publisher.node_namespace);
        }
        allocator.free(topic.publishers);
        for (topic.subscribers) |sub| {
            allocator.free(sub.node_name);
            allocator.free(sub.node_namespace);
        }
        allocator.free(topic.subscribers);
    }
    allocator.free(output.topics);

    for (output.services) |service| {
        allocator.free(service.name);
        allocator.free(service.type_name);
    }
    allocator.free(output.services);
}

fn uploadDiscovery(allocator: std.mem.Allocator, discovery: JsonOutput, robotId: []u8) !void {
    const base64_encoder = std.base64.standard.Encoder;

    // 1. Serialize discovery data to a buffer
    var body_payload: std.Io.Writer.Allocating = .init(allocator);
    defer body_payload.deinit();
    try body_payload.writer.print("{f}", .{std.json.fmt(discovery, .{})});
    const json_bytes = body_payload.written();

    // sign it
    const sig_bytes = try config.ConfigStorage.signPayload(allocator, json_bytes);
    var sig_b64: [base64_encoder.calcSize(64)]u8 = undefined;
    _ = base64_encoder.encode(&sig_b64, &sig_bytes);

    // 2. Setup HTTP Client
    var client = http.Client{ .allocator = allocator };
    defer client.deinit();

    // 3. Setup response capture
    var response_body: std.Io.Writer.Allocating = .init(allocator);
    defer response_body.deinit();

    // 4. Execute Fetch
    const url = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ constants.discovery_base_url, robotId });
    defer allocator.free(url);
    const result = try client.fetch(.{
        .method = .POST,
        .location = .{ .url = url },
        .payload = json_bytes,
        .response_writer = &response_body.writer,
        .headers = .{
            .content_type = .{ .override = "application/json" },
            .connection = .{ .override = "close" },
        },
        .extra_headers = &.{
            .{ .name = "X-Signature", .value = &sig_b64 },
        },
    });

    // 5. Handle Response
    if (result.status != .ok) {
        std.debug.print("Discovery upload failed: {d}\n", .{result.status});
        return error.UploadFailed;
    }

    std.debug.print("Discovery data successfully pushed to Orca.\n", .{});
}
