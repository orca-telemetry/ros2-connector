const std = @import("std");
const c = @import("c.zig").c;

pub const MAX_STRING_LEN: usize = 256;
pub const MAX_FLATTEN_DEPTH: u32 = 32;

pub const SchemaError = error{
    TypeSupportNotFound,
    MaxFlattenDepthExceeded,
    InvalidTypeName,
    TypeSupportFunctionNotFound,
    TypeSupportNull,
    OutOfMemory,
    UnknownROSField,
};

// A work item pushed onto the explicit stack instead of using recursion.
const FlattenTask = struct {
    members: *const c.rosidl_typesupport_introspection_c__MessageMembers,
    prefix: []const u8,
    depth: u32,
};

/// A fully resolved, flattened ROS field - no nesting, dotted name
pub const FlatField = struct {
    /// e.g. "pose.position.x"
    name: []const u8,
    field_type: FieldType,
    type_id: u8,
    is_array: bool,
    array_size: usize,
    is_upper_bound: bool,
    /// byte offset into the deserialised C struct produced by rcl_take.
    /// for nested fields this is parent_offset + member.offset_.
    offset: usize,

    pub fn stride(self: FlatField) usize {
        return self.field_type.stride();
    }
};

/// list of field types supported by this collector,
/// and the mapping to their stride
pub const FieldType = enum {
    int8,
    uint8,
    byte,
    char,
    boolean,
    int16,
    uint16,
    int32,
    uint32,
    float32,
    int64,
    uint64,
    float64,
    long_double,
    string,

    pub fn stride(self: FieldType) usize {
        return switch (self) {
            .int8, .uint8, .byte, .char, .boolean => 1,
            .int16, .uint16 => 2,
            .int32, .uint32, .float32 => 4,
            .int64, .uint64, .float64 => 8,
            .long_double => 16, // long_double is 128bits in ROS, as opposed to zigs 80
            .string => MAX_STRING_LEN, // has to be capped. we can't do variable length string messages
        };
    }

    pub fn fromRosTypeId(ros_type_id: u8) error{UnknownROSField}!FieldType {
        return switch (ros_type_id) {
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT8 => .int8,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT8 => .uint8,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT16 => .int16,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT16 => .uint16,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT32 => .int32,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT32 => .uint32,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT64 => .int64,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT64 => .uint64,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FLOAT => .float32,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_DOUBLE => .float64,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_LONG_DOUBLE => .long_double,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_CHAR => .char,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_WCHAR => .char,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOOLEAN => .boolean,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BYTE => .byte,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_STRING => .string,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_WSTRING => .string,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FIXED_STRING => .string,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FIXED_WSTRING => .string,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOUNDED_STRING => .string,
            c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOUNDED_WSTRING => .string,
            else => error.UnknownROSField,
        };
    }

    /// arrow format string for nanoarrow schema construction
    pub fn arrowFormat(self: FieldType) [*:0]const u8 {
        return switch (self) {
            .int8 => "c",
            .uint8 => "C",
            .byte => "C",
            .char => "c",
            .boolean => "b",
            .int16 => "s",
            .uint16 => "S",
            .int32 => "i",
            .uint32 => "I",
            .float32 => "f",
            .int64 => "l",
            .uint64 => "L",
            .float64 => "g",
            .long_double => "g", // no 128-bit float in Arrow - downcast to f64
            .string => "z", // binary, fixed width handled by stride
        };
    }
};

/// Flatten a ROS message type into a []FlatField.
/// All allocations go into the provided allocator (expected to be an arena).
/// `prefix` is the dotted path to prefix to the final path.
/// Load type support for `type_name_cstr`, flatten all fields into `out`,
/// and return the type support handle.
///
/// The underlying .so is intentionally NOT dlclose'd - the handle must remain
/// valid for the lifetime of the process. The OS reclaims it on exit.
pub fn flattenMessageType(
    allocator: std.mem.Allocator,
    type_name_cstr: [*c]const u8,
    prefix: []const u8,
    out: *std.ArrayList(FlatField),
) SchemaError!*const c.rosidl_message_type_support_t {
    const type_name = std.mem.span(type_name_cstr);

    var parts = std.mem.splitSequence(u8, type_name, "/");
    const package_name = parts.next() orelse return error.InvalidTypeName;
    _ = parts.next(); // skip "msg"
    const message_name = parts.next() orelse return error.InvalidTypeName;

    const lib_name = try std.fmt.allocPrint(
        allocator,
        "lib{s}__rosidl_typesupport_introspection_c.so",
        .{package_name},
    );
    defer allocator.free(lib_name);

    // RTLD_NODELETE keeps the .so mapped even if dlclose is called elsewhere
    const handle = c.dlopen(lib_name.ptr, c.RTLD_LAZY | c.RTLD_NODELETE);
    if (handle == null) {
        std.debug.print("Warning: Could not load type support for {s}\n", .{type_name});
        return error.TypeSupportNotFound;
    }
    // No defer dlclose — intentionally kept loaded for process lifetime

    const func_name = try std.fmt.allocPrint(
        allocator,
        "rosidl_typesupport_introspection_c__get_message_type_support_handle__{s}__msg__{s}",
        .{ package_name, message_name },
    );
    defer allocator.free(func_name);

    const get_ts_func = c.dlsym(handle, func_name.ptr) orelse return error.TypeSupportFunctionNotFound;
    const GetTypeSupportFunc = *const fn () callconv(.c) ?*const c.rosidl_message_type_support_t;
    const get_ts: GetTypeSupportFunc = @ptrCast(@alignCast(get_ts_func));
    const type_support = get_ts() orelse return error.TypeSupportNull;
    const intro_ts: *const c.rosidl_typesupport_introspection_c__MessageMembers =
        @ptrCast(@alignCast(type_support.data));

    // --- iterative flattening ---
    var stack: std.ArrayList(FlattenTask) = .empty;
    defer stack.deinit(allocator);

    try stack.append(allocator, .{ .members = intro_ts, .prefix = prefix, .depth = 0 });

    while (stack.items.len > 0) {
        const task = stack.pop() orelse break;

        if (task.depth >= MAX_FLATTEN_DEPTH) {
            std.debug.print(
                "Error: Max flatten depth ({d}) exceeded at prefix '{s}'\n",
                .{ MAX_FLATTEN_DEPTH, task.prefix },
            );
            return error.MaxFlattenDepthExceeded;
        }

        const members = task.members;
        for (0..members.member_count_) |i| {
            const member = &members.members_[i];
            const field_name = std.mem.span(member.name_);

            // build the dotted path for this field
            const full_name = if (task.prefix.len > 0)
                try std.fmt.allocPrint(allocator, "{s}.{s}", .{ task.prefix, field_name })
            else
                try allocator.dupe(u8, field_name);

            if (member.type_id_ == c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_NESTED_TYPE) {
                // nested message - push onto stack
                const nested_ts: *const c.rosidl_typesupport_introspection_c__MessageMembers =
                    @ptrCast(@alignCast(member.members_.*.data));
                try stack.append(allocator, .{
                    .members = nested_ts,
                    .prefix = full_name,
                    .depth = task.depth + 1,
                });
            } else {
                // primitive field - emit directly
                // cast ROS field type to field type
                const field_type_struct = try FieldType.fromRosTypeId(member.type_id_);
                try out.append(allocator, .{
                    .name = full_name,
                    .type_id = member.type_id_,
                    .offset = member.offset_,
                    .field_type = field_type_struct,
                    .is_array = member.is_array_,
                    .array_size = member.array_size_,
                    .is_upper_bound = member.is_upper_bound_,
                });
            }
        }
    }
    // --- end iterative flattening ---

    return type_support;
}
