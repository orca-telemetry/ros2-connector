const std = @import("std");
const c = @import("c.zig").c;

pub const MAX_STRING_LEN: usize = 256;

/// A fully resolved, flattened field — no nesting, dotted name
pub const FlatField = struct {
    /// e.g. "pose.position.x"
    name: []const u8,
    field_type: FieldType,
    /// Byte offset into the deserialised C struct produced by rcl_take.
    /// For nested fields this is parent_offset + member.offset_.
    offset: usize,

    pub fn stride(self: FlatField) usize {
        return self.field_type.stride();
    }
};

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
            .long_double => 16,
            .string => MAX_STRING_LEN,
        };
    }

    /// Map from the string names returned by getFieldTypeName()
    pub fn fromRosName(name: []const u8) ?FieldType {
        const map = std.StaticStringMap(FieldType).initComptime(.{
            .{ "int8", .int8 },
            .{ "uint8", .uint8 },
            .{ "byte", .byte },
            .{ "char", .char },
            .{ "boolean", .boolean },
            .{ "int16", .int16 },
            .{ "uint16", .uint16 },
            .{ "int32", .int32 },
            .{ "uint32", .uint32 },
            .{ "float", .float32 },
            .{ "int64", .int64 },
            .{ "uint64", .uint64 },
            .{ "double", .float64 },
            .{ "long_double", .long_double },
            .{ "string", .string },
            .{ "wstring", .string },
            .{ "fixed_string", .string },
            .{ "fixed_wstring", .string },
            .{ "bounded_string", .string },
            .{ "bounded_wstring", .string },
        });
        return map.get(name);
    }

    /// Arrow format string for nanoarrow schema construction
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
            .long_double => "g", // no 128-bit float in Arrow — downcast to f64
            .string => "z", // binary, fixed width handled by stride
        };
    }
};

/// Recursively flatten a ROS message type into a []FlatField.
/// All allocations go into the provided allocator (expected to be an arena).
/// `prefix` is the dotted path so far e.g. "pose.position"
/// Load type support for `type_name_cstr`, flatten all fields into `out`,
/// and return the type support handle.
///
/// The underlying .so is intentionally NOT dlclose'd — the handle must remain
/// valid for the lifetime of the process. The OS reclaims it on exit.
pub fn flattenMessageType(
    allocator: std.mem.Allocator,
    type_name_cstr: [*c]const u8,
    prefix: []const u8,
    out: *std.ArrayList(FlatField),
) !*const c.rosidl_message_type_support_t {
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

    try flattenNestedMembers(allocator, intro_ts, prefix, 0, out);

    return type_support;
}

/// Recurse into an already-loaded nested MessageMembers (no dlopen needed).
/// `parent_offset` is the byte offset of the containing struct within the
/// top-level message struct — accumulated as we recurse deeper.
fn flattenNestedMembers(
    allocator: std.mem.Allocator,
    members: *const c.rosidl_typesupport_introspection_c__MessageMembers,
    prefix: []const u8,
    parent_offset: usize,
    out: *std.ArrayList(FlatField),
) !void {
    var i: u32 = 0;
    while (i < members.member_count_) : (i += 1) {
        const member = &members.members_[i];
        const member_name = std.mem.span(member.name_);
        const abs_offset = parent_offset + member.offset_;

        const dotted_name = if (prefix.len > 0)
            try std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, member_name })
        else
            try allocator.dupe(u8, member_name);

        if (member.type_id_ == c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_NESTED_TYPE) {
            if (member.members_ != null) {
                // Cast from the generic type support pointer to the introspection
                // members struct — same pattern as casting type_support.data above
                const nested: *const c.rosidl_typesupport_introspection_c__MessageMembers =
                    @ptrCast(@alignCast(member.members_));
                try flattenNestedMembers(allocator, nested, dotted_name, abs_offset, out);
            } else {
                std.debug.print("Warning: nested type {s} has no members, skipping\n", .{dotted_name});
            }
        } else {
            const ros_type_name = rosTypeIdToName(member.type_id_);
            const field_type = FieldType.fromRosName(ros_type_name) orelse {
                std.debug.print("Warning: unknown type '{s}' for field {s}, skipping\n", .{ ros_type_name, dotted_name });
                continue;
            };
            try out.append(allocator, .{
                .name = dotted_name,
                .field_type = field_type,
                .offset = abs_offset,
            });
        }
    }
}

fn rosTypeIdToName(type_id: u8) []const u8 {
    return switch (type_id) {
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
        else => "unknown",
    };
}
