const std = @import("std");
const c = @import("c.zig").c;
const testing = std.testing;
const FieldType = @import("schema.zig").FieldType;
const FlatField = @import("schema.zig").FlatField;
const SchemaError = @import("schema.zig").SchemaError;
const flattenMessageType = @import("schema.zig").flattenMessageType;
const MAX_STRING_LEN = @import("schema.zig").MAX_STRING_LEN;

// ------------------------------------------------------------
// FieldType.stride — every variant must return a positive,
// power-of-two-or-capped value.  Catches regressions if a new
// variant is added without a stride arm.
// ------------------------------------------------------------
test "FieldType.stride: all variants return nonzero" {
    inline for (std.meta.fields(FieldType)) |f| {
        const ft: FieldType = @enumFromInt(f.value);
        try testing.expect(ft.stride() > 0);
    }
}

test "FieldType.stride: string capped at MAX_STRING_LEN" {
    try testing.expectEqual(MAX_STRING_LEN, FieldType.string.stride());
}

test "FieldType.stride: spot-check numeric widths" {
    try testing.expectEqual(@as(usize, 1), FieldType.boolean.stride());
    try testing.expectEqual(@as(usize, 2), FieldType.int16.stride());
    try testing.expectEqual(@as(usize, 4), FieldType.float32.stride());
    try testing.expectEqual(@as(usize, 8), FieldType.float64.stride());
    try testing.expectEqual(@as(usize, 16), FieldType.long_double.stride());
}

// ------------------------------------------------------------
// FieldType.fromRosName — every known name must round-trip;
// unknown names must return null, never panic.
// ------------------------------------------------------------
test "FieldType.fromRosName: known names resolve" {
    const cases = .{
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT8, FieldType.int8 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT8, FieldType.uint8 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT16, FieldType.int16 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT16, FieldType.uint16 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT32, FieldType.int32 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT32, FieldType.uint32 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_INT64, FieldType.int64 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_UINT64, FieldType.uint64 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FLOAT, FieldType.float32 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_DOUBLE, FieldType.float64 },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_LONG_DOUBLE, FieldType.long_double },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_CHAR, FieldType.char },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_WCHAR, FieldType.char },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOOLEAN, FieldType.boolean },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BYTE, FieldType.byte },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_STRING, FieldType.string },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_WSTRING, FieldType.string },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FIXED_STRING, FieldType.string },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_FIXED_WSTRING, FieldType.string },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOUNDED_STRING, FieldType.string },
        .{ c.ROSIDL_DYNAMIC_TYPESUPPORT_FIELD_TYPE_BOUNDED_WSTRING, FieldType.string },
    };
    inline for (cases) |tc| {
        const result = try FieldType.fromRosTypeId(tc[0]);
        try testing.expectEqual(tc[1], result);
    }
}

// ------------------------------------------------------------
// FieldType.arrowFormat — all variants must return a non-empty
// sentinel-terminated string.
// ------------------------------------------------------------
test "FieldType.arrowFormat: all variants return nonempty format string" {
    inline for (std.meta.fields(FieldType)) |f| {
        const ft: FieldType = @enumFromInt(f.value);
        const fmt = ft.arrowFormat();
        try testing.expect(fmt[0] != 0); // at least one character
    }
}

// ------------------------------------------------------------
// flattenMessageType — type name parsing.
// These tests exercise only the parsing / error path, which does
// NOT require a live ROS environment.  The dlopen call will fail
// and return TypeSupportNotFound, which is still the right error
// to observe here (it means we got past the parse stage).
//
// We distinguish:
//   InvalidTypeName      → bad structure before we even try dlopen
//   TypeSupportNotFound  → structure was valid, dlopen failed (expected
//                          in a unit-test environment with no ROS libs)
// ------------------------------------------------------------
fn arenaAllocator() std.heap.ArenaAllocator {
    return std.heap.ArenaAllocator.init(std.heap.page_allocator);
}

test "flattenMessageType: missing message segment → InvalidTypeName" {
    var arena = arenaAllocator();
    defer arena.deinit();
    var out: std.ArrayList(FlatField) = .empty;
    defer out.deinit(arena.allocator());

    // Only one slash segment — message_name will be null
    const result = flattenMessageType(arena.allocator(), "std_msgs/msg", "", &out);
    try testing.expectError(error.InvalidTypeName, result);
}

test "flattenMessageType: empty string → InvalidTypeName" {
    var arena = arenaAllocator();
    defer arena.deinit();
    var out: std.ArrayList(FlatField) = .empty;
    defer out.deinit(arena.allocator());

    const result = flattenMessageType(arena.allocator(), "", "", &out);
    try testing.expectError(error.InvalidTypeName, result);
}

test "flattenMessageType: valid type name but no ROS libs → TypeSupportNotFound" {
    var arena = arenaAllocator();
    defer arena.deinit();
    var out: std.ArrayList(FlatField) = .empty;
    defer out.deinit(arena.allocator());

    // well-formed name; dlopen will fail in a no-ROS environment.
    const result = flattenMessageType(arena.allocator(), "std_msgs/msg/String", "", &out);
    try testing.expectError(error.TypeSupportNotFound, result);
}

test "flattenMessageType: no-ROS-libs error does not leak into out" {
    var arena = arenaAllocator();
    defer arena.deinit();
    var out: std.ArrayList(FlatField) = .empty;
    defer out.deinit(arena.allocator());

    _ = flattenMessageType(arena.allocator(), "std_msgs/msg/String", "", &out) catch {};
    try testing.expectEqual(@as(usize, 0), out.items.len);
}

// // ------------------------------------------------------------
// // Fuzz targets
// // ------------------------------------------------------------
//
// // Target 1: fromRosName must never panic on arbitrary input.
// test "fuzz: FieldType.fromRosName never panics" {
//     const input = std.testing.fuzzInput(.{});
//     _ = FieldType.fromRosName(input);
//     // reaching here without a panic is the assertion
// }
//
// // Target 2: type name parser must always return a clean error,
// // never undefined behaviour, on arbitrary C-string-shaped input.
// test "fuzz: flattenMessageType type name parsing never panics" {
//     const raw = std.testing.fuzzInput(.{});
//
//     // Ensure null-termination for the C-string cast
//     var arena = arenaAllocator();
//     defer arena.deinit();
//     const alloc = arena.allocator();
//
//     // Copy the fuzz bytes into a null-terminated buffer
//     const cstr = alloc.dupeZ(u8, raw) catch return;
//     var out: std.ArrayList(FlatField) = .empty;
//     defer out.deinit(alloc);
//
//     // Any result is acceptable — we only care that we don't crash or
//     // invoke undefined behaviour.  TypeSupportNotFound and InvalidTypeName
//     // are both expected outcomes; MaxFlattenDepthExceeded is not reachable
//     // through this path alone.
//     _ = flattenMessageType(alloc, cstr.ptr, "", &out) catch {};
// }
