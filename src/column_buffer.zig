/// Contains utilities for storing ROS column types to a buffer
const std = @import("std");
const schema = @import("schema.zig");
const RingBuffer = @import("ring.zig").RingBuffer;

const FieldType = schema.FieldType;
const FlatField = schema.FlatField;

pub const COLUMN_BUFFER_BYTES: usize = 1024 * 1024; // 1MB per column
pub const FLUSH_THRESHOLD: usize = 100; // messages

pub const ColumnBuffer = struct {
    name: []const u8,
    field_type: FieldType,
    buf: RingBuffer(COLUMN_BUFFER_BYTES),

    pub fn init(field: FlatField) ColumnBuffer {
        return .{
            .name = field.name,
            .field_type = field.field_type,
            .buf = RingBuffer(COLUMN_BUFFER_BYTES).init(field.stride()),
        };
    }

    /// Push one sample. For strings, pads to MAX_STRING_LEN.
    /// For primitives, value.len must equal field_type.stride().
    pub fn push(self: *ColumnBuffer, value: []const u8) !void {
        if (self.field_type == .string) {
            var slot: [schema.MAX_STRING_LEN]u8 = .{0} ** schema.MAX_STRING_LEN;
            const n = @min(value.len, schema.MAX_STRING_LEN);
            @memcpy(slot[0..n], value[0..n]);
            try self.buf.push(&slot);
        } else {
            try self.buf.push(value);
        }
    }

    /// Drain all samples into dest. Returns element count.
    pub fn drain(self: *ColumnBuffer, dest: []u8) !usize {
        return self.buf.drain(dest);
    }

    pub fn len(self: *const ColumnBuffer) usize {
        return self.buf.len();
    }

    pub fn isFull(self: *const ColumnBuffer) bool {
        return self.buf.is_full();
    }
};
