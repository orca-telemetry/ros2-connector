const std = @import("std");
const schema = @import("schema.zig");
const cb = @import("column_buffer.zig");

const FlatField = schema.FlatField;
const ColumnBuffer = cb.ColumnBuffer;
const FLUSH_THRESHOLD = cb.FLUSH_THRESHOLD;

pub const TopicBuffer = struct {
    topic_name: []const u8,
    type_name: []const u8,
    columns: []ColumnBuffer,
    /// Incremented on every push, reset on every flush
    message_count: usize,

    pub fn init(
        allocator: std.mem.Allocator,
        topic_name: []const u8,
        type_name: []const u8,
        fields: []const FlatField,
    ) !TopicBuffer {
        const cols = try allocator.alloc(ColumnBuffer, fields.len);
        for (fields, 0..) |field, i| {
            cols[i] = ColumnBuffer.init(field);
        }
        return .{
            .topic_name = topic_name,
            .type_name = type_name,
            .columns = cols,
            .message_count = 0,
        };
    }

    /// Push one full message - values must be in the same order as columns.
    /// Each value slice is the raw bytes for that field.
    pub fn push(self: *TopicBuffer, values: []const []const u8) !void {
        std.debug.assert(values.len == self.columns.len);
        for (self.columns, values) |*col, val| {
            try col.push(val);
        }
        self.message_count += 1;
    }

    pub fn needsFlush(self: *const TopicBuffer) bool {
        return self.message_count >= FLUSH_THRESHOLD;
    }

    /// Drain all columns into a flat per-column byte slice array.
    /// Caller provides a pre-allocated [][]u8 with one entry per column,
    /// each large enough to hold FLUSH_THRESHOLD * stride bytes.
    /// Returns the number of elements drained (same for all columns).
    pub fn drain(self: *TopicBuffer, dest: [][]u8) !usize {
        std.debug.assert(dest.len == self.columns.len);
        var n: usize = 0;
        for (self.columns, dest) |*col, dest_col| {
            n = try col.drain(dest_col);
        }
        self.message_count = 0;
        return n;
    }

    pub fn len(self: *const TopicBuffer) usize {
        return self.message_count;
    }
};

/// A pool of TopicBuffers, one per subscribed topic.
/// Owns all allocations via an arena — single deinit at shutdown.
pub const TopicBufferPool = struct {
    arena: std.heap.ArenaAllocator,
    buffers: []TopicBuffer,

    pub fn init(
        backing: std.mem.Allocator,
        topics: []const TopicSpec,
    ) !TopicBufferPool {
        var arena = std.heap.ArenaAllocator.init(backing);
        const allocator = arena.allocator();

        const buffers = try allocator.alloc(TopicBuffer, topics.len);
        for (topics, 0..) |topic, i| {
            buffers[i] = try TopicBuffer.init(
                allocator,
                topic.name,
                topic.type_name,
                topic.fields,
            );
        }

        return .{ .arena = arena, .buffers = buffers };
    }

    pub fn deinit(self: *TopicBufferPool) void {
        self.arena.deinit();
    }

    /// Find a buffer by topic name. O(n) — fine for typical topic counts (<100).
    // FIXME: replace with std.StringHashMap(*TopicBuffer) for better scaling
    pub fn get(self: *TopicBufferPool, topic_name: []const u8) ?*TopicBuffer {
        for (self.buffers) |*buf| {
            if (std.mem.eql(u8, buf.topic_name, topic_name)) return buf;
        }
        return null;
    }
};

/// Fully resolved topic descriptor, produced by the startup pipeline
pub const TopicSpec = struct {
    name: []const u8,
    type_name: []const u8,
    fields: []const FlatField,
};
