const std = @import("std");

const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024; // 8 MB per topic

pub const TimestampedMessage = struct {
    data: []u8,
    timestamp_ns: u64,
};

pub const MessageBuffer = struct {
    topic_name: []const u8,
    type_name: []const u8,
    messages: std.ArrayList(TimestampedMessage),
    total_bytes: usize,
    max_bytes: usize,
    drop_count: u64,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        topic_name: []const u8,
        type_name: []const u8,
        max_bytes: usize,
    ) MessageBuffer {
        return .{
            .topic_name = topic_name,
            .type_name = type_name,
            .messages = .empty,
            .total_bytes = 0,
            .max_bytes = max_bytes,
            .drop_count = 0,
            .allocator = allocator,
        };
    }

    /// Push a copy of the serialized message bytes into the buffer.
    /// If the buffer exceeds max_bytes, drops the oldest message.
    pub fn push(self: *MessageBuffer, serialized_bytes: []const u8, timestamp_ns: u64) !void {
        const copy = try self.allocator.dupe(u8, serialized_bytes);
        try self.messages.append(self.allocator, .{ .data = copy, .timestamp_ns = timestamp_ns });
        self.total_bytes += copy.len;

        // Drop oldest messages while over budget
        while (self.total_bytes > self.max_bytes and self.messages.items.len > 1) {
            const oldest = self.messages.orderedRemove(0);
            self.total_bytes -= oldest.data.len;
            self.allocator.free(oldest.data);
            self.drop_count += 1;
        }
    }

    /// Return all buffered messages and reset the buffer.
    /// After drainAll, the caller owns the returned slice and all message slices.
    pub fn drainAll(self: *MessageBuffer) []TimestampedMessage {
        const items = self.messages.toOwnedSlice(self.allocator) catch {
            // On OOM for the owned slice, just return empty — messages stay buffered
            return &.{};
        };
        self.total_bytes = 0;
        return items;
    }

    pub fn len(self: *const MessageBuffer) usize {
        return self.messages.items.len;
    }

    pub fn needsFlush(self: *const MessageBuffer) bool {
        return self.messages.items.len > 0;
    }

    pub fn deinit(self: *MessageBuffer) void {
        for (self.messages.items) |msg| {
            self.allocator.free(msg.data);
        }
        self.messages.deinit(self.allocator);
    }
};

/// Descriptor for a topic to subscribe to.
pub const TopicSpec = struct {
    name: []const u8,
    type_name: []const u8,
};

/// A pool of MessageBuffers, one per subscribed topic.
pub const MessageBufferPool = struct {
    arena: std.heap.ArenaAllocator,
    buffers: []MessageBuffer,

    pub fn init(
        backing: std.mem.Allocator,
        topics: []const TopicSpec,
        max_bytes_per_topic: usize,
    ) !MessageBufferPool {
        var arena = std.heap.ArenaAllocator.init(backing);
        const allocator = arena.allocator();

        const buffers = try allocator.alloc(MessageBuffer, topics.len);
        for (topics, 0..) |topic, i| {
            buffers[i] = MessageBuffer.init(
                allocator,
                topic.name,
                topic.type_name,
                max_bytes_per_topic,
            );
        }

        return .{ .arena = arena, .buffers = buffers };
    }

    pub fn deinit(self: *MessageBufferPool) void {
        for (self.buffers) |*buf| {
            buf.deinit();
        }
        self.arena.deinit();
    }

    /// Find a buffer by topic name. O(n) — fine for typical topic counts (<100).
    pub fn get(self: *MessageBufferPool, topic_name: []const u8) ?*MessageBuffer {
        for (self.buffers) |*buf| {
            if (std.mem.eql(u8, buf.topic_name, topic_name)) return buf;
        }
        return null;
    }
};
