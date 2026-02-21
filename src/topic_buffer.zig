const std = @import("std");

const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024; // 8 MB per topic

pub const TimestampedMessage = struct {
    data: []u8,
    timestamp_ns: u64,
};

pub const MessageBuffer = struct {
    topic_name: []const u8,
    type_name: []const u8,
    topic_schema: []const u8,
    messages: std.ArrayList(TimestampedMessage),
    total_bytes: usize,
    max_bytes: usize,
    drop_count: u64,
    messages_received: u64,
    bytes_received: u64,

    pub fn init(
        topic_name: []const u8,
        type_name: []const u8,
        topic_schema: []const u8,
        max_bytes: usize,
    ) MessageBuffer {
        return .{
            .topic_name = topic_name,
            .type_name = type_name,
            .topic_schema = topic_schema,
            .messages = .empty,
            .total_bytes = 0,
            .max_bytes = max_bytes,
            .drop_count = 0,
            .messages_received = 0,
            .bytes_received = 0,
        };
    }

    /// Push a copy of the serialized message bytes into the buffer.
    /// If the buffer exceeds max_bytes, drops the oldest message.
    pub fn push(self: *MessageBuffer, allocator: std.mem.Allocator, serialized_bytes: []const u8, timestamp_ns: u64) !void {
        self.messages_received += 1;
        self.bytes_received += serialized_bytes.len;

        const copy = try allocator.dupe(u8, serialized_bytes);
        try self.messages.append(allocator, .{ .data = copy, .timestamp_ns = timestamp_ns });
        self.total_bytes += copy.len;

        // Drop oldest messages while over budget
        while (self.total_bytes > self.max_bytes and self.messages.items.len > 1) {
            const oldest = self.messages.orderedRemove(0);
            self.total_bytes -= oldest.data.len;
            allocator.free(oldest.data);
            self.drop_count += 1;
        }
    }

    /// Return all buffered messages and reset the buffer.
    /// After drainAll, the caller owns the returned slice and all message slices.
    pub fn drainAll(self: *MessageBuffer, allocator: std.mem.Allocator) []TimestampedMessage {
        const items = self.messages.toOwnedSlice(allocator) catch {
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

    pub fn deinit(self: *MessageBuffer, allocator: std.mem.Allocator) void {
        for (self.messages.items) |msg| {
            allocator.free(msg.data);
        }
        self.messages.deinit(allocator);
    }
};

/// Descriptor for a topic to subscribe to.
pub const TopicSpec = struct {
    name: []const u8,
    type_name: []const u8,
    topic_schema: []const u8,
};

/// A pool of MessageBuffers, one per subscribed topic.
/// Unmanaged — caller provides the allocator to each method.
pub const MessageBufferPool = struct {
    buffers: []MessageBuffer,

    pub fn init(
        allocator: std.mem.Allocator,
        topics: []const TopicSpec,
        max_bytes_per_topic: usize,
    ) !MessageBufferPool {
        const buffers = try allocator.alloc(MessageBuffer, topics.len);
        for (topics, 0..) |topic, i| {
            buffers[i] = MessageBuffer.init(
                topic.name,
                topic.type_name,
                topic.topic_schema,
                max_bytes_per_topic,
            );
        }

        return .{ .buffers = buffers };
    }

    pub fn deinit(self: *MessageBufferPool, allocator: std.mem.Allocator) void {
        for (self.buffers) |*buf| {
            buf.deinit(allocator);
        }
        allocator.free(self.buffers);
    }

    /// Find a buffer by topic name. O(n) — fine for typical topic counts (<100).
    pub fn get(self: *MessageBufferPool, topic_name: []const u8) ?*MessageBuffer {
        for (self.buffers) |*buf| {
            if (std.mem.eql(u8, buf.topic_name, topic_name)) return buf;
        }
        return null;
    }
};
