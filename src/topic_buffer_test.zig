const std = @import("std");
const tb = @import("topic_buffer.zig");

const MessageBuffer = tb.MessageBuffer;
const MessageBufferPool = tb.MessageBufferPool;

test "MessageBuffer: push and len" {
    var buf = MessageBuffer.init(
        std.testing.allocator,
        "/test/topic",
        "std_msgs/msg/String",
        1024,
    );
    defer buf.deinit();

    try std.testing.expectEqual(@as(usize, 0), buf.len());

    try buf.push("hello", 100);
    try std.testing.expectEqual(@as(usize, 1), buf.len());
    try std.testing.expectEqual(@as(usize, 5), buf.total_bytes);

    try buf.push("world!", 200);
    try std.testing.expectEqual(@as(usize, 2), buf.len());
    try std.testing.expectEqual(@as(usize, 11), buf.total_bytes);
}

test "MessageBuffer: drainAll returns all messages and resets" {
    var buf = MessageBuffer.init(
        std.testing.allocator,
        "/test/topic",
        "std_msgs/msg/String",
        1024,
    );
    defer buf.deinit();

    try buf.push("msg1", 100);
    try buf.push("msg2", 200);
    try buf.push("msg3", 300);

    const messages = buf.drainAll();
    defer std.testing.allocator.free(messages);

    try std.testing.expectEqual(@as(usize, 3), messages.len);
    try std.testing.expectEqualStrings("msg1", messages[0].data);
    try std.testing.expectEqualStrings("msg2", messages[1].data);
    try std.testing.expectEqualStrings("msg3", messages[2].data);

    // Free individual messages (caller responsibility after drain)
    for (messages) |msg| {
        std.testing.allocator.free(msg.data);
    }

    // Buffer should be empty after drain
    try std.testing.expectEqual(@as(usize, 0), buf.len());
    try std.testing.expectEqual(@as(usize, 0), buf.total_bytes);
}

test "MessageBuffer: drainAll on empty buffer" {
    var buf = MessageBuffer.init(
        std.testing.allocator,
        "/test/topic",
        "std_msgs/msg/String",
        1024,
    );
    defer buf.deinit();

    const messages = buf.drainAll();
    defer std.testing.allocator.free(messages);
    try std.testing.expectEqual(@as(usize, 0), messages.len);
}

test "MessageBuffer: overflow drops oldest messages" {
    // max_bytes = 20, so after filling past 20 bytes, oldest get dropped
    var buf = MessageBuffer.init(
        std.testing.allocator,
        "/test/topic",
        "std_msgs/msg/String",
        20,
    );
    defer buf.deinit();

    // Push 10 bytes each — after second push we're at 20 (at limit, no drop)
    try buf.push("aaaaaaaaaa", 100); // 10 bytes
    try std.testing.expectEqual(@as(u64, 0), buf.drop_count);
    try buf.push("bbbbbbbbbb", 200); // 10 bytes, total 20
    try std.testing.expectEqual(@as(u64, 0), buf.drop_count);

    // Third push puts us at 30 > 20, should drop oldest
    try buf.push("cccccccccc", 300); // 10 bytes, total would be 30 → drop "aaa..."
    try std.testing.expectEqual(@as(u64, 1), buf.drop_count);
    try std.testing.expectEqual(@as(usize, 2), buf.len());
    try std.testing.expectEqual(@as(usize, 20), buf.total_bytes);

    // Drain and verify we have "bbb..." and "ccc..."
    const messages = buf.drainAll();
    defer std.testing.allocator.free(messages);

    try std.testing.expectEqual(@as(usize, 2), messages.len);
    try std.testing.expectEqualStrings("bbbbbbbbbb", messages[0].data);
    try std.testing.expectEqualStrings("cccccccccc", messages[1].data);

    for (messages) |msg| {
        std.testing.allocator.free(msg.data);
    }
}

test "MessageBuffer: needsFlush" {
    var buf = MessageBuffer.init(
        std.testing.allocator,
        "/test/topic",
        "std_msgs/msg/String",
        1024,
    );
    defer buf.deinit();

    try std.testing.expect(!buf.needsFlush());
    try buf.push("data", 0);
    try std.testing.expect(buf.needsFlush());
}

test "MessageBuffer: push copies data" {
    var buf = MessageBuffer.init(
        std.testing.allocator,
        "/test/topic",
        "std_msgs/msg/String",
        1024,
    );
    defer buf.deinit();

    var data = [_]u8{ 1, 2, 3, 4 };
    try buf.push(&data, 0);

    // Modify the original — buffer should have a copy
    data[0] = 99;

    const messages = buf.drainAll();
    defer std.testing.allocator.free(messages);
    defer for (messages) |msg| std.testing.allocator.free(msg.data);

    try std.testing.expectEqual(@as(u8, 1), messages[0].data[0]);
}

test "MessageBuffer: timestamps are preserved through push/drain" {
    var buf = MessageBuffer.init(
        std.testing.allocator,
        "/test/topic",
        "std_msgs/msg/String",
        1024,
    );
    defer buf.deinit();

    try buf.push("first", 1000000000);
    try buf.push("second", 2000000000);
    try buf.push("third", 3000000000);

    const messages = buf.drainAll();
    defer std.testing.allocator.free(messages);
    defer for (messages) |msg| std.testing.allocator.free(msg.data);

    try std.testing.expectEqual(@as(u64, 1000000000), messages[0].timestamp_ns);
    try std.testing.expectEqual(@as(u64, 2000000000), messages[1].timestamp_ns);
    try std.testing.expectEqual(@as(u64, 3000000000), messages[2].timestamp_ns);
}

test "MessageBufferPool: get by topic name" {
    const specs = [_]tb.TopicSpec{
        .{ .name = "/topic_a", .type_name = "std_msgs/msg/String" },
        .{ .name = "/topic_b", .type_name = "sensor_msgs/msg/LaserScan" },
    };

    var pool = try MessageBufferPool.init(std.testing.allocator, &specs, 8 * 1024 * 1024);
    defer pool.deinit();

    const a = pool.get("/topic_a");
    try std.testing.expect(a != null);
    try std.testing.expectEqualStrings("/topic_a", a.?.topic_name);

    const b = pool.get("/topic_b");
    try std.testing.expect(b != null);
    try std.testing.expectEqualStrings("sensor_msgs/msg/LaserScan", b.?.type_name);

    const missing = pool.get("/nonexistent");
    try std.testing.expect(missing == null);
}
