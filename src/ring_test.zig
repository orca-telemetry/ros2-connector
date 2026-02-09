const std = @import("std");
const rb = @import("ring.zig");
const testing = std.testing;

test "initialisation" {
    const ringBuffer8 = rb.ringBuffer(8);
    var ring: ringBuffer8 = .{};
    try testing.expect(ring.is_empty());
    try testing.expect(!ring.is_full());
    try testing.expectEqual(@as(usize, 0), ring.count);
    try testing.expectEqual(@as(usize, 0), ring.start);
    try testing.expectEqual(@as(usize, 0), ring.end);
}

test "push single element" {
    const ringBuffer8 = rb.ringBuffer(8);
    var ring: ringBuffer8 = .{};

    try ring.push(42.0);

    try testing.expect(!ring.is_empty());
    try testing.expect(!ring.is_full());
    try testing.expectEqual(@as(usize, 1), ring.count);
    try testing.expectEqual(@as(f64, 42.0), ring.buffer[0]);
}

test "push and pop single element" {
    const ringBuffer8 = rb.ringBuffer(8);
    var ring: ringBuffer8 = .{};

    try ring.push(3.14);
    const value = ring.pop();

    try testing.expect(value != null);
    try testing.expectEqual(@as(f64, 3.14), value.?);
    try testing.expect(ring.is_empty());
}

test "push multiple elements" {
    const ringBuffer8 = rb.ringBuffer(8);
    var ring: ringBuffer8 = .{};

    try ring.push(1.0);
    try ring.push(2.0);
    try ring.push(3.0);

    try testing.expectEqual(@as(usize, 3), ring.count);
    try testing.expect(!ring.is_full());
}

test "pop from empty buffer returns null" {
    const ringBuffer8 = rb.ringBuffer(8);
    var ring: ringBuffer8 = .{};

    const value = ring.pop();

    try testing.expect(value == null);
}

test "fill to capacity" {
    const ringBuffer4 = rb.ringBuffer(4);
    var ring: ringBuffer4 = .{};

    try ring.push(1.0);
    try ring.push(2.0);
    try ring.push(3.0);
    try ring.push(4.0);

    try testing.expect(ring.is_full());
    try testing.expectEqual(@as(usize, 4), ring.count);
}

test "push to full buffer returns error" {
    const ringBuffer4 = rb.ringBuffer(4);
    var ring: ringBuffer4 = .{};

    try ring.push(1.0);
    try ring.push(2.0);
    try ring.push(3.0);
    try ring.push(4.0);

    const result = ring.push(5.0);

    try testing.expectError(error.BufferFull, result);
}

test "FIFO ordering" {
    const ringBuffer8 = rb.ringBuffer(8);
    var ring: ringBuffer8 = .{};

    try ring.push(10.0);
    try ring.push(20.0);
    try ring.push(30.0);

    try testing.expectEqual(@as(f64, 10.0), ring.pop().?);
    try testing.expectEqual(@as(f64, 20.0), ring.pop().?);
    try testing.expectEqual(@as(f64, 30.0), ring.pop().?);
}

test "wrap around behavior" {
    const ringBuffer4 = rb.ringBuffer(4);
    var ring: ringBuffer4 = .{};

    // Fill buffer
    try ring.push(1.0);
    try ring.push(2.0);
    try ring.push(3.0);
    try ring.push(4.0);

    // Remove two elements
    _ = ring.pop();
    _ = ring.pop();

    // Add two more (should wrap around)
    try ring.push(5.0);
    try ring.push(6.0);

    try testing.expect(ring.is_full());

    // Verify order
    try testing.expectEqual(@as(f64, 3.0), ring.pop().?);
    try testing.expectEqual(@as(f64, 4.0), ring.pop().?);
    try testing.expectEqual(@as(f64, 5.0), ring.pop().?);
    try testing.expectEqual(@as(f64, 6.0), ring.pop().?);
}

test "alternating push and pop" {
    const ringBuffer8 = rb.ringBuffer(8);
    var ring: ringBuffer8 = .{};

    try ring.push(1.0);
    try testing.expectEqual(@as(f64, 1.0), ring.pop().?);

    try ring.push(2.0);
    try testing.expectEqual(@as(f64, 2.0), ring.pop().?);

    try ring.push(3.0);
    try testing.expectEqual(@as(f64, 3.0), ring.pop().?);

    try testing.expect(ring.is_empty());
}

test "full cycle multiple times" {
    const ringBuffer3 = rb.ringBuffer(3);
    var ring: ringBuffer3 = .{};

    // first cycle
    try ring.push(1.0);
    try ring.push(2.0);
    try ring.push(3.0);
    _ = ring.pop();
    _ = ring.pop();
    _ = ring.pop();

    // second cycle
    try ring.push(4.0);
    try ring.push(5.0);
    try ring.push(6.0);

    try testing.expectEqual(@as(f64, 4.0), ring.pop().?);
    try testing.expectEqual(@as(f64, 5.0), ring.pop().?);
    try testing.expectEqual(@as(f64, 6.0), ring.pop().?);
}

test "capacity of 1" {
    const ringBuffer1 = rb.ringBuffer(1);
    var ring: ringBuffer1 = .{};

    try ring.push(99.0);
    try testing.expect(ring.is_full());

    try testing.expectError(error.BufferFull, ring.push(100.0));

    try testing.expectEqual(@as(f64, 99.0), ring.pop().?);
    try testing.expect(ring.is_empty());
}

test "different buffer sizes compile" {
    const ringBuffer16 = rb.ringBuffer(16);
    const ringBuffer32 = rb.ringBuffer(32);
    const ringBuffer64 = rb.ringBuffer(64);

    var r16: ringBuffer16 = .{};
    var r32: ringBuffer32 = .{};
    var r64: ringBuffer64 = .{};

    try r16.push(1.0);
    try r32.push(2.0);
    try r64.push(3.0);

    try testing.expectEqual(@as(f64, 1.0), r16.pop().?);
    try testing.expectEqual(@as(f64, 2.0), r32.pop().?);
    try testing.expectEqual(@as(f64, 3.0), r64.pop().?);
}

test "state consistency after operations" {
    const ringBuffer5 = rb.ringBuffer(5);
    var ring: ringBuffer5 = .{};

    try ring.push(1.0);
    try ring.push(2.0);
    try testing.expectEqual(@as(usize, 2), ring.count);
    try testing.expectEqual(@as(usize, 0), ring.start);
    try testing.expectEqual(@as(usize, 2), ring.end);

    _ = ring.pop();
    try testing.expectEqual(@as(usize, 1), ring.count);
    try testing.expectEqual(@as(usize, 1), ring.start);
    try testing.expectEqual(@as(usize, 2), ring.end);
}

test "comptime check" {
    const RingBuffer8 = rb.ringBuffer(8);

    comptime {
        var ring: RingBuffer8 = .{};
        ring.push(1.0);
        _ = ring.pop();
    }
}
