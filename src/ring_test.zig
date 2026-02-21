const RingBuffer = @import("ring.zig").RingBuffer;
const RingBufferError = @import("ring.zig").RingBufferError;
const std = @import("std");
const testing = std.testing;

// arbitrary values for testing
const val1: [8]u8 = @bitCast(@as(f64, 1.0));
const val2: [8]u8 = @bitCast(@as(f64, 2.0));
const val3: [8]u8 = @bitCast(@as(f64, 3.0));
const val4: [8]u8 = @bitCast(@as(f64, 4.0));
const val5: [8]u8 = @bitCast(@as(f64, 5.0));
const val6: [8]u8 = @bitCast(@as(f64, 6.0));
var out1: [8]u8 = undefined;
var out2: [8]u8 = undefined;
var out3: [8]u8 = undefined;
var out4: [8]u8 = undefined;

test "initialisation" {
    const ringBuffer8 = RingBuffer(8);
    var ring = ringBuffer8.init(8);
    try testing.expect(ring.is_empty());
    try testing.expect(!ring.is_full());
    try testing.expectEqual(@as(usize, 0), ring.count);
    try testing.expectEqual(@as(usize, 0), ring.start);
    try testing.expectEqual(@as(usize, 0), ring.end);
}

test "single element ring buffer" {
    const ringBuffer8 = RingBuffer(8); //  contains 8 bytes

    var ring = ringBuffer8.init(8);

    const value: [8]u8 = @bitCast(@as(f64, 42.0)); //  single element  that takes up the full buffer

    try ring.push(&value); // push works fine

    try testing.expect(!ring.is_empty());
    try testing.expect(ring.is_full());
    try testing.expectEqual(ring.count, @as(usize, 8));

    // pop the value and compare it
    var direct_access_value: [8]u8 = undefined;
    try ring.pop(&direct_access_value);

    try testing.expectEqual(direct_access_value, value);

    try testing.expect(ring.is_empty());
}

test "push multiple elements to capacity" {
    const ringBuffer24 = RingBuffer(24);
    var ring = ringBuffer24.init(8);

    try ring.push(&val1);
    try ring.push(&val2);
    try ring.push(&val3);

    try testing.expectEqual(@as(usize, 24), ring.count);
    try testing.expect(ring.is_full());
    try testing.expect(!ring.is_empty());

    // unpack them and check the size is going down
    var val_out: [8]u8 = undefined;
    try ring.pop(&val_out);
    try testing.expectEqual(@as(usize, 16), ring.count);
    try testing.expect(!ring.is_full());
    try testing.expect(!ring.is_empty());
    try ring.pop(&val_out);
    try testing.expectEqual(@as(usize, 8), ring.count);
    try testing.expect(!ring.is_full());
    try testing.expect(!ring.is_empty());
    try ring.pop(&val_out);
    try testing.expectEqual(@as(usize, 0), ring.count);
    try testing.expect(!ring.is_full());
    try testing.expect(ring.is_empty());
}

test "pop from empty buffer returns error" {
    const ringBuffer8 = RingBuffer(8);
    var ring = ringBuffer8.init(8);
    var value: [8]u8 = undefined;

    const val = ring.pop(&value);
    try testing.expectError(RingBufferError.BufferEmpty, val);
}

test "push to full buffer returns error" {
    const ringBuffer4 = RingBuffer(32);
    var ring = ringBuffer4.init(8);

    try ring.push(&val1);
    try ring.push(&val2);
    try ring.push(&val3);
    try ring.push(&val4);

    try testing.expect(ring.is_full());
    try testing.expect(!ring.is_empty());

    const result = ring.push(&val5);

    try testing.expectError(error.BufferFull, result);
    try testing.expect(ring.is_full());
    try testing.expect(!ring.is_empty());
}

test "FIFO ordering" {
    const ringBuffer8 = RingBuffer(24);
    var ring = ringBuffer8.init(8);

    try ring.push(&val1);
    try ring.push(&val2);
    try ring.push(&val3);

    try ring.pop(&out1);
    try ring.pop(&out2);
    try ring.pop(&out3);

    try testing.expectEqual(val1, out1);
    try testing.expectEqual(val2, out2);
    try testing.expectEqual(val3, out3);
}

test "wrap around behavior" {
    const ringBuffer4 = RingBuffer(32);
    var ring = ringBuffer4.init(8);

    // fill buffer
    try ring.push(&val1);
    try ring.push(&val2);
    try ring.push(&val3);
    try ring.push(&val4);
    try testing.expect(ring.is_full());
    try testing.expect(!ring.is_empty());

    // remove two elements
    try ring.pop(&out1);
    try ring.pop(&out2);
    try testing.expect(!ring.is_full());
    try testing.expect(!ring.is_empty());

    // add two more (should wrap around)
    try ring.push(&val5);
    try ring.push(&val6);
    try testing.expect(ring.is_full());
    try testing.expect(!ring.is_empty());

    // verify order
    try ring.pop(&out1);
    try ring.pop(&out2);
    try ring.pop(&out3);
    try ring.pop(&out4);

    try testing.expectEqual(out1, val3);
    try testing.expectEqual(out2, val4);
    try testing.expectEqual(out3, val5);
    try testing.expectEqual(out4, val6);
}

test "alternating push and pop" {
    const ringBuffer8 = RingBuffer(24);
    var ring = ringBuffer8.init(8);

    try ring.push(&val1);
    try ring.pop(&out1);
    try testing.expectEqual(out1, val1);

    try ring.push(&val2);
    try ring.pop(&out2);
    try testing.expectEqual(out2, val2);

    try ring.push(&val3);
    try ring.pop(&out3);
    try testing.expectEqual(out3, val3);

    try testing.expect(ring.is_empty());
}

test "full cycle multiple times" {
    const ringBuffer3 = RingBuffer(24);
    var ring = ringBuffer3.init(8);

    // first cycle
    try ring.push(&val1);
    try ring.push(&val2);
    try ring.push(&val3);
    try ring.pop(&out1);
    try ring.pop(&out2);
    try ring.pop(&out3);

    // second cycle
    try ring.push(&val4);
    try ring.push(&val5);
    try ring.push(&val6);
    try ring.pop(&out1);
    try ring.pop(&out2);
    try ring.pop(&out3);

    try testing.expectEqual(out1, val4);
    try testing.expectEqual(out2, val5);
    try testing.expectEqual(out3, val6);
}

test "different buffer sizes compile" {
    const ringBuffer16 = RingBuffer(16);
    const ringBuffer32 = RingBuffer(32);
    const ringBuffer64 = RingBuffer(64);

    var r16 = ringBuffer16.init(8);
    var r32 = ringBuffer32.init(8);
    var r64 = ringBuffer64.init(8);

    try r16.push(&val1);
    try r32.push(&val2);
    try r64.push(&val3);

    try r16.pop(&out1);
    try r32.pop(&out2);
    try r64.pop(&out3);

    try testing.expectEqual(val1, out1);
    try testing.expectEqual(val2, out2);
    try testing.expectEqual(val3, out3);
}

test "state consistency after operations" {
    const ringBuffer5 = RingBuffer(16);
    var ring = ringBuffer5.init(8);

    try ring.push(&val1);
    try testing.expectEqual(@as(usize, 8), ring.count);
    try testing.expectEqual(@as(usize, 0), ring.start);
    try testing.expectEqual(@as(usize, 8), ring.end);
    try ring.push(&val2);
    try testing.expectEqual(@as(usize, 16), ring.count);
    try testing.expectEqual(@as(usize, 0), ring.start);
    try testing.expectEqual(@as(usize, 0), ring.end); // 0 as the buffer is full. start and end occupy the same location

    try ring.pop(&out1);
    try testing.expectEqual(@as(usize, 8), ring.count);
    try testing.expectEqual(@as(usize, 8), ring.start);
    try testing.expectEqual(@as(usize, 0), ring.end);
}
