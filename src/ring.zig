/// Contains a single channel ring buffer
const std = @import("std");

const RingBufferError = error{
    BufferFull,
    BufferEmpty,
    DestTooSmall,
    StrideMismatch,
};

/// A single channel ring buffer, with a maxium capacity in bytes
pub fn RingBuffer(comptime capacity: usize) type {
    // design choice that capacity is a factor of 2.
    // this is because most data types that will be managed
    // are of size n^2. If they're not, do we want to
    // handle them?
    std.debug.assert(capacity % 2 == 0);

    // capacity is in bytes
    return struct {
        buffer: [capacity]u8 = undefined,
        start: usize = 0,
        end: usize = 0,
        count: usize = 0, // bytes used - not the number of elements
        stride: usize = 0, // bytes per element, set on first push

        const Self = @This();

        pub fn init(stride: usize) Self {
            std.debug.assert(stride > 0);
            std.debug.assert(stride % 2 == 0);
            std.debug.assert(capacity % stride == 0);
            return .{ .stride = stride };
        }

        pub fn is_full(self: *const Self) bool {
            return self.count == capacity;
        }

        pub fn is_empty(self: *const Self) bool {
            return self.count == 0;
        }

        pub fn len(self: *const Self) usize {
            return self.count / self.stride;
        }

        pub fn max_len(self: *const Self) usize {
            return capacity / self.stride;
        }

        /// push one element (must be exactly stride bytes)
        pub fn push(self: *Self, value: []const u8) RingBufferError!void {
            if (value.len != self.stride) return error.StrideMismatch;
            if (self.count + self.stride >= capacity) return error.BufferFull;

            const end = self.end;
            // lt only here becuase of 0 indexing
            if (end + self.stride < capacity) {
                // no wraparound
                @memcpy(self.buffer[end .. end + self.stride], value);
            } else {
                // wraparound - split into two copies
                const first = capacity - end;
                @memcpy(self.buffer[end..capacity], value[0..first]);
                @memcpy(self.buffer[0 .. self.stride - first], value[first..]);
            }

            self.end = (self.end + self.stride) % capacity;
            self.count += self.stride;
        }

        /// Pop one element into dest (must be exactly stride bytes)
        pub fn pop(self: *Self, dest: []u8) RingBufferError!void {
            if (self.is_empty()) return error.BufferEmpty;
            if (dest.len != self.stride) return error.DestTooSmall;

            const start = self.start;
            if (start + self.stride <= capacity) {
                @memcpy(dest, self.buffer[start .. start + self.stride]);
            } else {
                const first = capacity - start;
                @memcpy(dest[0..first], self.buffer[start..capacity]);
                @memcpy(dest[first..], self.buffer[0 .. self.stride - first]);
            }

            self.start = (self.start + self.stride) % capacity;
            self.count -= self.stride;
        }

        /// Drain all elements into dest slice - dest.len must be a multiple of stride
        /// Returns number of elements copied
        pub fn drain(self: *Self, dest: []u8) RingBufferError!usize {
            if (dest.len % self.stride != 0) return error.StrideMismatch;
            var copied: usize = 0;
            while (!self.is_empty() and copied + self.stride <= dest.len) {
                try self.pop(dest[copied .. copied + self.stride]);
                copied += self.stride;
            }
            return copied / self.stride;
        }
    };
}
