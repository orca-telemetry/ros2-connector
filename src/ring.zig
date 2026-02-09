const RingBufferError = error{BufferFull};

pub fn ringBuffer(comptime capacity: usize) type {
    return struct {
        buffer: [capacity]f64 = undefined,
        start: usize = 0,
        end: usize = 0,
        count: usize = 0,

        const Self = @This();

        pub fn is_full(self: *const Self) bool {
            return self.count == capacity;
        }

        pub fn is_empty(self: *const Self) bool {
            return self.count == 0;
        }

        pub fn push(self: *Self, value: f64) RingBufferError!void {
            if (self.is_full()) {
                return error.BufferFull;
            }
            self.buffer[self.end] = value;
            self.end = (self.end + 1) % capacity;
            self.count += 1;
        }

        pub fn pop(self: *Self) ?f64 {
            if (self.is_empty()) {
                return null;
            }
            const value = self.buffer[self.start];
            self.start = (self.start + 1) % capacity;
            self.count -= 1;
            return value;
        }
    };
}
