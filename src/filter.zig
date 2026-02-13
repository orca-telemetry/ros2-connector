const math = @import("std").math;

pub fn butterworthFilter(comptime channels: usize, comptime dt: f32, comptime wc: f32, comptime dc_gain: f32) type {
    // currently only second order
    //
    // calculate Butterworth coefficients for this filter
    //
    // y(n) = h(n)x(n) + h(n-1)x(n-1) + h(n-2)x(n-2) + h(n-3)y(n-1) + h(n-4)y(n-2)
    //
    const V = @Vector(channels, f64);

    const wc_sq = @as(f64, wc * wc);
    const dt_sq = @as(f64, dt * dt);
    const sqrt2 = @as(f64, @sqrt(2));

    const A = dt_sq * wc_sq + 2 * sqrt2 * @as(f64, dt) * @as(f64, wc) + 4;
    const B = 8 - 2 * dt_sq * wc_sq;
    const C = @as(f64, dc_gain) * dt_sq * wc_sq;

    const b0 = C / A;
    const b1 = (-2.0 * C) / A;
    const b2 = C / A;
    const a1 = -B / A;
    const a2 = -C / A;

    return struct {
        // state registers (each vector holds one "history" slot for all channels)
        x1: V = @splat(0), // x(n-1)
        x2: V = @splat(0), // x(n-2)
        y1: V = @splat(0), // y(n-1)
        y2: V = @splat(0), // y(n-2)

        pub fn iterate(self: *@This(), input: V) V {
            // y(n) = b0*x(n) + b1*x(n-1) + b2*x(n-2) + a1*y(n-1) + a2*y(n-2)
            const output = (input * @as(V, @splat(b0))) +
                (self.x1 * @as(V, @splat(b1))) +
                (self.x2 * @as(V, @splat(b2))) +
                (self.y1 * @as(V, @splat(a1))) +
                (self.y2 * @as(V, @splat(a2)));

            //shift the states
            self.x2 = self.x1;
            self.x1 = input;
            self.y2 = self.y1;
            self.y1 = output;

            return output;
        }
    };
}

pub fn processAndDecimate(filter: anytype, buffer: anytype, comptime M: usize) ?@TypeOf(filter.x1) {
    // We need at least M samples to produce 1 decimated output
    if (buffer.count < M) return null;

    var latest_output: @TypeOf(filter.x1) = undefined;

    // Process M samples, but only the last one is our decimated result
    for (0..M) |_| {
        const input_vec = buffer.pop() orelse unreachable;
        latest_output = filter.iterate(input_vec);
    }

    return latest_output;
}
