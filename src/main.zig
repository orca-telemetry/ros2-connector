const std = @import("std");
const ros_collector = @import("ros_collector");

const constant: i32 = 5;
var variable: u16 = 100;

const inferredConst = @as(i32, 5);

const a: f64 = undefined;
const hw = [_]u8{ 'W', 'o', 'r', 'l', 'd' };

pub fn main() !void {
    // Prints to stderr, ignoring potential errors.
    std.debug.print("Hello {s}!\n", .{hw});
    try ros_collector.bufferedPrint();
}

test "simple test" {
    const gpa = std.testing.allocator;
    var list: std.ArrayList(i32) = .empty;
    defer list.deinit(gpa); // Try commenting this out and see if zig detects the memory leak!
    try list.append(gpa, 42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "fuzz example" {
    const Context = struct {
        fn testOne(context: @This(), input: []const u8) anyerror!void {
            _ = context;
            // Try passing `--fuzz` to `zig build test` and see if it manages to fail this test case!
            try std.testing.expect(!std.mem.eql(u8, "canyoufindme", input));
        }
    };
    try std.testing.fuzz(Context{}, Context.testOne, .{});
}
