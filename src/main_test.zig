const std = @import("std");
const expect = std.testing.expect;

test "always succeeds" {
    try expect(true);
}

test "if statement" {
    const a = true;
    var x: u16 = 0;

    if (a) {
        x += 1;
    } else {
        x += 2;
    }
    try expect(x == 1);
}

test "another if statement" {
    const a = true;
    var x: u16 = 0;
    x += if (a) 1 else 2;
    try expect(x == 1);
}

test "while" {
    var i: u8 = 2;
    while (i < 100) {
        i *= 2;
    }
    try expect(i == 128);
}

test "for" {
    const string = [_]u8{ "H", "e", "l", "l", "o" };

    for (string, 0..) |character, index| {
        _ = character;
        _ = index;
    }
}
