const std = @import("std");
const crypto = std.crypto;

pub const RobotConfig = struct {
    id: []u8,
};

pub const ConfigStorage = struct {
    const dir_name = ".orca";
    pub const pub_key_file = "id_ed25519.pub";
    pub const priv_key_file = "id_ed25519";
    pub const robot_config_file = "config.json";

    pub fn getStoragePath(allocator: std.mem.Allocator) ![]u8 {
        const home_owned = std.process.getEnvVarOwned(allocator, "HOME") catch null;
        defer if (home_owned) |h| allocator.free(h);
        const home = home_owned orelse "/tmp";
        return std.fs.path.join(allocator, &.{ home, dir_name });
    }

    pub fn getRobotId(allocator: std.mem.Allocator) ![]u8 {
        const storage_path = try getStoragePath(allocator);
        defer allocator.free(storage_path);

        var dir = try std.fs.openDirAbsolute(storage_path, .{});
        defer dir.close();

        const file_contents = try dir.readFileAlloc(allocator, "config.json", 1024 * 1024);
        defer allocator.free(file_contents);

        const parsed = try std.json.parseFromSlice(RobotConfig, allocator, file_contents, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();

        return try allocator.dupe(u8, parsed.value.id);
    }

    pub fn signPayload(allocator: std.mem.Allocator, message: []const u8) ![64]u8 { // Note: Ed25519 signature is 64 bytes
        const storage_path = try getStoragePath(allocator);
        defer allocator.free(storage_path);

        var dir = try std.fs.openDirAbsolute(storage_path, .{});
        defer dir.close();

        // 1. Ed25519 SecretKey is 64 bytes. If stored as HEX, the file is 128 bytes.
        var hex_buffer: [crypto.sign.Ed25519.SecretKey.encoded_length * 2]u8 = undefined;
        const file = try dir.openFile(priv_key_file, .{});
        defer file.close();

        const amt = try file.readAll(&hex_buffer);
        if (amt < hex_buffer.len) return error.InvalidKeyLength;

        // 2. Decode the Hex string into actual bytes
        var secret_key_bytes: [crypto.sign.Ed25519.SecretKey.encoded_length]u8 = undefined;
        _ = try std.fmt.hexToBytes(&secret_key_bytes, &hex_buffer);

        // 3. Now this will work!
        const secretKey = try crypto.sign.Ed25519.SecretKey.fromBytes(secret_key_bytes);
        const keypair = try crypto.sign.Ed25519.KeyPair.fromSecretKey(secretKey);

        const sig = try keypair.sign(message, null);
        return sig.toBytes();
    }
};
