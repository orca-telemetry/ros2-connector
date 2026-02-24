const std = @import("std");
const config = @import("config.zig");
const constants = @import("constants.zig");
const crypto = std.crypto;
const http = std.http;

const ProvisionPayload = struct {
    publicKey: []const u8,
};

pub fn provisionRobot(allocator: std.mem.Allocator, token: []const u8) !void {
    std.debug.print("Starting provisioning with token: {s}\n", .{token});

    // 1. generate Ed25519 Keypair
    const kp = crypto.sign.Ed25519.KeyPair.generate();

    // 2. prepare Storage
    const path = try config.ConfigStorage.getStoragePath(allocator);
    defer allocator.free(path);
    std.fs.makeDirAbsolute(path) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    var dir = try std.fs.openDirAbsolute(path, .{});
    defer dir.close();

    // 3. save Private Key as hex (Restrict permissions: 600)
    const priv_hex = std.fmt.bytesToHex(&kp.secret_key.bytes, .lower);
    // will throw error if file exists
    const priv_file = try dir.createFile(config.ConfigStorage.priv_key_file, .{ .mode = 0o600, .exclusive = true });
    try priv_file.writeAll(&priv_hex);
    priv_file.close();

    // 4. save Public Key as hex
    const pub_hex = std.fmt.bytesToHex(&kp.public_key.bytes, .lower);
    const pub_file = try dir.createFile(config.ConfigStorage.pub_key_file, .{ .exclusive = true });
    try pub_file.writeAll(&pub_hex);
    pub_file.close();

    std.debug.print("Keys generated and stored in {s}\n", .{path});

    // 5. send Public Key to Orca
    try uploadPublicKey(allocator, dir, token, kp);
}

const ProvisionResponse = struct {
    robotId: []const u8,
};

fn uploadPublicKey(allocator: std.mem.Allocator, dir: std.fs.Dir, token: []const u8, key_pair: crypto.sign.Ed25519.KeyPair) !void {
    const base64_encoder = std.base64.standard.Encoder;

    // 1. Base64 encode the public key
    var pub_b64: [base64_encoder.calcSize(32)]u8 = undefined;
    _ = base64_encoder.encode(&pub_b64, &key_pair.public_key.bytes);

    // 2. Prepare the JSON body
    const payload = ProvisionPayload{
        .publicKey = &pub_b64,
    };

    var string: std.Io.Writer.Allocating = .init(allocator);
    defer string.deinit();
    try string.writer.print("{f}", .{std.json.fmt(payload, .{})});
    const json_bytes = string.written();

    // 3. Sign the JSON body and base64 encode the signature
    const sig_bytes = try config.ConfigStorage.signPayload(allocator, json_bytes);
    var sig_b64: [base64_encoder.calcSize(64)]u8 = undefined;
    _ = base64_encoder.encode(&sig_b64, &sig_bytes);

    // 4. Setup HTTP Client
    var client = http.Client{ .allocator = allocator };
    defer client.deinit();

    var body: std.Io.Writer.Allocating = .init(allocator);
    defer body.deinit();

    // 5. Execute Fetch
    const url = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ constants.provision_base_url, token });
    defer allocator.free(url);
    const result = try client.fetch(.{
        .method = .POST,
        .location = .{ .url = url },
        .payload = json_bytes,
        .response_writer = &body.writer,
        .headers = .{
            .content_type = .{ .override = "application/json" },
        },
        .extra_headers = &.{
            .{ .name = "X-Signature", .value = &sig_b64 },
        },
    });

    // 6. Handle Response
    if (result.status != .ok) {
        std.debug.print("MotherApp upload failed with status: {d}\n", .{result.status});
        return error.UploadFailed;
    }

    // 7. Parse robot ID from response and save to disk
    const response_data = body.written();
    const parsed = std.json.parseFromSlice(ProvisionResponse, allocator, response_data, .{ .ignore_unknown_fields = true }) catch {
        std.debug.print("Failed to parse provision response: {s}\n", .{response_data});
        return error.InvalidResponse;
    };
    defer parsed.deinit();

    const robot_id_file = try dir.createFile(config.ConfigStorage.robot_config_file, .{});
    defer robot_id_file.close();
    try robot_id_file.writeAll(parsed.value.robotId);

    std.debug.print("Successfully provisioned robot: {s}\n", .{parsed.value.robotId});
}

pub fn getPublicKeyHex(allocator: std.mem.Allocator) ![]u8 {
    const path = try config.ConfigStorage.getStoragePath(allocator);
    defer allocator.free(path);
    var dir = try std.fs.openDirAbsolute(path, .{});
    defer dir.close();

    const pub_file = try dir.openFile(config.ConfigStorage.pub_key_file, .{});
    defer pub_file.close();

    // File is already stored as hex
    var hex_buf: [crypto.sign.Ed25519.PublicKey.encoded_length * 2]u8 = undefined;
    _ = try pub_file.readAll(&hex_buf);

    return try allocator.dupe(u8, &hex_buf);
}
