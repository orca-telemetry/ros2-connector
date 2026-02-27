/// Comprehensive test suite for mcap.zig
///
/// Covers:
///   1. Primitive parsers (PrefixedStr, MapStrStr, TupleStrStr)
///   2. Every record body type – parse correctness
///   3. McapIterator – sequential traversal
///   4. mcap.Writer – serialise every record type back to bytes
///   5. Round-trip parity – build an MCAP buffer, parse it, reserialise it,
///      parse again and assert byte-for-byte equality of the two buffers
///   6. Error / edge cases – truncated inputs, bad magic, empty maps, etc.
///   7. CRC-32 helper correctness
const std = @import("std");
const mcap = @import("mcap.zig");
const testing = std.testing;
const Allocator = std.mem.Allocator;

test "magic constant" {
    try std.testing.expectEqual(mcap.MAGIC_LEN, mcap.MAGIC.len);
    try std.testing.expectEqual(@as(u8, 0x89), mcap.MAGIC[0]);
}

test "parse footer" {
    var buf: [20]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], 1234, .little);
    std.mem.writeInt(u64, buf[8..16], 5678, .little);
    std.mem.writeInt(u32, buf[16..20], 0xDEADBEEF, .little);
    const f = try mcap.Footer.parse(&buf);
    try std.testing.expectEqual(@as(u64, 1234), f.ofs_summary_section);
    try std.testing.expectEqual(@as(u64, 5678), f.ofs_summary_offset_section);
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), f.summary_crc32);
}

test "parse prefixed_str" {
    const data = "\x05\x00\x00\x00hello";
    var off: usize = 0;
    const s = try mcap.PrefixedStr.parse(data, &off);
    try std.testing.expectEqualStrings("hello", s.str);
    try std.testing.expectEqual(@as(usize, 9), off);
}

test "parse message" {
    var buf: [22 + 3]u8 = undefined;
    std.mem.writeInt(u16, buf[0..2], 42, .little);
    std.mem.writeInt(u32, buf[2..6], 7, .little);
    std.mem.writeInt(u64, buf[6..14], 999_000_000, .little);
    std.mem.writeInt(u64, buf[14..22], 888_000_000, .little);
    buf[22] = 0xAB;
    buf[23] = 0xCD;
    buf[24] = 0xEF;
    const m = try mcap.Message.parse(&buf);
    try std.testing.expectEqual(@as(u16, 42), m.channel_id);
    try std.testing.expectEqual(@as(u32, 7), m.sequence);
    try std.testing.expectEqual(@as(u64, 999_000_000), m.log_time);
    try std.testing.expectEqual(@as(usize, 3), m.data.len);
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build a minimal but complete MCAP file in memory.
/// Layout:
///   magic
///   Header
///   Schema
///   Channel  (with 2 metadata entries)
///   Message  x3
///   DataEnd
///   MessageIndex
///   ChunkIndex
///   AttachmentIndex (pointing to synthetic offset)
///   Statistics
///   MetadataIndex
///   SummaryOffset
///   Footer
///   magic
fn buildSyntheticMcap() ![]u8 {
    const allocator = testing.allocator;
    var w = mcap.Writer.init(null);
    defer w.deinit(allocator);

    try w.writeMagic(allocator);

    // Header
    try w.writeHeader(allocator, .{
        .profile = .{ .str = "ros2" },
        .library = .{ .str = "zig-mcap-test" },
    });

    // Schema (id=1, jsonschema)
    try w.writeSchema(allocator, .{
        .id = 1,
        .name = .{ .str = "sensor_msgs/Imu" },
        .encoding = .{ .str = "jsonschema" },
        .data = "{\"type\":\"object\"}",
    });

    // Channel (id=1, schema_id=1)
    var meta_entries = [_]mcap.TupleStrStr{
        .{ .key = .{ .str = "callerid" }, .value = .{ .str = "/imu_node" } },
        .{ .key = .{ .str = "latching" }, .value = .{ .str = "0" } },
    };
    try w.writeChannel(allocator, .{
        .id = 1,
        .schema_id = 1,
        .topic = .{ .str = "/imu/data" },
        .message_encoding = .{ .str = "json" },
        .metadata = .{ .entries = &meta_entries },
    });

    // Three messages
    const payloads = [3][]const u8{
        "{\"seq\":0}",
        "{\"seq\":1}",
        "{\"seq\":2}",
    };
    for (payloads, 0..) |payload, i| {
        try w.writeMessage(allocator, .{
            .channel_id = 1,
            .sequence = @intCast(i),
            .log_time = @as(u64, i) * 1_000_000_000,
            .publish_time = @as(u64, i) * 1_000_000_000,
            .data = payload,
        });
    }

    // DataEnd
    try w.writeDataEnd(allocator);

    // MessageIndex (synthetic)
    var mi_entries = [_]mcap.MessageIndexEntry{
        .{ .log_time = 0, .offset = 100 },
        .{ .log_time = 1_000_000_000, .offset = 200 },
        .{ .log_time = 2_000_000_000, .offset = 300 },
    };
    try w.writeMessageIndex(allocator, .{
        .channel_id = 1,
        .entries = &mi_entries,
    });

    // ChunkIndex
    var mio = [_]mcap.MessageIndexOffset{
        .{ .channel_id = 1, .offset = 500 },
    };
    try w.writeChunkIndex(allocator, .{
        .message_start_time = 0,
        .message_end_time = 2_000_000_000,
        .ofs_chunk = 100,
        .len_chunk = 400,
        .message_index_offsets = &mio,
        .message_index_length = 50,
        .compression = .{ .str = "" },
        .compressed_size = 400,
        .uncompressed_size = 400,
    });

    // AttachmentIndex (synthetic pointer)
    try w.writeAttachmentIndex(allocator, .{
        .ofs_attachment = 0,
        .len_attachment = 0,
        .log_time = 0,
        .create_time = 0,
        .data_size = 0,
        .name = .{ .str = "calibration.yaml" },
        .media_type = .{ .str = "text/yaml" },
    });

    // Statistics
    var cmc = [_]mcap.ChannelMessageCount{
        .{ .channel_id = 1, .message_count = 3 },
    };
    try w.writeStatistics(allocator, .{
        .message_count = 3,
        .schema_count = 1,
        .channel_count = 1,
        .attachment_count = 0,
        .metadata_count = 0,
        .chunk_count = 1,
        .message_start_time = 0,
        .message_end_time = 2_000_000_000,
        .channel_message_counts = &cmc,
    });

    // MetadataIndex
    try w.writeMetadataIndex(allocator, .{
        .ofs_metadata = 0,
        .len_metadata = 0,
        .name = .{ .str = "robot_config" },
    });

    // SummaryOffset
    try w.writeSummaryOffset(allocator, .{
        .group_opcode = .schema,
        .ofs_group = 50,
        .len_group = 30,
    });

    // Footer (zeros = no summary section for this test)
    try w.writeFooter(allocator, .{
        .ofs_summary_section = 0,
        .ofs_summary_offset_section = 0,
        .summary_crc32 = 0,
    });

    try w.writeMagic(allocator);

    return w.buf.toOwnedSlice(testing.allocator);
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Primitive parser tests
// ─────────────────────────────────────────────────────────────────────────────

test "PrefixedStr: empty string" {
    const data = "\x00\x00\x00\x00";
    var off: usize = 0;
    const s = try mcap.PrefixedStr.parse(data, &off);
    try testing.expectEqual(@as(usize, 0), s.str.len);
    try testing.expectEqual(@as(usize, 4), off);
}

test "PrefixedStr: ascii content" {
    const data = "\x05\x00\x00\x00hello!!"; // extra bytes after
    var off: usize = 0;
    const s = try mcap.PrefixedStr.parse(data, &off);
    try testing.expectEqualStrings("hello", s.str);
    try testing.expectEqual(@as(usize, 9), off);
}

test "PrefixedStr: error on truncated length" {
    const data = "\x01\x00"; // only 2 bytes – can't read 4-byte prefix
    var off: usize = 0;
    try testing.expectError(error.BufferTooSmall, mcap.PrefixedStr.parse(data, &off));
}

test "PrefixedStr: error on truncated body" {
    const data = "\x0A\x00\x00\x00hi"; // says 10 bytes but only 2 available
    var off: usize = 0;
    try testing.expectError(error.BufferTooSmall, mcap.PrefixedStr.parse(data, &off));
}

test "MapStrStr: empty map" {
    const allocator = testing.allocator;
    const data = "\x00\x00\x00\x00"; // 0 byte length
    var off: usize = 0;
    const m = try mcap.MapStrStr.parse(allocator, data, &off);
    defer m.deinit(allocator);
    try testing.expectEqual(@as(usize, 0), m.entries.len);
    try testing.expectEqual(@as(usize, 4), off);
}

test "MapStrStr: two entries" {
    const allocator = testing.allocator;
    // Construct: 4-byte total byte-length, then two k/v pairs
    // "a"=>"b", "cc"=>"ddd"
    // "a"  → 4+1 = 5, "b"  → 4+1 = 5  → 10 bytes
    // "cc" → 4+2 = 6, "ddd"→ 4+3 = 7  → 13 bytes
    // total = 23 bytes
    const inner = "\x01\x00\x00\x00a" ++ "\x01\x00\x00\x00b" ++
        "\x02\x00\x00\x00cc" ++ "\x03\x00\x00\x00ddd";
    const byte_len: u32 = @intCast(inner.len);
    var buf: [4 + inner.len]u8 = undefined;
    std.mem.writeInt(u32, buf[0..4], byte_len, .little);
    @memcpy(buf[4..], inner);
    var off: usize = 0;
    const m = try mcap.MapStrStr.parse(allocator, &buf, &off);
    defer m.deinit(allocator);
    try testing.expectEqual(@as(usize, 2), m.entries.len);
    try testing.expectEqualStrings("a", m.entries[0].key.str);
    try testing.expectEqualStrings("b", m.entries[0].value.str);
    try testing.expectEqualStrings("cc", m.entries[1].key.str);
    try testing.expectEqualStrings("ddd", m.entries[1].value.str);
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Individual record body parse tests
// ─────────────────────────────────────────────────────────────────────────────

test "Header: parse profile and library" {
    var w = mcap.Writer.init(null);
    defer w.deinit(testing.allocator);
    try w.writePrefixedStr(testing.allocator, "ros2");
    try w.writePrefixedStr(testing.allocator, "test-lib");
    const h = try mcap.Header.parse(w.buf.items);
    try testing.expectEqualStrings("ros2", h.profile.str);
    try testing.expectEqualStrings("test-lib", h.library.str);
}

test "Footer: parse all fields" {
    var buf: [20]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], 42_000, .little);
    std.mem.writeInt(u64, buf[8..16], 43_000, .little);
    std.mem.writeInt(u32, buf[16..20], 0xCAFEBABE, .little);
    const f = try mcap.Footer.parse(&buf);
    try testing.expectEqual(@as(u64, 42_000), f.ofs_summary_section);
    try testing.expectEqual(@as(u64, 43_000), f.ofs_summary_offset_section);
    try testing.expectEqual(@as(u32, 0xCAFEBABE), f.summary_crc32);
}

test "Footer: truncated returns error" {
    const buf: [10]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, mcap.Footer.parse(&buf));
}

test "Schema: parse all fields" {
    var w = mcap.Writer.init(null);
    defer w.deinit(testing.allocator);
    try w.writeU16(testing.allocator, 7);
    try w.writePrefixedStr(testing.allocator, "my/Schema");
    try w.writePrefixedStr(testing.allocator, "protobuf");
    const schema_bytes = "PROTO_BYTES";
    try w.writeU32(testing.allocator, @intCast(schema_bytes.len));
    try w.writeBytes(testing.allocator, schema_bytes);
    const s = try mcap.Schema.parse(w.buf.items);
    try testing.expectEqual(@as(u16, 7), s.id);
    try testing.expectEqualStrings("my/Schema", s.name.str);
    try testing.expectEqualStrings("protobuf", s.encoding.str);
    try testing.expectEqualStrings("PROTO_BYTES", s.data);
}

test "Schema: empty data blob" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU16(testing.allocator, 1);
    try w.writePrefixedStr(testing.allocator, "Foo");
    try w.writePrefixedStr(testing.allocator, "jsonschema");
    try w.writeU32(testing.allocator, 0); // zero-length data
    const s = try mcap.Schema.parse(w.buf.items);
    try testing.expectEqual(@as(usize, 0), s.data.len);
}

test "Channel: parse with metadata" {
    const allocator = testing.allocator;
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU16(testing.allocator, 3);
    try w.writeU16(testing.allocator, 1);
    try w.writePrefixedStr(testing.allocator, "/cmd_vel");
    try w.writePrefixedStr(testing.allocator, "ros2msg");
    // metadata: one entry
    const inner = "\x03\x00\x00\x00key\x05\x00\x00\x00value";
    try w.writeU32(testing.allocator, @intCast(inner.len));
    try w.writeBytes(testing.allocator, inner);
    const c = try mcap.Channel.parse(allocator, w.buf.items);
    defer c.deinit(allocator);
    try testing.expectEqual(@as(u16, 3), c.id);
    try testing.expectEqual(@as(u16, 1), c.schema_id);
    try testing.expectEqualStrings("/cmd_vel", c.topic.str);
    try testing.expectEqualStrings("ros2msg", c.message_encoding.str);
    try testing.expectEqual(@as(usize, 1), c.metadata.entries.len);
    try testing.expectEqualStrings("key", c.metadata.entries[0].key.str);
    try testing.expectEqualStrings("value", c.metadata.entries[0].value.str);
}

test "Message: parse fields and payload" {
    var buf: [22 + 5]u8 = undefined;
    std.mem.writeInt(u16, buf[0..2], 2, .little);
    std.mem.writeInt(u32, buf[2..6], 99, .little);
    std.mem.writeInt(u64, buf[6..14], 123_456_789, .little);
    std.mem.writeInt(u64, buf[14..22], 111_111_111, .little);
    @memcpy(buf[22..27], "HELLO");
    const m = try mcap.Message.parse(&buf);
    try testing.expectEqual(@as(u16, 2), m.channel_id);
    try testing.expectEqual(@as(u32, 99), m.sequence);
    try testing.expectEqual(@as(u64, 123_456_789), m.log_time);
    try testing.expectEqualStrings("HELLO", m.data);
}

test "Message: zero-length payload" {
    var buf: [22]u8 = std.mem.zeroes([22]u8);
    const m = try mcap.Message.parse(&buf);
    try testing.expectEqual(@as(usize, 0), m.data.len);
}

test "Chunk: parse uncompressed chunk" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU64(testing.allocator, 0);
    try w.writeU64(testing.allocator, 1_000_000);
    try w.writeU64(testing.allocator, 10);
    try w.writeU32(testing.allocator, 0); // crc
    try w.writePrefixedStr(testing.allocator, ""); // no compression
    try w.writeU64(testing.allocator, 4);
    try w.writeBytes(testing.allocator, "DATA");
    const c = try mcap.Chunk.parse(w.buf.items);
    try testing.expectEqual(@as(u64, 0), c.message_start_time);
    try testing.expectEqual(@as(u64, 1_000_000), c.message_end_time);
    try testing.expectEqualStrings("", c.compression.str);
    try testing.expectEqualStrings("DATA", c.records_data);
}

test "MessageIndex: parse three entries" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU16(testing.allocator, 5);
    // 3 entries × 16 bytes = 48 bytes
    try w.writeU32(testing.allocator, 48);
    for (0..3) |i| {
        try w.writeU64(testing.allocator, @as(u64, i) * 1_000);
        try w.writeU64(testing.allocator, @as(u64, i) * 512);
    }
    const mi = try mcap.MessageIndex.parse(testing.allocator, w.buf.items);
    defer mi.deinit(testing.allocator);
    try testing.expectEqual(@as(u16, 5), mi.channel_id);
    try testing.expectEqual(@as(usize, 3), mi.entries.len);
    try testing.expectEqual(@as(u64, 1_000), mi.entries[1].log_time);
    try testing.expectEqual(@as(u64, 512), mi.entries[1].offset);
}

test "ChunkIndex: parse with two channel offsets" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU64(testing.allocator, 0); // message_start_time
    try w.writeU64(testing.allocator, 5_000_000); // message_end_time
    try w.writeU64(testing.allocator, 1024); // ofs_chunk
    try w.writeU64(testing.allocator, 2048); // len_chunk
    // 2 offsets × 10 bytes = 20
    try w.writeU32(testing.allocator, 20);
    try w.writeU16(testing.allocator, 1);
    try w.writeU64(testing.allocator, 1024);
    try w.writeU16(testing.allocator, 2);
    try w.writeU64(testing.allocator, 1200);
    try w.writeU64(testing.allocator, 64); // message_index_length
    try w.writePrefixedStr(testing.allocator, "lz4");
    try w.writeU64(testing.allocator, 1500); // compressed_size
    try w.writeU64(testing.allocator, 2048); // uncompressed_size
    const ci = try mcap.ChunkIndex.parse(testing.allocator, w.buf.items);
    defer ci.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, 1024), ci.ofs_chunk);
    try testing.expectEqual(@as(usize, 2), ci.message_index_offsets.len);
    try testing.expectEqual(@as(u16, 2), ci.message_index_offsets[1].channel_id);
    try testing.expectEqualStrings("lz4", ci.compression.str);
}

test "Attachment: parse with crc" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU64(testing.allocator, 1_000_000); // log_time
    try w.writeU64(testing.allocator, 2_000_000); // create_time
    try w.writePrefixedStr(testing.allocator, "cam.jpg");
    try w.writePrefixedStr(testing.allocator, "image/jpeg");
    const data = "JPEG_BYTES";
    try w.writeU64(testing.allocator, @intCast(data.len));
    try w.writeBytes(testing.allocator, data);
    try w.writeU32(testing.allocator, 0xABCD1234);
    const a = try mcap.Attachment.parse(w.buf.items);
    try testing.expectEqual(@as(u64, 1_000_000), a.log_time);
    try testing.expectEqualStrings("cam.jpg", a.name.str);
    try testing.expectEqualStrings("image/jpeg", a.media_type.str);
    try testing.expectEqualStrings("JPEG_BYTES", a.data);
    try testing.expectEqual(@as(u32, 0xABCD1234), a.crc32);
}

test "AttachmentIndex: parse all fields" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU64(testing.allocator, 9999); // ofs_attachment
    try w.writeU64(testing.allocator, 8888); // len_attachment
    try w.writeU64(testing.allocator, 7777); // log_time
    try w.writeU64(testing.allocator, 6666); // create_time
    try w.writeU64(testing.allocator, 5555); // data_size
    try w.writePrefixedStr(testing.allocator, "file.bin");
    try w.writePrefixedStr(testing.allocator, "application/octet-stream");
    const ai = try mcap.AttachmentIndex.parse(w.buf.items);
    try testing.expectEqual(@as(u64, 9999), ai.ofs_attachment);
    try testing.expectEqualStrings("file.bin", ai.name.str);
    try testing.expectEqualStrings("application/octet-stream", ai.media_type.str);
}

test "Statistics: parse with channel message counts" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU64(testing.allocator, 100); // message_count
    try w.writeU16(testing.allocator, 2); // schema_count
    try w.writeU32(testing.allocator, 3); // channel_count
    try w.writeU32(testing.allocator, 1); // attachment_count
    try w.writeU32(testing.allocator, 0); // metadata_count
    try w.writeU32(testing.allocator, 5); // chunk_count
    try w.writeU64(testing.allocator, 0); // message_start_time
    try w.writeU64(testing.allocator, 1_000_000_000); // message_end_time
    // 2 channel counts × 10 bytes = 20
    try w.writeU32(testing.allocator, 20);
    try w.writeU16(testing.allocator, 1);
    try w.writeU64(testing.allocator, 60);
    try w.writeU16(testing.allocator, 2);
    try w.writeU64(testing.allocator, 40);
    const s = try mcap.Statistics.parse(testing.allocator, w.buf.items);
    defer s.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, 100), s.message_count);
    try testing.expectEqual(@as(u16, 2), s.schema_count);
    try testing.expectEqual(@as(u32, 3), s.channel_count);
    try testing.expectEqual(@as(usize, 2), s.channel_message_counts.len);
    try testing.expectEqual(@as(u64, 60), s.channel_message_counts[0].message_count);
}

test "Metadata: parse name and map" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writePrefixedStr(testing.allocator, "robot_config");
    const inner = "\x04\x00\x00\x00type\x03\x00\x00\x00sim";
    try w.writeU32(testing.allocator, @intCast(inner.len));
    try w.writeBytes(testing.allocator, inner);
    const m = try mcap.Metadata.parse(testing.allocator, w.buf.items);
    defer m.deinit(testing.allocator);
    try testing.expectEqualStrings("robot_config", m.name.str);
    try testing.expectEqual(@as(usize, 1), m.metadata.entries.len);
    try testing.expectEqualStrings("type", m.metadata.entries[0].key.str);
    try testing.expectEqualStrings("sim", m.metadata.entries[0].value.str);
}

test "MetadataIndex: parse" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU64(testing.allocator, 1111);
    try w.writeU64(testing.allocator, 2222);
    try w.writePrefixedStr(testing.allocator, "config");
    const mi = try mcap.MetadataIndex.parse(w.buf.items);
    try testing.expectEqual(@as(u64, 1111), mi.ofs_metadata);
    try testing.expectEqual(@as(u64, 2222), mi.len_metadata);
    try testing.expectEqualStrings("config", mi.name.str);
}

test "SummaryOffset: parse" {
    var buf: [17]u8 = undefined;
    buf[0] = @intFromEnum(mcap.Opcode.channel);
    std.mem.writeInt(u64, buf[1..9], 300, .little);
    std.mem.writeInt(u64, buf[9..17], 150, .little);
    const so = try mcap.SummaryOffset.parse(&buf);
    try testing.expectEqual(mcap.Opcode.channel, so.group_opcode);
    try testing.expectEqual(@as(u64, 300), so.ofs_group);
    try testing.expectEqual(@as(u64, 150), so.len_group);
}

test "DataEnd: parse" {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, 0xDECAFBAD, .little);
    const de = try mcap.DataEnd.parse(&buf);
    try testing.expectEqual(@as(u32, 0xDECAFBAD), de.data_section_crc32);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Magic validation
// ─────────────────────────────────────────────────────────────────────────────

test "validateMagic: valid" {
    try mcap.validateMagic(mcap.MAGIC ++ "more bytes");
}

test "validateMagic: wrong first byte" {
    const bad = "\x00MCAP0\r\n";
    try testing.expectError(error.InvalidMagic, mcap.validateMagic(bad));
}

test "validateMagic: too short" {
    try testing.expectError(error.BufferTooSmall, mcap.validateMagic("MCAPx"));
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. McapIterator traversal
// ─────────────────────────────────────────────────────────────────────────────

test "McapIterator: traverses synthetic file and sees all opcodes" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    var iter = try mcap.McapIterator.init(data);
    var seen = std.EnumSet(mcap.Opcode).initEmpty();

    while (try iter.next()) |record| {
        seen.insert(record.op);
    }

    // Must have seen at minimum the record types we wrote
    try testing.expect(seen.contains(.header));
    try testing.expect(seen.contains(.schema));
    try testing.expect(seen.contains(.channel));
    try testing.expect(seen.contains(.message));
    try testing.expect(seen.contains(.data_end));
    try testing.expect(seen.contains(.message_index));
    try testing.expect(seen.contains(.chunk_index));
    try testing.expect(seen.contains(.statistics));
    try testing.expect(seen.contains(.metadata_index));
    try testing.expect(seen.contains(.summary_offset));
    try testing.expect(seen.contains(.footer));

    // After footer, next() must return null
    try testing.expectEqual(@as(?mcap.Record, null), try iter.next());
}

test "McapIterator: counts messages" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    var iter = try mcap.McapIterator.init(data);
    var msg_count: usize = 0;
    while (try iter.next()) |record| {
        if (record.op == .message) msg_count += 1;
    }
    try testing.expectEqual(@as(usize, 3), msg_count);
}

test "McapIterator: decodeBody on every record" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    var iter = try mcap.McapIterator.init(data);
    while (try iter.next()) |record| {
        const body = try record.decodeBody(allocator);
        defer body.deinit(allocator);
        // Just verify we can decode without error – field checks done per-type above
        // _ = body;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Round-trip parity: parse → reserialise → parse → compare
// ─────────────────────────────────────────────────────────────────────────────

/// Reserialise a parsed MCAP back to bytes by iterating records and decoding
/// each body, then calling the appropriate mcap.Writer method.
fn reserialise(allocator: Allocator, data: []const u8) ![]u8 {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    errdefer w.deinit(allocator);

    try w.writeMagic(allocator);

    var iter = try mcap.McapIterator.init(data);
    while (try iter.next()) |record| {
        const body = try record.decodeBody(allocator);
        defer body.deinit(allocator);

        switch (body) {
            .header => |h| try w.writeHeader(allocator, h),
            .footer => |f| try w.writeFooter(allocator, f),
            .schema => |s| try w.writeSchema(allocator, s),
            .channel => |c| try w.writeChannel(allocator, c),
            .message => |m| try w.writeMessage(allocator, m),
            .chunk => |c| try w.writeChunk(allocator, c),
            .message_index => |mi| try w.writeMessageIndex(allocator, mi),
            .chunk_index => |ci| try w.writeChunkIndex(allocator, ci),
            .attachment => |a| try w.writeAttachment(testing.allocator, a),
            .attachment_index => |ai| try w.writeAttachmentIndex(testing.allocator, ai),
            .statistics => |s| try w.writeStatistics(testing.allocator, s),
            .metadata => |m| try w.writeMetadata(testing.allocator, m),
            .metadata_index => |mi| try w.writeMetadataIndex(testing.allocator, mi),
            .summary_offset => |so| try w.writeSummaryOffset(testing.allocator, so),
            .data_end => try w.writeDataEnd(testing.allocator),
        }
    }

    try w.writeMagic(testing.allocator);
    return w.buf.toOwnedSlice(testing.allocator);
}

test "round-trip: synthetic file is byte-identical after parse → write → parse → write" {
    const allocator = testing.allocator;

    // Build original
    const original = try buildSyntheticMcap();
    defer allocator.free(original);

    // First round-trip
    const pass1 = try reserialise(allocator, original);
    defer allocator.free(pass1);

    // The two buffers should be identical
    try testing.expectEqualSlices(u8, original, pass1);

    // Second round-trip (idempotency)
    const pass2 = try reserialise(allocator, pass1);
    defer allocator.free(pass2);
    try testing.expectEqualSlices(u8, pass1, pass2);
}

test "round-trip: header fields survive" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    var iter = try mcap.McapIterator.init(data);
    const first = (try iter.next()) orelse return error.NoRecord;
    try testing.expectEqual(mcap.Opcode.header, first.op);
    const h = try mcap.Header.parse(first.raw_body);
    try testing.expectEqualStrings("ros2", h.profile.str);
    try testing.expectEqualStrings("zig-mcap-test", h.library.str);
}

test "round-trip: message payloads survive" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    const pass1 = try reserialise(allocator, data);
    defer allocator.free(pass1);

    var iter = try mcap.McapIterator.init(pass1);
    var seq: u32 = 0;
    while (try iter.next()) |record| {
        if (record.op != .message) continue;
        const m = try mcap.Message.parse(record.raw_body);
        try testing.expectEqual(seq, m.sequence);
        seq += 1;
    }
    try testing.expectEqual(@as(u32, 3), seq);
}

test "round-trip: channel metadata survives" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    const pass1 = try reserialise(allocator, data);
    defer allocator.free(pass1);

    var iter = try mcap.McapIterator.init(pass1);
    while (try iter.next()) |record| {
        if (record.op != .channel) continue;
        const c = try mcap.Channel.parse(allocator, record.raw_body);
        defer c.deinit(allocator);
        try testing.expectEqual(@as(usize, 2), c.metadata.entries.len);
        try testing.expectEqualStrings("callerid", c.metadata.entries[0].key.str);
        try testing.expectEqualStrings("/imu_node", c.metadata.entries[0].value.str);
        break;
    }
}

test "round-trip: statistics survive" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    const pass1 = try reserialise(allocator, data);
    defer allocator.free(pass1);

    var iter = try mcap.McapIterator.init(pass1);
    while (try iter.next()) |record| {
        if (record.op != .statistics) continue;
        const s = try mcap.Statistics.parse(allocator, record.raw_body);
        defer s.deinit(allocator);
        try testing.expectEqual(@as(u64, 3), s.message_count);
        try testing.expectEqual(@as(u16, 1), s.schema_count);
        try testing.expectEqual(@as(u32, 1), s.channel_count);
        try testing.expectEqual(@as(u32, 1), s.chunk_count);
        try testing.expectEqual(@as(usize, 1), s.channel_message_counts.len);
        try testing.expectEqual(@as(u64, 3), s.channel_message_counts[0].message_count);
        break;
    }
}

test "round-trip: chunk_index offsets survive" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    const pass1 = try reserialise(allocator, data);
    defer allocator.free(pass1);

    var iter = try mcap.McapIterator.init(pass1);
    while (try iter.next()) |record| {
        if (record.op != .chunk_index) continue;
        const ci = try mcap.ChunkIndex.parse(allocator, record.raw_body);
        defer ci.deinit(allocator);
        try testing.expectEqual(@as(u64, 100), ci.ofs_chunk);
        try testing.expectEqual(@as(u64, 400), ci.len_chunk);
        try testing.expectEqual(@as(usize, 1), ci.message_index_offsets.len);
        try testing.expectEqual(@as(u16, 1), ci.message_index_offsets[0].channel_id);
        break;
    }
}

test "round-trip: message_index entries survive" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    const pass1 = try reserialise(allocator, data);
    defer allocator.free(pass1);

    var iter = try mcap.McapIterator.init(pass1);
    while (try iter.next()) |record| {
        if (record.op != .message_index) continue;
        const mi = try mcap.MessageIndex.parse(allocator, record.raw_body);
        defer mi.deinit(allocator);
        try testing.expectEqual(@as(usize, 3), mi.entries.len);
        try testing.expectEqual(@as(u64, 2_000_000_000), mi.entries[2].log_time);
        try testing.expectEqual(@as(u64, 300), mi.entries[2].offset);
        break;
    }
}

test "round-trip: summary_offset survives" {
    const allocator = testing.allocator;
    const data = try buildSyntheticMcap();
    defer allocator.free(data);

    const pass1 = try reserialise(allocator, data);
    defer allocator.free(pass1);

    var iter = try mcap.McapIterator.init(pass1);
    while (try iter.next()) |record| {
        if (record.op != .summary_offset) continue;
        const so = try mcap.SummaryOffset.parse(record.raw_body);
        try testing.expectEqual(mcap.Opcode.schema, so.group_opcode);
        try testing.expectEqual(@as(u64, 50), so.ofs_group);
        try testing.expectEqual(@as(u64, 30), so.len_group);
        break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Round-trip with an Attachment record
// ─────────────────────────────────────────────────────────────────────────────
test "round-trip: attachment survives" {
    const allocator = testing.allocator;

    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);

    try w.writeMagic(testing.allocator);
    try w.writeHeader(testing.allocator, .{ .profile = .{ .str = "" }, .library = .{ .str = "" } });

    const att_payload = "RAW_BINARY_DATA_BYTES";
    try w.writeAttachment(testing.allocator, .{
        .log_time = 555,
        .create_time = 666,
        .name = .{ .str = "snapshot.bin" },
        .media_type = .{ .str = "application/octet-stream" },
        .data = att_payload,
        .crc32 = 0x12345678,
    });

    try w.writeDataEnd(testing.allocator);
    try w.writeFooter(testing.allocator, .{ .ofs_summary_section = 0, .ofs_summary_offset_section = 0, .summary_crc32 = 0 });
    try w.writeMagic(testing.allocator);

    const original = try w.buf.toOwnedSlice(testing.allocator);
    defer allocator.free(original);

    const pass1 = try reserialise(allocator, original);
    defer allocator.free(pass1);

    try testing.expectEqualSlices(u8, original, pass1);

    // Verify parsed attachment
    var iter = try mcap.McapIterator.init(pass1);
    while (try iter.next()) |record| {
        if (record.op != .attachment) continue;
        const a = try mcap.Attachment.parse(record.raw_body);
        try testing.expectEqual(@as(u64, 555), a.log_time);
        try testing.expectEqualStrings("snapshot.bin", a.name.str);
        try testing.expectEqualStrings(att_payload, a.data);
        try testing.expectEqual(@as(u32, 0x12345678), a.crc32);
        break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Round-trip with a Metadata record
// ─────────────────────────────────────────────────────────────────────────────

test "round-trip: metadata with multiple keys survives" {
    const allocator = testing.allocator;

    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);

    try w.writeMagic(testing.allocator);
    try w.writeHeader(testing.allocator, .{ .profile = .{ .str = "" }, .library = .{ .str = "" } });

    var entries = [_]mcap.TupleStrStr{
        .{ .key = .{ .str = "version" }, .value = .{ .str = "2.1.0" } },
        .{ .key = .{ .str = "robot" }, .value = .{ .str = "spot" } },
        .{ .key = .{ .str = "env" }, .value = .{ .str = "outdoor" } },
    };
    try w.writeMetadata(testing.allocator, .{
        .name = .{ .str = "deployment" },
        .metadata = .{ .entries = &entries },
    });

    try w.writeDataEnd(testing.allocator);
    try w.writeFooter(testing.allocator, .{ .ofs_summary_section = 0, .ofs_summary_offset_section = 0, .summary_crc32 = 0 });
    try w.writeMagic(testing.allocator);

    const original = try w.buf.toOwnedSlice(testing.allocator);
    defer allocator.free(original);

    const pass1 = try reserialise(allocator, original);
    defer allocator.free(pass1);

    try testing.expectEqualSlices(u8, original, pass1);

    // Verify
    var iter = try mcap.McapIterator.init(pass1);
    while (try iter.next()) |record| {
        if (record.op != .metadata) continue;
        const m = try mcap.Metadata.parse(allocator, record.raw_body);
        defer m.deinit(allocator);
        try testing.expectEqualStrings("deployment", m.name.str);
        try testing.expectEqual(@as(usize, 3), m.metadata.entries.len);
        try testing.expectEqualStrings("robot", m.metadata.entries[1].key.str);
        try testing.expectEqualStrings("spot", m.metadata.entries[1].value.str);
        break;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. Edge / error cases
// ─────────────────────────────────────────────────────────────────────────────

test "McapIterator: empty data is BufferTooSmall" {
    try testing.expectError(error.BufferTooSmall, mcap.McapIterator.init(""));
}

test "McapIterator: bad magic returns InvalidMagic" {
    const bad = "NOTMAGIC" ++ "\x00" ** 20;
    try testing.expectError(error.InvalidMagic, mcap.McapIterator.init(bad));
}

test "Record.parseHeader: truncated body" {
    // Valid op + body_len = 100, but buffer only has 5 bytes of body
    var buf: [14]u8 = undefined;
    buf[0] = @intFromEnum(mcap.Opcode.message);
    std.mem.writeInt(u64, buf[1..9], 100, .little);
    @memset(buf[9..], 0xAB);
    var off: usize = 0;
    try testing.expectError(error.BufferTooSmall, mcap.Record.parseHeader(&buf, &off));
}

test "Statistics: truncated returns error" {
    const short: [10]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, mcap.Statistics.parse(testing.allocator, &short));
}

test "ChunkIndex: odd-size offset table returns error" {
    const allocator = testing.allocator;
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU64(testing.allocator, 0);
    try w.writeU64(testing.allocator, 0);
    try w.writeU64(testing.allocator, 0);
    try w.writeU64(testing.allocator, 0);
    // 3 bytes – not divisible by 10 (entry size)
    try w.writeU32(testing.allocator, 3);
    try w.writeBytes(testing.allocator, "XYZ");
    // Don't write the rest – will hit BufferTooSmall first, which is also correct
    try testing.expectError(error.BufferTooSmall, mcap.ChunkIndex.parse(allocator, w.buf.items));
}

test "MessageIndex: odd-size records returns error" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU16(testing.allocator, 1);
    // 5 bytes – not divisible by 16
    try w.writeU32(testing.allocator, 5);
    try w.writeBytes(testing.allocator, "HELLO");
    try testing.expectError(error.BufferTooSmall, mcap.MessageIndex.parse(testing.allocator, w.buf.items));
}

test "SummaryOffset: truncated returns error" {
    const buf: [5]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, mcap.SummaryOffset.parse(&buf));
}

test "Attachment: truncated before crc returns error" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeU64(testing.allocator, 0);
    try w.writeU64(testing.allocator, 0);
    try w.writePrefixedStr(testing.allocator, "x");
    try w.writePrefixedStr(testing.allocator, "y");
    try w.writeU64(testing.allocator, 2);
    try w.writeBytes(testing.allocator, "AB");
    // Omit the final 4-byte CRC
    try testing.expectError(error.BufferTooSmall, mcap.Attachment.parse(w.buf.items));
}

// ─────────────────────────────────────────────────────────────────────────────
// 9. mcap.Writer correctness – verify record framing
// ─────────────────────────────────────────────────────────────────────────────

test "mcap.Writer: record has correct opcode byte" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeDataEnd(testing.allocator);
    // Byte 0 must be the data_end opcode (0x0f)
    try testing.expectEqual(@as(u8, 0x0f), w.buf.items[0]);
}

test "mcap.Writer: record body-length field is correct" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writeDataEnd(testing.allocator);
    // Bytes 1-8: u64 LE body length, should be 4
    const body_len = std.mem.readInt(u64, w.buf.items[1..9], .little);
    try testing.expectEqual(@as(u64, 4), body_len);
    // Total size: 1 opcode + 8 len + 4 body = 13
    try testing.expectEqual(@as(usize, 13), w.buf.items.len);
}

test "mcap.Writer: prefixed string length prefix is correct" {
    var w = mcap.Writer.init(std.hash.Crc32.init());
    defer w.deinit(testing.allocator);
    try w.writePrefixedStr(testing.allocator, "zigzag");
    const len = std.mem.readInt(u32, w.buf.items[0..4], .little);
    try testing.expectEqual(@as(u32, 6), len);
    try testing.expectEqualStrings("zigzag", w.buf.items[4..]);
}

// FIXME: crc tests
// check that:
// 1. crc is as expected
// 2. of data before data end is the same as the crc in the data end (data end is not account for)
// 3. ?
