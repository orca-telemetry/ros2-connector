/// Pure Zig implementation of the MCAP binary file format.
/// Spec: https://github.com/foxglove/mcap/tree/c1cc51d/docs/specification#readme
///
/// MCAP is a modular container file format for pub/sub messages with arbitrary
/// message serialization, primarily used in robotics applications.
///
/// File layout:
///   <magic> <record>* <footer_record> <magic>
///
/// All multi-byte integers are little-endian.
/// Time values (log_time, publish_time, create_time) are nanoseconds since epoch.
const std = @import("std");
const Allocator = std.mem.Allocator;

// ─────────────────────────────────────────────────────────────────────────────
// Magic
// ─────────────────────────────────────────────────────────────────────────────

pub const MAGIC = "\x89MCAP0\r\n";
pub const MAGIC_LEN = 8;

// ─────────────────────────────────────────────────────────────────────────────
// Opcodes
// ─────────────────────────────────────────────────────────────────────────────

pub const Opcode = enum(u8) {
    header = 0x01,
    footer = 0x02,
    schema = 0x03,
    channel = 0x04,
    message = 0x05,
    chunk = 0x06,
    message_index = 0x07,
    chunk_index = 0x08,
    attachment = 0x09,
    attachment_index = 0x0a,
    statistics = 0x0b,
    metadata = 0x0c,
    metadata_index = 0x0d,
    summary_offset = 0x0e,
    data_end = 0x0f,
    _,
};

// ─────────────────────────────────────────────────────────────────────────────
// Primitive / helper types
// ─────────────────────────────────────────────────────────────────────────────

/// A length-prefixed UTF-8 string (4-byte LE length prefix).
/// The returned slice points into the source buffer – no copy is made.
pub const PrefixedStr = struct {
    str: []const u8,

    pub fn parse(data: []const u8, offset: *usize) error{BufferTooSmall}!PrefixedStr {
        if (offset.* + 4 > data.len) return error.BufferTooSmall;
        const len = std.mem.readInt(u32, data[offset.*..][0..4], .little);
        offset.* += 4;
        if (offset.* + len > data.len) return error.BufferTooSmall;
        const s = data[offset.* .. offset.* + len];
        offset.* += len;
        return .{ .str = s };
    }
};

/// A single key-value string pair.
pub const TupleStrStr = struct {
    key: PrefixedStr,
    value: PrefixedStr,

    pub fn parse(data: []const u8, offset: *usize) error{BufferTooSmall}!TupleStrStr {
        const key = try PrefixedStr.parse(data, offset);
        const value = try PrefixedStr.parse(data, offset);
        return .{ .key = key, .value = value };
    }
};

/// A map of string→string, length-prefixed (4-byte LE byte-count of all entries).
/// Entries are stored in the order they appear in the file.
pub const MapStrStr = struct {
    entries: []TupleStrStr,

    pub fn parse(allocator: Allocator, data: []const u8, offset: *usize) !MapStrStr {
        if (offset.* + 4 > data.len) return error.BufferTooSmall;
        const byte_len = std.mem.readInt(u32, data[offset.*..][0..4], .little);
        offset.* += 4;
        if (offset.* + byte_len > data.len) return error.BufferTooSmall;

        const entries_data = data[offset.* .. offset.* + byte_len];
        offset.* += byte_len;

        var inner: usize = 0;
        var list: std.ArrayList(TupleStrStr) = .empty;
        defer list.deinit(allocator);

        while (inner < entries_data.len) {
            const entry = try TupleStrStr.parse(entries_data, &inner);
            try list.append(allocator, entry);
        }

        return .{ .entries = try list.toOwnedSlice(allocator) };
    }

    pub fn deinit(self: MapStrStr, allocator: Allocator) void {
        allocator.free(self.entries);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Record body types
// ─────────────────────────────────────────────────────────────────────────────

pub const Header = struct {
    profile: PrefixedStr,
    library: PrefixedStr,

    pub fn parse(data: []const u8) error{BufferTooSmall}!Header {
        var off: usize = 0;
        const profile = try PrefixedStr.parse(data, &off);
        const library = try PrefixedStr.parse(data, &off);
        return .{ .profile = profile, .library = library };
    }
};

pub const Footer = struct {
    /// Byte offset to the start of the Summary section (0 = absent).
    ofs_summary_section: u64,
    /// Byte offset to the start of the Summary Offset section (0 = absent).
    ofs_summary_offset_section: u64,
    /// CRC-32 of all bytes from the Summary section start through
    /// `ofs_summary_offset_section`. 0 = not available.
    summary_crc32: u32,

    pub fn parse(data: []const u8) error{BufferTooSmall}!Footer {
        if (data.len < 20) return error.BufferTooSmall;
        return .{
            .ofs_summary_section = std.mem.readInt(u64, data[0..8], .little),
            .ofs_summary_offset_section = std.mem.readInt(u64, data[8..16], .little),
            .summary_crc32 = std.mem.readInt(u32, data[16..20], .little),
        };
    }
};

pub const Schema = struct {
    id: u16,
    name: PrefixedStr,
    encoding: PrefixedStr,
    /// Raw schema data (format depends on `encoding`).
    data: []const u8,

    pub fn parse(data: []const u8) error{BufferTooSmall}!Schema {
        if (data.len < 2) return error.BufferTooSmall;
        var off: usize = 0;
        const id = std.mem.readInt(u16, data[off..][0..2], .little);
        off += 2;
        const name = try PrefixedStr.parse(data, &off);
        const encoding = try PrefixedStr.parse(data, &off);
        if (off + 4 > data.len) return error.BufferTooSmall;
        const data_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + data_len > data.len) return error.BufferTooSmall;
        const schema_data = data[off .. off + data_len];
        return .{ .id = id, .name = name, .encoding = encoding, .data = schema_data };
    }
};

pub const Channel = struct {
    id: u16,
    schema_id: u16,
    topic: PrefixedStr,
    message_encoding: PrefixedStr,
    metadata: MapStrStr,

    pub fn parse(allocator: Allocator, data: []const u8) !Channel {
        if (data.len < 4) return error.BufferTooSmall;
        var off: usize = 0;
        const id = std.mem.readInt(u16, data[off..][0..2], .little);
        off += 2;
        const schema_id = std.mem.readInt(u16, data[off..][0..2], .little);
        off += 2;
        const topic = try PrefixedStr.parse(data, &off);
        const message_encoding = try PrefixedStr.parse(data, &off);
        const metadata = try MapStrStr.parse(allocator, data, &off);
        return .{
            .id = id,
            .schema_id = schema_id,
            .topic = topic,
            .message_encoding = message_encoding,
            .metadata = metadata,
        };
    }

    pub fn deinit(self: Channel, allocator: Allocator) void {
        self.metadata.deinit(allocator);
    }
};

pub const Message = struct {
    channel_id: u16,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    /// Message payload; points into the original buffer.
    data: []const u8,

    pub fn parse(data: []const u8) error{BufferTooSmall}!Message {
        const header_len = 2 + 4 + 8 + 8; // 22 bytes
        if (data.len < header_len) return error.BufferTooSmall;
        return .{
            .channel_id = std.mem.readInt(u16, data[0..2], .little),
            .sequence = std.mem.readInt(u32, data[2..6], .little),
            .log_time = std.mem.readInt(u64, data[6..14], .little),
            .publish_time = std.mem.readInt(u64, data[14..22], .little),
            .data = data[header_len..],
        };
    }
};

pub const Chunk = struct {
    message_start_time: u64,
    message_end_time: u64,
    uncompressed_size: u64,
    /// CRC-32 of uncompressed records. 0 = skip validation.
    uncompressed_crc32: u32,
    compression: PrefixedStr,
    /// Raw (possibly compressed) records bytes.
    records_data: []const u8,

    pub fn parse(data: []const u8) error{BufferTooSmall}!Chunk {
        if (data.len < 8 + 8 + 8 + 4) return error.BufferTooSmall;
        var off: usize = 0;
        const message_start_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const message_end_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const uncompressed_size = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const uncompressed_crc32 = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        const compression = try PrefixedStr.parse(data, &off);
        if (off + 8 > data.len) return error.BufferTooSmall;
        const records_len = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        if (off + records_len > data.len) return error.BufferTooSmall;
        const records_data = data[off .. off + records_len];
        return .{
            .message_start_time = message_start_time,
            .message_end_time = message_end_time,
            .uncompressed_size = uncompressed_size,
            .uncompressed_crc32 = uncompressed_crc32,
            .compression = compression,
            .records_data = records_data,
        };
    }
};

pub const MessageIndexEntry = struct {
    log_time: u64,
    offset: u64,
};

pub const MessageIndex = struct {
    channel_id: u16,
    entries: []MessageIndexEntry,

    pub fn parse(allocator: Allocator, data: []const u8) !MessageIndex {
        if (data.len < 2 + 4) return error.BufferTooSmall;
        var off: usize = 0;
        const channel_id = std.mem.readInt(u16, data[off..][0..2], .little);
        off += 2;
        const byte_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + byte_len > data.len) return error.BufferTooSmall;

        const entries_data = data[off .. off + byte_len];
        const entry_count = entries_data.len / 16; // each entry is 8+8 bytes
        if (entries_data.len % 16 != 0) return error.BufferTooSmall;

        const entries = try allocator.alloc(MessageIndexEntry, entry_count);
        for (entries, 0..) |*e, i| {
            const base = i * 16;
            e.log_time = std.mem.readInt(u64, entries_data[base..][0..8], .little);
            e.offset = std.mem.readInt(u64, entries_data[base + 8 ..][0..8], .little);
        }
        return .{ .channel_id = channel_id, .entries = entries };
    }

    pub fn deinit(self: MessageIndex, allocator: Allocator) void {
        allocator.free(self.entries);
    }
};

pub const MessageIndexOffset = struct {
    channel_id: u16,
    offset: u64,
};

pub const ChunkIndex = struct {
    message_start_time: u64,
    message_end_time: u64,
    /// Byte offset of the chunk record from the start of the file.
    ofs_chunk: u64,
    len_chunk: u64,
    message_index_offsets: []MessageIndexOffset,
    message_index_length: u64,
    compression: PrefixedStr,
    compressed_size: u64,
    uncompressed_size: u64,

    pub fn parse(allocator: Allocator, data: []const u8) !ChunkIndex {
        if (data.len < 8 * 3 + 4) return error.BufferTooSmall;
        var off: usize = 0;

        const message_start_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const message_end_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const ofs_chunk = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const len_chunk = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;

        if (off + 4 > data.len) return error.BufferTooSmall;
        const mio_byte_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        if (off + mio_byte_len > data.len) return error.BufferTooSmall;

        const mio_data = data[off .. off + mio_byte_len];
        off += mio_byte_len;

        const entry_size = 2 + 8; // u16 + u64
        const entry_count = mio_data.len / entry_size;
        if (mio_data.len % entry_size != 0) return error.BufferTooSmall;

        const offsets = try allocator.alloc(MessageIndexOffset, entry_count);
        errdefer allocator.free(offsets);
        for (offsets, 0..) |*o, i| {
            const base = i * entry_size;
            o.channel_id = std.mem.readInt(u16, mio_data[base..][0..2], .little);
            o.offset = std.mem.readInt(u64, mio_data[base + 2 ..][0..8], .little);
        }

        if (off + 8 > data.len) return error.BufferTooSmall;
        const message_index_length = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;

        const compression = try PrefixedStr.parse(data, &off);

        if (off + 16 > data.len) return error.BufferTooSmall;
        const compressed_size = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const uncompressed_size = std.mem.readInt(u64, data[off..][0..8], .little);

        return .{
            .message_start_time = message_start_time,
            .message_end_time = message_end_time,
            .ofs_chunk = ofs_chunk,
            .len_chunk = len_chunk,
            .message_index_offsets = offsets,
            .message_index_length = message_index_length,
            .compression = compression,
            .compressed_size = compressed_size,
            .uncompressed_size = uncompressed_size,
        };
    }

    pub fn deinit(self: ChunkIndex, allocator: Allocator) void {
        allocator.free(self.message_index_offsets);
    }
};

pub const Attachment = struct {
    log_time: u64,
    create_time: u64,
    name: PrefixedStr,
    media_type: PrefixedStr,
    /// Raw attachment bytes; points into the source buffer.
    data: []const u8,
    /// CRC-32 of all preceding fields. 0 = not validated.
    crc32: u32,

    pub fn parse(data: []const u8) error{BufferTooSmall}!Attachment {
        if (data.len < 16) return error.BufferTooSmall;
        var off: usize = 0;
        const log_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const create_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const name = try PrefixedStr.parse(data, &off);
        const media_type = try PrefixedStr.parse(data, &off);
        if (off + 8 > data.len) return error.BufferTooSmall;
        const data_len = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        if (off + data_len > data.len) return error.BufferTooSmall;
        const attachment_data = data[off .. off + data_len];
        off += data_len;
        if (off + 4 > data.len) return error.BufferTooSmall;
        const crc32_ = std.mem.readInt(u32, data[off..][0..4], .little);
        return .{
            .log_time = log_time,
            .create_time = create_time,
            .name = name,
            .media_type = media_type,
            .data = attachment_data,
            .crc32 = crc32_,
        };
    }
};

pub const AttachmentIndex = struct {
    /// Byte offset of the Attachment record from the start of the file.
    ofs_attachment: u64,
    len_attachment: u64,
    log_time: u64,
    create_time: u64,
    data_size: u64,
    name: PrefixedStr,
    media_type: PrefixedStr,

    pub fn parse(data: []const u8) error{BufferTooSmall}!AttachmentIndex {
        if (data.len < 5 * 8) return error.BufferTooSmall;
        var off: usize = 0;
        const ofs_attachment = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const len_attachment = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const log_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const create_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const data_size = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const name = try PrefixedStr.parse(data, &off);
        const media_type = try PrefixedStr.parse(data, &off);
        return .{
            .ofs_attachment = ofs_attachment,
            .len_attachment = len_attachment,
            .log_time = log_time,
            .create_time = create_time,
            .data_size = data_size,
            .name = name,
            .media_type = media_type,
        };
    }
};

pub const ChannelMessageCount = struct {
    channel_id: u16,
    message_count: u64,
};

pub const Statistics = struct {
    message_count: u64,
    schema_count: u16,
    channel_count: u32,
    attachment_count: u32,
    metadata_count: u32,
    chunk_count: u32,
    message_start_time: u64,
    message_end_time: u64,
    channel_message_counts: []ChannelMessageCount,

    pub fn parse(allocator: Allocator, data: []const u8) !Statistics {
        const fixed_len = 8 + 2 + 4 + 4 + 4 + 4 + 8 + 8 + 4;
        if (data.len < fixed_len) return error.BufferTooSmall;
        var off: usize = 0;

        const message_count = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const schema_count = std.mem.readInt(u16, data[off..][0..2], .little);
        off += 2;
        const channel_count = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        const attachment_count = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        const metadata_count = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        const chunk_count = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;
        const message_start_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const message_end_time = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const cmc_byte_len = std.mem.readInt(u32, data[off..][0..4], .little);
        off += 4;

        if (off + cmc_byte_len > data.len) return error.BufferTooSmall;
        const cmc_data = data[off .. off + cmc_byte_len];
        const entry_size = 2 + 8;
        const entry_count = cmc_data.len / entry_size;
        if (cmc_data.len % entry_size != 0) return error.BufferTooSmall;

        const counts = try allocator.alloc(ChannelMessageCount, entry_count);
        for (counts, 0..) |*c, i| {
            const base = i * entry_size;
            c.channel_id = std.mem.readInt(u16, cmc_data[base..][0..2], .little);
            c.message_count = std.mem.readInt(u64, cmc_data[base + 2 ..][0..8], .little);
        }

        return .{
            .message_count = message_count,
            .schema_count = schema_count,
            .channel_count = channel_count,
            .attachment_count = attachment_count,
            .metadata_count = metadata_count,
            .chunk_count = chunk_count,
            .message_start_time = message_start_time,
            .message_end_time = message_end_time,
            .channel_message_counts = counts,
        };
    }

    pub fn deinit(self: Statistics, allocator: Allocator) void {
        allocator.free(self.channel_message_counts);
    }
};

pub const Metadata = struct {
    name: PrefixedStr,
    metadata: MapStrStr,

    pub fn parse(allocator: Allocator, data: []const u8) !Metadata {
        var off: usize = 0;
        const name = try PrefixedStr.parse(data, &off);
        const metadata = try MapStrStr.parse(allocator, data, &off);
        return .{ .name = name, .metadata = metadata };
    }

    pub fn deinit(self: Metadata, allocator: Allocator) void {
        self.metadata.deinit(allocator);
    }
};

pub const MetadataIndex = struct {
    /// Byte offset of the Metadata record from the start of the file.
    ofs_metadata: u64,
    len_metadata: u64,
    name: PrefixedStr,

    pub fn parse(data: []const u8) error{BufferTooSmall}!MetadataIndex {
        if (data.len < 16) return error.BufferTooSmall;
        var off: usize = 0;
        const ofs_metadata = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const len_metadata = std.mem.readInt(u64, data[off..][0..8], .little);
        off += 8;
        const name = try PrefixedStr.parse(data, &off);
        return .{ .ofs_metadata = ofs_metadata, .len_metadata = len_metadata, .name = name };
    }
};

pub const SummaryOffset = struct {
    group_opcode: Opcode,
    /// Byte offset of the group start from the beginning of the file.
    ofs_group: u64,
    len_group: u64,

    pub fn parse(data: []const u8) error{BufferTooSmall}!SummaryOffset {
        if (data.len < 1 + 8 + 8) return error.BufferTooSmall;
        return .{
            .group_opcode = @enumFromInt(data[0]),
            .ofs_group = std.mem.readInt(u64, data[1..][0..8], .little),
            .len_group = std.mem.readInt(u64, data[9..][0..8], .little),
        };
    }
};

pub const DataEnd = struct {
    /// CRC-32 of all bytes in the data section. 0 = not available.
    data_section_crc32: u32,

    pub fn parse(data: []const u8) error{BufferTooSmall}!DataEnd {
        if (data.len < 4) return error.BufferTooSmall;
        return .{ .data_section_crc32 = std.mem.readInt(u32, data[0..4], .little) };
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tagged union for record bodies
// ─────────────────────────────────────────────────────────────────────────────

pub const RecordBody = union(Opcode) {
    header: Header,
    footer: Footer,
    schema: Schema,
    channel: Channel,
    message: Message,
    chunk: Chunk,
    message_index: MessageIndex,
    chunk_index: ChunkIndex,
    attachment: Attachment,
    attachment_index: AttachmentIndex,
    statistics: Statistics,
    metadata: Metadata,
    metadata_index: MetadataIndex,
    summary_offset: SummaryOffset,
    data_end: DataEnd,

    pub fn deinit(self: RecordBody, allocator: Allocator) void {
        switch (self) {
            .channel => |c| c.deinit(allocator),
            .message_index => |m| m.deinit(allocator),
            .chunk_index => |c| c.deinit(allocator),
            .statistics => |s| s.deinit(allocator),
            .metadata => |m| m.deinit(allocator),
            else => {},
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Record (opcode + body length + body)
// ─────────────────────────────────────────────────────────────────────────────

pub const Record = struct {
    op: Opcode,
    /// Raw body bytes (slice into source buffer).
    raw_body: []const u8,

    /// Parse a record header only (op byte + 8-byte body length).
    /// After this call `offset` points to the first byte of the body.
    /// Advances `offset` past the entire record (header + body).
    pub fn parseHeader(data: []const u8, offset: *usize) error{BufferTooSmall}!Record {
        if (offset.* + 9 > data.len) return error.BufferTooSmall;
        const op: Opcode = @enumFromInt(data[offset.*]);
        offset.* += 1;
        const body_len = std.mem.readInt(u64, data[offset.*..][0..8], .little);
        offset.* += 8;
        if (offset.* + body_len > data.len) return error.BufferTooSmall;
        const raw_body = data[offset.* .. offset.* + body_len];
        offset.* += body_len;
        return .{ .op = op, .raw_body = raw_body };
    }

    /// Decode the body into a typed `RecordBody`. Allocator is required for
    /// records that contain variable-length collections.
    pub fn decodeBody(self: Record, allocator: Allocator) !RecordBody {
        return switch (self.op) {
            .header => .{ .header = try Header.parse(self.raw_body) },
            .footer => .{ .footer = try Footer.parse(self.raw_body) },
            .schema => .{ .schema = try Schema.parse(self.raw_body) },
            .channel => .{ .channel = try Channel.parse(allocator, self.raw_body) },
            .message => .{ .message = try Message.parse(self.raw_body) },
            .chunk => .{ .chunk = try Chunk.parse(self.raw_body) },
            .message_index => .{ .message_index = try MessageIndex.parse(allocator, self.raw_body) },
            .chunk_index => .{ .chunk_index = try ChunkIndex.parse(allocator, self.raw_body) },
            .attachment => .{ .attachment = try Attachment.parse(self.raw_body) },
            .attachment_index => .{ .attachment_index = try AttachmentIndex.parse(self.raw_body) },
            .statistics => .{ .statistics = try Statistics.parse(allocator, self.raw_body) },
            .metadata => .{ .metadata = try Metadata.parse(allocator, self.raw_body) },
            .metadata_index => .{ .metadata_index = try MetadataIndex.parse(self.raw_body) },
            .summary_offset => .{ .summary_offset = try SummaryOffset.parse(self.raw_body) },
            .data_end => .{ .data_end = try DataEnd.parse(self.raw_body) },
            _ => error.UnknownOpcode,
        };
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Top-level reader / iterator
// ─────────────────────────────────────────────────────────────────────────────

/// Validates both magic bytes and returns the offset of the first record.
pub fn validateMagic(data: []const u8) error{ InvalidMagic, BufferTooSmall }!void {
    if (data.len < MAGIC_LEN) return error.BufferTooSmall;
    if (!std.mem.eql(u8, data[0..MAGIC_LEN], MAGIC)) return error.InvalidMagic;
}

/// Lightweight iterator over raw records in an MCAP file.
/// Does not allocate. Decode individual bodies with `record.decodeBody(allocator)`.
///
/// Usage:
///   var iter = try McapIterator.init(file_data);
///   while (try iter.next()) |record| { ... }
pub const McapIterator = struct {
    data: []const u8,
    offset: usize,
    done: bool,

    pub fn init(data: []const u8) error{ InvalidMagic, BufferTooSmall }!McapIterator {
        try validateMagic(data);
        // Validate trailing magic lazily – just check there's enough data.
        if (data.len < MAGIC_LEN * 2 + 9) return error.BufferTooSmall;
        return .{ .data = data, .offset = MAGIC_LEN, .done = false };
    }

    /// Returns the next record, or null when the footer has been consumed.
    pub fn next(self: *McapIterator) error{BufferTooSmall}!?Record {
        if (self.done) return null;
        const record = try Record.parseHeader(self.data, &self.offset);
        if (record.op == .footer) self.done = true;
        return record;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// CRC-32 helper (ISO 3309 / ITU-T V.42 polynomial)
// ─────────────────────────────────────────────────────────────────────────────

pub fn crc32(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

// ─────────────────────────────────────────────────────────────────────────────
// Writer  (serialiser)
//
// The Writer turns typed structs back into the binary wire format so we can
// verify parse → write → parse produces identical data.
// ─────────────────────────────────────────────────────────────────────────────

pub const Writer = struct {
    buf: std.ArrayList(u8),

    pub fn init() Writer {
        const buf: std.ArrayList(u8) = .empty;
        return .{ .buf = buf };
    }

    pub fn deinit(self: *Writer, allocator: std.mem.Allocator) void {
        self.buf.deinit(allocator);
    }

    pub fn toOwnedSlice(self: *Writer) ![]u8 {
        return self.buf.toOwnedSlice();
    }

    // ── low-level helpers ────────────────────────────────────────────────────

    fn writeU8(self: *Writer, allocator: std.mem.Allocator, v: u8) !void {
        try self.buf.append(allocator, v);
    }

    pub fn writeU16(self: *Writer, allocator: std.mem.Allocator, v: u16) !void {
        var tmp: [2]u8 = undefined;
        std.mem.writeInt(u16, &tmp, v, .little);
        try self.buf.appendSlice(allocator, &tmp);
    }

    pub fn writeU32(self: *Writer, allocator: std.mem.Allocator, v: u32) !void {
        var tmp: [4]u8 = undefined;
        std.mem.writeInt(u32, &tmp, v, .little);
        try self.buf.appendSlice(allocator, &tmp);
    }

    pub fn writeU64(self: *Writer, allocator: std.mem.Allocator, v: u64) !void {
        var tmp: [8]u8 = undefined;
        std.mem.writeInt(u64, &tmp, v, .little);
        try self.buf.appendSlice(allocator, &tmp);
    }

    pub fn writePrefixedStr(self: *Writer, allocator: std.mem.Allocator, s: []const u8) !void {
        try self.writeU32(allocator, @intCast(s.len));
        try self.buf.appendSlice(allocator, s);
    }

    pub fn writeBytes(self: *Writer, allocator: std.mem.Allocator, b: []const u8) !void {
        try self.buf.appendSlice(allocator, b);
    }

    // Serialise a map of string→string: 4-byte byte-length prefix, then entries.
    fn writeMapStrStr(self: *Writer, allocator: std.mem.Allocator, entries: []const TupleStrStr) !void {
        // Calculate the byte length of the entries payload first.
        var byte_len: u32 = 0;
        for (entries) |e| {
            byte_len += 4 + @as(u32, @intCast(e.key.str.len));
            byte_len += 4 + @as(u32, @intCast(e.value.str.len));
        }
        try self.writeU32(allocator, byte_len);
        for (entries) |e| {
            try self.writePrefixedStr(allocator, e.key.str);
            try self.writePrefixedStr(allocator, e.value.str);
        }
    }

    // ── record-level helpers ─────────────────────────────────────────────────

    /// Write a complete record: opcode + 8-byte body-length + body bytes.
    fn writeRecord(self: *Writer, allocator: std.mem.Allocator, op: Opcode, body: []const u8) !void {
        try self.writeU8(allocator, @intFromEnum(op));
        try self.writeU64(allocator, @intCast(body.len));
        try self.writeBytes(allocator, body);
    }

    /// Build a body into a temporary buffer, then emit it as a record.
    fn writeRecordFromBody(self: *Writer, allocator: std.mem.Allocator, op: Opcode, body_writer: *Writer) !void {
        const body = try body_writer.buf.toOwnedSlice(allocator);
        defer allocator.free(body);
        try self.writeRecord(allocator, op, body);
    }

    // ── magic ────────────────────────────────────────────────────────────────

    pub fn writeMagic(self: *Writer, allocator: std.mem.Allocator) !void {
        try self.writeBytes(allocator, MAGIC);
    }

    // ── typed record writers ─────────────────────────────────────────────────
    pub fn writeHeader(self: *Writer, allocator: std.mem.Allocator, h: Header) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writePrefixedStr(allocator, h.profile.str);
        try b.writePrefixedStr(allocator, h.library.str);
        try self.writeRecordFromBody(allocator, .header, &b);
    }

    pub fn writeFooter(self: *Writer, allocator: std.mem.Allocator, f: Footer) !void {
        var body: [20]u8 = undefined;
        std.mem.writeInt(u64, body[0..8], f.ofs_summary_section, .little);
        std.mem.writeInt(u64, body[8..16], f.ofs_summary_offset_section, .little);
        std.mem.writeInt(u32, body[16..20], f.summary_crc32, .little);
        try self.writeRecord(allocator, .footer, &body);
    }

    pub fn writeSchema(self: *Writer, allocator: std.mem.Allocator, s: Schema) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU16(allocator, s.id);
        try b.writePrefixedStr(allocator, s.name.str);
        try b.writePrefixedStr(allocator, s.encoding.str);
        try b.writeU32(allocator, @intCast(s.data.len));
        try b.writeBytes(allocator, s.data);
        try self.writeRecordFromBody(allocator, .schema, &b);
    }

    pub fn writeChannel(self: *Writer, allocator: std.mem.Allocator, c: Channel) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU16(allocator, c.id);
        try b.writeU16(allocator, c.schema_id);
        try b.writePrefixedStr(allocator, c.topic.str);
        try b.writePrefixedStr(allocator, c.message_encoding.str);
        try b.writeMapStrStr(allocator, c.metadata.entries);
        try self.writeRecordFromBody(allocator, .channel, &b);
    }

    pub fn writeMessage(self: *Writer, allocator: std.mem.Allocator, m: Message) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU16(allocator, m.channel_id);
        try b.writeU32(allocator, m.sequence);
        try b.writeU64(allocator, m.log_time);
        try b.writeU64(allocator, m.publish_time);
        try b.writeBytes(allocator, m.data);
        try self.writeRecordFromBody(allocator, .message, &b);
    }

    pub fn writeChunk(self: *Writer, allocator: std.mem.Allocator, c: Chunk) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU64(allocator, c.message_start_time);
        try b.writeU64(allocator, c.message_end_time);
        try b.writeU64(allocator, c.uncompressed_size);
        try b.writeU32(allocator, c.uncompressed_crc32);
        try b.writePrefixedStr(allocator, c.compression.str);
        try b.writeU64(allocator, @intCast(c.records_data.len));
        try b.writeBytes(allocator, c.records_data);
        try self.writeRecordFromBody(allocator, .chunk, &b);
    }

    pub fn writeMessageIndex(self: *Writer, allocator: std.mem.Allocator, mi: MessageIndex) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU16(allocator, mi.channel_id);
        // entries payload: each entry is 8+8 bytes
        const byte_len: u32 = @intCast(mi.entries.len * 16);
        try b.writeU32(allocator, byte_len);
        for (mi.entries) |e| {
            try b.writeU64(allocator, e.log_time);
            try b.writeU64(allocator, e.offset);
        }
        try self.writeRecordFromBody(allocator, .message_index, &b);
    }

    pub fn writeChunkIndex(self: *Writer, allocator: std.mem.Allocator, ci: ChunkIndex) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU64(allocator, ci.message_start_time);
        try b.writeU64(allocator, ci.message_end_time);
        try b.writeU64(allocator, ci.ofs_chunk);
        try b.writeU64(allocator, ci.len_chunk);
        // message_index_offsets: 4-byte byte-length prefix, each entry is 2+8
        const mio_byte_len: u32 = @intCast(ci.message_index_offsets.len * 10);
        try b.writeU32(allocator, mio_byte_len);
        for (ci.message_index_offsets) |o| {
            try b.writeU16(allocator, o.channel_id);
            try b.writeU64(allocator, o.offset);
        }
        try b.writeU64(allocator, ci.message_index_length);
        try b.writePrefixedStr(allocator, ci.compression.str);
        try b.writeU64(allocator, ci.compressed_size);
        try b.writeU64(allocator, ci.uncompressed_size);
        try self.writeRecordFromBody(allocator, .chunk_index, &b);
    }

    pub fn writeAttachment(self: *Writer, allocator: std.mem.Allocator, a: Attachment) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU64(allocator, a.log_time);
        try b.writeU64(allocator, a.create_time);
        try b.writePrefixedStr(allocator, a.name.str);
        try b.writePrefixedStr(allocator, a.media_type.str);
        try b.writeU64(allocator, @intCast(a.data.len));
        try b.writeBytes(allocator, a.data);
        try b.writeU32(allocator, a.crc32);
        try self.writeRecordFromBody(allocator, .attachment, &b);
    }

    pub fn writeAttachmentIndex(self: *Writer, allocator: std.mem.Allocator, ai: AttachmentIndex) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU64(allocator, ai.ofs_attachment);
        try b.writeU64(allocator, ai.len_attachment);
        try b.writeU64(allocator, ai.log_time);
        try b.writeU64(allocator, ai.create_time);
        try b.writeU64(allocator, ai.data_size);
        try b.writePrefixedStr(allocator, ai.name.str);
        try b.writePrefixedStr(allocator, ai.media_type.str);
        try self.writeRecordFromBody(allocator, .attachment_index, &b);
    }

    pub fn writeStatistics(self: *Writer, allocator: std.mem.Allocator, s: Statistics) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU64(allocator, s.message_count);
        try b.writeU16(allocator, s.schema_count);
        try b.writeU32(allocator, s.channel_count);
        try b.writeU32(allocator, s.attachment_count);
        try b.writeU32(allocator, s.metadata_count);
        try b.writeU32(allocator, s.chunk_count);
        try b.writeU64(allocator, s.message_start_time);
        try b.writeU64(allocator, s.message_end_time);
        const cmc_byte_len: u32 = @intCast(s.channel_message_counts.len * 10);
        try b.writeU32(allocator, cmc_byte_len);
        for (s.channel_message_counts) |c| {
            try b.writeU16(allocator, c.channel_id);
            try b.writeU64(allocator, c.message_count);
        }
        try self.writeRecordFromBody(allocator, .statistics, &b);
    }

    pub fn writeMetadata(self: *Writer, allocator: std.mem.Allocator, m: Metadata) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writePrefixedStr(allocator, m.name.str);
        try b.writeMapStrStr(allocator, m.metadata.entries);
        try self.writeRecordFromBody(allocator, .metadata, &b);
    }

    pub fn writeMetadataIndex(self: *Writer, allocator: std.mem.Allocator, mi: MetadataIndex) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU64(allocator, mi.ofs_metadata);
        try b.writeU64(allocator, mi.len_metadata);
        try b.writePrefixedStr(allocator, mi.name.str);
        try self.writeRecordFromBody(allocator, .metadata_index, &b);
    }

    pub fn writeSummaryOffset(self: *Writer, allocator: std.mem.Allocator, so: SummaryOffset) !void {
        var b = Writer.init();
        defer b.deinit(allocator);
        try b.writeU8(allocator, @intFromEnum(so.group_opcode));
        try b.writeU64(allocator, so.ofs_group);
        try b.writeU64(allocator, so.len_group);
        try self.writeRecordFromBody(allocator, .summary_offset, &b);
    }

    pub fn writeDataEnd(self: *Writer, allocator: std.mem.Allocator, de: DataEnd) !void {
        var body: [4]u8 = undefined;
        std.mem.writeInt(u32, &body, de.data_section_crc32, .little);
        try self.writeRecord(allocator, .data_end, &body);
    }
};
