#include <mcap/writer.hpp>
#include <mcap/reader.hpp>
#include <cstdint>
#include <cstring>
#include <string>
#include <unordered_map>

struct McapHandle {
    mcap::McapWriter writer;
    std::unordered_map<std::string, mcap::SchemaId> schemas;
    uint32_t sequence = 0;
};

extern "C" {

void* mcap_writer_open(const char* path) {
    auto* h = new McapHandle();
    mcap::McapWriterOptions opts("ros2");
    auto status = h->writer.open(path, opts);
    if (!status.ok()) { delete h; return nullptr; }
    return h;
}

void mcap_writer_close(void* w) {
    auto* h = static_cast<McapHandle*>(w);
    h->writer.close();
    delete h;
}

uint16_t mcap_writer_add_channel(void* w, const char* topic,
    const char* message_type, const char* schema_encoding,
    const char* schema_data) {
    auto* h = static_cast<McapHandle*>(w);

    std::string type_key(message_type);
    mcap::SchemaId schema_id;
    auto it = h->schemas.find(type_key);
    if (it != h->schemas.end()) {
        schema_id = it->second;
    } else {
        mcap::Schema schema;
        schema.name = message_type;
        schema.encoding = schema_encoding;
        size_t data_len = strlen(schema_data);
        schema.data.assign(
            reinterpret_cast<const std::byte*>(schema_data),
            reinterpret_cast<const std::byte*>(schema_data) + data_len);
        h->writer.addSchema(schema);
        schema_id = schema.id;
        h->schemas[type_key] = schema_id;
    }

    mcap::Channel channel;
    channel.topic = topic;
    channel.messageEncoding = "cdr";
    channel.schemaId = schema_id;
    h->writer.addChannel(channel);
    return channel.id;
}

int mcap_writer_write(void* w, uint16_t channel_id,
    uint64_t log_time_ns, const void* data, size_t len) {
    auto* h = static_cast<McapHandle*>(w);
    mcap::Message msg;
    msg.channelId = channel_id;
    msg.sequence = h->sequence++;
    msg.logTime = log_time_ns;
    msg.publishTime = log_time_ns;
    msg.dataSize = len;
    msg.data = static_cast<const std::byte*>(data);
    auto status = h->writer.write(msg);
    return status.ok() ? 0 : -1;
}

void mcap_writer_write_metadata(void* w, const char* name,
    const char** keys, const char** values, size_t count) {
    auto* h = static_cast<McapHandle*>(w);
    mcap::Metadata meta;
    meta.name = name;
    for (size_t i = 0; i < count; i++) {
        meta.metadata.emplace(keys[i], values[i]);
    }
    (void)h->writer.write(meta);
}

int mcap_recover(const char* src, const char* dst) {
    mcap::McapReader reader;
    auto status = reader.open(src);
    if (!status.ok()) return -1;

    status = reader.readSummary(mcap::ReadSummaryMethod::AllowFallbackScan);
    // readSummary may fail on truncated files — that's OK, we'll still try to read messages

    // Open output writer
    McapHandle out;
    mcap::McapWriterOptions opts("ros2");
    auto wstatus = out.writer.open(dst, opts);
    if (!wstatus.ok()) { reader.close(); return -1; }

    // Copy schemas
    std::unordered_map<mcap::SchemaId, mcap::SchemaId> schema_map;
    for (const auto& [old_id, schema_ptr] : reader.schemas()) {
        mcap::Schema s;
        s.name = schema_ptr->name;
        s.encoding = schema_ptr->encoding;
        s.data = schema_ptr->data;
        out.writer.addSchema(s);
        schema_map[old_id] = s.id;
    }

    // Copy channels
    std::unordered_map<mcap::ChannelId, mcap::ChannelId> channel_map;
    for (const auto& [old_id, channel_ptr] : reader.channels()) {
        mcap::Channel ch;
        ch.topic = channel_ptr->topic;
        ch.messageEncoding = channel_ptr->messageEncoding;
        ch.schemaId = schema_map.count(channel_ptr->schemaId)
            ? schema_map[channel_ptr->schemaId] : 0;
        ch.metadata = channel_ptr->metadata;
        out.writer.addChannel(ch);
        channel_map[old_id] = ch.id;
    }

    // Copy messages
    int result = 0; // 0 = clean
    auto messages = reader.readMessages();
    for (auto it = messages.begin(); it != messages.end(); ++it) {
        const auto& view = *it;
        mcap::Message msg;
        auto ch_it = channel_map.find(view.message.channelId);
        if (ch_it == channel_map.end()) { result = 1; continue; }
        msg.channelId = ch_it->second;
        msg.sequence = view.message.sequence;
        msg.logTime = view.message.logTime;
        msg.publishTime = view.message.publishTime;
        msg.dataSize = view.message.dataSize;
        msg.data = view.message.data;
        auto ws = out.writer.write(msg);
        if (!ws.ok()) result = 1;
    }

    out.writer.close();
    reader.close();
    return result;
}

} // extern "C"
