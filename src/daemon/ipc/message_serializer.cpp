// Protobuf-backed shim replacing legacy binary serializer
#include <spdlog/spdlog.h>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_serializer.h>
#include <yams/daemon/ipc/proto_serializer.h>

#include <cstddef>
#include <cstring>
#include <span>
#include <stdexcept>
#include <vector>

namespace yams::daemon {

// Static helpers (legacy API retained)
std::vector<std::byte> MessageSerializer::serialize_bytes(const Message& msg) {
    auto r = ProtoSerializer::encode_payload(msg);
    if (!r) {
        throw std::runtime_error("Proto encode failed: " + r.error().message);
    }
    const auto& bytes = r.value();
    std::vector<std::byte> out(bytes.size());
    std::memcpy(out.data(), bytes.data(), bytes.size());
    return out;
}

Result<Message> MessageSerializer::deserialize(std::span<const std::byte> data) {
    std::vector<uint8_t> bytes(data.size());
    std::memcpy(bytes.data(), data.data(), data.size());
    return ProtoSerializer::decode_payload(bytes);
}

Result<size_t> MessageSerializer::getMessageSize(std::span<const std::byte> /*header*/) {
    // Not applicable in protobuf payload-only path; transport header provides sizes.
    return Error{ErrorCode::InvalidArgument, "getMessageSize not supported"};
}

// Minimal PIMPL to preserve existing construction/stream APIs
class MessageSerializer::Impl {
public:
    std::vector<uint8_t> data;
};

MessageSerializer::MessageSerializer() : pImpl(std::make_unique<Impl>()) {}
MessageSerializer::~MessageSerializer() = default;

std::vector<uint8_t> MessageSerializer::serialize(const Message& msg) {
    auto r = ProtoSerializer::encode_payload(msg);
    if (!r) {
        throw std::runtime_error("Proto encode failed: " + r.error().message);
    }
    return r.value();
}

Result<Message> MessageSerializer::deserialize(const std::vector<uint8_t>& data) {
    return ProtoSerializer::decode_payload(data);
}

MessageSerializer& MessageSerializer::operator<<(const Message& msg) {
    auto r = ProtoSerializer::encode_payload(msg);
    if (!r) {
        throw std::runtime_error("Proto encode failed: " + r.error().message);
    }
    pImpl->data = std::move(r.value());
    return *this;
}

std::vector<uint8_t> MessageSerializer::getData() const {
    return pImpl->data;
}

// MessageDeserializer maintained for API compatibility; no-op stream wrapper
class MessageDeserializer::Impl {
public:
    std::vector<uint8_t> data;
    explicit Impl(const std::vector<uint8_t>& input) : data(input) {}
};

MessageDeserializer::MessageDeserializer(const std::vector<uint8_t>& data)
    : pImpl(std::make_unique<Impl>(data)) {}
MessageDeserializer::~MessageDeserializer() = default;

MessageDeserializer& MessageDeserializer::operator>>(Message& msg) {
    auto r = ProtoSerializer::decode_payload(pImpl->data);
    if (!r) {
        throw std::runtime_error("Proto decode failed: " + r.error().message);
    }
    msg = std::move(r.value());
    return *this;
}

} // namespace yams::daemon
