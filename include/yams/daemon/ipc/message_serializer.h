#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

// Forward declarations
class BinarySerializer;
class BinaryDeserializer;

class MessageSerializer {
public:
    MessageSerializer();
    ~MessageSerializer();

    // Methods expected by message_framing.cpp
    std::vector<uint8_t> serialize(const Message& msg);
    Result<Message> deserialize(const std::vector<uint8_t>& data);

    // Serialize message using operator<<
    MessageSerializer& operator<<(const Message& msg);

    // Get serialized data
    std::vector<uint8_t> getData() const;

    // Static methods for compatibility (with different return type)
    static std::vector<std::byte> serialize_bytes(const Message& msg);
    static Result<Message> deserialize(std::span<const std::byte> data);
    static Result<size_t> getMessageSize(std::span<const std::byte> header);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

class MessageDeserializer {
public:
    explicit MessageDeserializer(const std::vector<uint8_t>& data);
    ~MessageDeserializer();

    // Deserialize message using operator>>
    MessageDeserializer& operator>>(Message& msg);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::daemon