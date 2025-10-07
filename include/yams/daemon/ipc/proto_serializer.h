#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <cstdint>
#include <vector>

namespace yams::daemon {

// ProtoSerializer encodes/decodes Message.payload (Request/Response) using protobuf Envelope.
// Transport header (request_id, chunking flags, CRC) remains handled by MessageFramer.
class ProtoSerializer {
public:
    // Encode just the payload of Message into protobuf Envelope bytes.
    static yams::Result<std::vector<uint8_t>> encode_payload(const Message& msg);

    // Append serialized payload bytes to the provided buffer. The new bytes are appended
    // starting at buffer.size(), allowing callers to reserve space (e.g., for frame headers)
    // and avoid intermediate allocations.
    static yams::Result<void> encode_payload_into(const Message& msg, std::vector<uint8_t>& buffer);

    // Decode protobuf Envelope bytes into Message with payload set; caller sets msg.requestId.
    static yams::Result<Message> decode_payload(const std::vector<uint8_t>& bytes);
};

} // namespace yams::daemon
