#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/message_serializer.h>

#include <spdlog/spdlog.h>
#include <cstring>
#include <zlib.h> // For CRC32
#include <arpa/inet.h>

namespace yams::daemon {

// ============================================================================
// CRC32 Calculation
// ============================================================================

uint32_t calculate_crc32(const uint8_t* data, size_t size) {
    return crc32(0, data, size);
}

// ============================================================================
// MessageFramer Implementation
// ============================================================================

Result<std::vector<uint8_t>> MessageFramer::frame_message(const Message& message) {
    // Serialize the message first
    auto serialized = serialize_message(message);
    if (!serialized) {
        return serialized.error();
    }

    auto& data = serialized.value();

    // Check size limits
    if (data.size() > max_message_size_) {
        return Error{ErrorCode::InvalidData, "Message size " + std::to_string(data.size()) +
                                                 " exceeds maximum " +
                                                 std::to_string(max_message_size_)};
    }

    // Create frame header
    FrameHeader header;
    header.magic = FRAME_MAGIC;
    header.version = FRAME_VERSION;
    header.payload_size = static_cast<uint32_t>(data.size());
    header.checksum = ::yams::daemon::calculate_crc32(data.data(), data.size());

    // Build complete frame
    std::vector<uint8_t> frame;
    frame.reserve(sizeof(FrameHeader) + data.size());

    // Convert header to network byte order and append
    FrameHeader network_header;
    network_header.magic = htonl(header.magic);
    network_header.version = htonl(header.version);
    network_header.payload_size = htonl(header.payload_size);
    network_header.checksum = htonl(header.checksum);

    const uint8_t* header_bytes = reinterpret_cast<const uint8_t*>(&network_header);
    frame.insert(frame.end(), header_bytes, header_bytes + sizeof(FrameHeader));

    // Append message data
    frame.insert(frame.end(), data.begin(), data.end());

    return frame;
}

Result<Message> MessageFramer::parse_frame(const std::vector<uint8_t>& frame) {
    if (frame.size() < sizeof(FrameHeader)) {
        return Error{ErrorCode::InvalidData, "Frame too small for header"};
    }

    // Extract and convert header from network byte order
    FrameHeader network_header;
    std::memcpy(&network_header, frame.data(), sizeof(FrameHeader));

    FrameHeader header;
    header.magic = ntohl(network_header.magic);
    header.version = ntohl(network_header.version);
    header.payload_size = ntohl(network_header.payload_size);
    header.checksum = ntohl(network_header.checksum);

    // Validate magic number
    if (header.magic != FRAME_MAGIC) {
        return Error{ErrorCode::InvalidData,
                     "Invalid frame magic: " + std::to_string(header.magic)};
    }

    // Validate version
    if (header.version != FRAME_VERSION) {
        return Error{ErrorCode::InvalidData,
                     "Unsupported frame version: " + std::to_string(header.version)};
    }

    // Validate size
    if (header.payload_size > max_message_size_) {
        return Error{ErrorCode::InvalidData, "Message size exceeds maximum"};
    }

    if (frame.size() != sizeof(FrameHeader) + header.payload_size) {
        return Error{ErrorCode::InvalidData, "Frame size mismatch"};
    }

    // Extract message data
    std::vector<uint8_t> message_data(frame.begin() + sizeof(FrameHeader), frame.end());

    // Verify checksum
    uint32_t calculated_crc =
        ::yams::daemon::calculate_crc32(message_data.data(), message_data.size());
    if (calculated_crc != header.checksum) {
        return Error{ErrorCode::InvalidData, "Checksum mismatch: expected " +
                                                 std::to_string(header.checksum) + ", got " +
                                                 std::to_string(calculated_crc)};
    }

    // Deserialize message
    return deserialize_message(message_data);
}

Result<std::vector<uint8_t>> MessageFramer::serialize_message(const Message& message) {
    try {
        MessageSerializer serializer;
        return serializer.serialize(message);
    } catch (const std::exception& e) {
        return Error{ErrorCode::SerializationError,
                     std::string("Failed to serialize message: ") + e.what()};
    }
}

Result<Message> MessageFramer::deserialize_message(const std::vector<uint8_t>& data) {
    try {
        MessageSerializer serializer;
        return serializer.deserialize(data);
    } catch (const std::exception& e) {
        return Error{ErrorCode::SerializationError,
                     std::string("Failed to deserialize message: ") + e.what()};
    }
}

// ============================================================================
// FrameReader Implementation
// ============================================================================

void FrameReader::reset() {
    buffer_.clear();
    state_ = State::WaitingForHeader;
    expected_size_ = sizeof(MessageFramer::FrameHeader);
}

FrameReader::FeedResult FrameReader::feed(const uint8_t* data, size_t size) {
    size_t consumed = 0;

    while (consumed < size && state_ != State::FrameReady) {
        size_t to_copy = std::min(size - consumed, expected_size_ - buffer_.size());
        buffer_.insert(buffer_.end(), data + consumed, data + consumed + to_copy);
        consumed += to_copy;

        if (buffer_.size() == expected_size_) {
            if (state_ == State::WaitingForHeader) {
                // Parse header
                MessageFramer::FrameHeader network_header;
                std::memcpy(&network_header, buffer_.data(), sizeof(MessageFramer::FrameHeader));

                MessageFramer::FrameHeader header;
                header.magic = ntohl(network_header.magic);
                header.version = ntohl(network_header.version);
                header.payload_size = ntohl(network_header.payload_size);
                header.checksum = ntohl(network_header.checksum);

                // Validate header
                if (header.magic != MessageFramer::FRAME_MAGIC) {
                    reset();
                    return {consumed, FrameStatus::InvalidFrame};
                }

                if (header.version != MessageFramer::FRAME_VERSION) {
                    reset();
                    return {consumed, FrameStatus::InvalidFrame};
                }

                if (header.payload_size > max_frame_size_ - sizeof(MessageFramer::FrameHeader)) {
                    reset();
                    return {consumed, FrameStatus::FrameTooLarge};
                }

                // Transition to reading body
                expected_size_ = sizeof(MessageFramer::FrameHeader) + header.payload_size;
                state_ = State::ReadingBody;

                // Reserve space for complete frame
                buffer_.reserve(expected_size_);
            } else if (state_ == State::ReadingBody) {
                // Frame complete
                state_ = State::FrameReady;
                return {consumed, FrameStatus::FrameComplete};
            }
        }
    }

    if (state_ == State::FrameReady) {
        return {consumed, FrameStatus::FrameComplete};
    }

    return {consumed, FrameStatus::NeedMoreData};
}

bool FrameReader::has_frame() const {
    return state_ == State::FrameReady;
}

Result<std::vector<uint8_t>> FrameReader::get_frame() {
    if (state_ != State::FrameReady) {
        return Error{ErrorCode::InvalidState, "No complete frame available"};
    }

    auto frame = std::move(buffer_);
    reset();
    return frame;
}

// Note: FrameWriter removed as it's not defined in the header

// Static method implementation
uint32_t MessageFramer::calculate_crc32(std::span<const uint8_t> data) noexcept {
    return ::yams::daemon::calculate_crc32(data.data(), data.size());
}

} // namespace yams::daemon
