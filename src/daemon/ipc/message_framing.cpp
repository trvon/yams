#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/proto_serializer.h>

#include <spdlog/spdlog.h>
#include <cstring>
#include <zlib.h> // For CRC32
#include <arpa/inet.h>

namespace yams::daemon {

// ============================================================================
// CRC32 Calculation
// ============================================================================

namespace {
uint32_t calculate_crc32_impl(const uint8_t* data, size_t size) {
    return crc32(0, data, size);
}
} // namespace

uint32_t MessageFramer::calculate_crc32(std::span<const uint8_t> data) noexcept {
    return calculate_crc32_impl(data.data(), data.size());
}

// ============================================================================
// MessageFramer Implementation
// ============================================================================

namespace {

Result<void> append_frame_bytes(const Message& message, std::vector<uint8_t>& buffer,
                                std::size_t max_message_size, bool chunked, bool last_chunk,
                                bool header_only) {
    const auto base = buffer.size();
    buffer.resize(base + sizeof(MessageFramer::FrameHeader));
    const auto payload_offset = buffer.size();

    auto payload_result = ProtoSerializer::encode_payload_into(message, buffer);
    if (!payload_result) {
        buffer.resize(base);
        return payload_result.error();
    }

    const std::size_t payload_size = buffer.size() - payload_offset;
    if (payload_size > max_message_size) {
        buffer.resize(base);
        return Error{ErrorCode::InvalidData, "Message size " + std::to_string(payload_size) +
                                                 " exceeds maximum " +
                                                 std::to_string(max_message_size)};
    }

    MessageFramer::FrameHeader header;
    header.payload_size = static_cast<uint32_t>(payload_size);
    header.checksum = calculate_crc32_impl(buffer.data() + payload_offset, payload_size);
    header.set_chunked(chunked);
    header.set_last_chunk(last_chunk);
    header.set_header_only(header_only);
    header.to_network();

    std::memcpy(buffer.data() + base, &header, sizeof(header));

    return Result<void>();
}

} // namespace

Result<void> MessageFramer::frame_message_into(const Message& message,
                                               std::vector<uint8_t>& buffer) {
    return append_frame_bytes(message, buffer, max_message_size_, /*chunked=*/false,
                              /*last_chunk=*/false, /*header_only=*/false);
}

Result<void> MessageFramer::frame_message_header_into(const Message& message,
                                                      std::vector<uint8_t>& buffer,
                                                      uint64_t /*total_size*/) {
    // Header frames are marked chunked + header_only so readers treat payload as metadata prelude.
    return append_frame_bytes(message, buffer, max_message_size_, /*chunked=*/true,
                              /*last_chunk=*/false, /*header_only=*/true);
}

Result<void> MessageFramer::frame_message_chunk_into(const Message& message,
                                                     std::vector<uint8_t>& buffer,
                                                     bool last_chunk) {
    return append_frame_bytes(message, buffer, max_message_size_, /*chunked=*/true, last_chunk,
                              /*header_only=*/false);
}

Result<std::vector<uint8_t>> MessageFramer::frame_message(const Message& message) {
    std::vector<uint8_t> frame;
    auto res = frame_message_into(message, frame);
    if (!res)
        return res.error();
    return frame;
}

Result<std::vector<uint8_t>> MessageFramer::frame_message_header(const Message& message,
                                                                 uint64_t /*total_size*/) {
    std::vector<uint8_t> frame;
    auto res = frame_message_header_into(message, frame);
    if (!res)
        return res.error();
    return frame;
}

Result<std::vector<uint8_t>> MessageFramer::frame_message_chunk(const Message& message,
                                                                bool last_chunk) {
    std::vector<uint8_t> frame;
    auto res = frame_message_chunk_into(message, frame, last_chunk);
    if (!res)
        return res.error();
    return frame;
}

Result<MessageFramer::ChunkedMessageInfo>
MessageFramer::get_frame_info(std::span<const uint8_t> data) {
    if (data.size() < sizeof(FrameHeader)) {
        return Error{ErrorCode::InvalidData, "Insufficient data for frame header"};
    }

    FrameHeader header;
    std::memcpy(&header, data.data(), sizeof(header));
    header.from_network();

    if (!header.is_valid()) {
        return Error{ErrorCode::InvalidData, "Invalid frame header"};
    }

    ChunkedMessageInfo info;
    info.is_chunked = header.is_chunked();
    info.is_last_chunk = header.is_last_chunk();
    info.is_header_only = header.is_header_only();
    info.is_error = header.is_error();
    info.payload_size = header.payload_size;

    return info;
}

Result<Message> MessageFramer::parse_frame(const std::vector<uint8_t>& frame) {
    if (frame.size() < sizeof(FrameHeader)) {
        return Error{ErrorCode::InvalidData, "Frame too small for header"};
    }

    // Extract and convert header from network byte order
    FrameHeader header;
    std::memcpy(&header, frame.data(), sizeof(FrameHeader));
    header.from_network();

    // Validate magic number
    if (!header.is_valid()) {
        return Error{ErrorCode::InvalidData, "Invalid frame magic or version"};
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
    uint32_t calculated_crc = calculate_crc32(message_data);
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
        return ProtoSerializer::encode_payload(message);
    } catch (const std::exception& e) {
        return Error{ErrorCode::SerializationError,
                     std::string("Failed to serialize message: ") + e.what()};
    }
}

Result<Message> MessageFramer::deserialize_message(const std::vector<uint8_t>& data) {
    try {
        return ProtoSerializer::decode_payload(data);
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
    processing_chunked_ = false;
    received_last_chunk_ = false;
    chunks_received_ = 0;
    total_bytes_received_ = 0;
    expected_total_bytes_ = 0;
}

FrameReader::FrameInfo FrameReader::get_frame_info() const {
    FrameInfo info;
    info.status =
        state_ == State::FrameReady ? FrameStatus::FrameComplete : FrameStatus::NeedMoreData;

    if (buffer_.size() >= sizeof(MessageFramer::FrameHeader)) {
        MessageFramer::FrameHeader header;
        std::memcpy(&header, buffer_.data(), sizeof(header));
        header.from_network();

        if (header.is_valid()) {
            info.is_chunked = header.is_chunked();
            info.is_last_chunk = header.is_last_chunk();
            info.is_header_only = header.is_header_only();
            info.is_error = header.is_error();
            info.payload_size = header.payload_size;

            if (info.is_chunked) {
                info.status =
                    info.is_last_chunk ? FrameStatus::ChunkedComplete : FrameStatus::ChunkedFrame;
            }
        }
    }

    return info;
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
                MessageFramer::FrameHeader header;
                std::memcpy(&header, buffer_.data(), sizeof(MessageFramer::FrameHeader));
                header.from_network();

                // Store current header for later use
                current_header_ = header;

                // Validate header
                if (!header.is_valid()) {
                    reset();
                    return {consumed, FrameStatus::InvalidFrame};
                }

                if (header.payload_size > max_frame_size_ - sizeof(MessageFramer::FrameHeader)) {
                    reset();
                    return {consumed, FrameStatus::FrameTooLarge};
                }

                // Check if this is a header-only frame
                if (header.is_header_only()) {
                    // Tolerate legacy frames that erroneously carried a non-zero payload_size.
                    if (header.payload_size != 0) {
                        spdlog::warn(
                            "FrameReader: header-only frame with non-zero payload_size={} — "
                            "treating as zero for compatibility",
                            header.payload_size);
                    }
                    // Header-only frame is already complete
                    state_ = State::FrameReady;
                    processing_chunked_ = header.is_chunked();
                    return {consumed,
                            FrameStatus::FrameComplete,
                            header.is_chunked(),
                            header.is_last_chunk(),
                            header.is_header_only(),
                            header.is_error()};
                }

                // Transition to reading body
                expected_size_ = sizeof(MessageFramer::FrameHeader) + header.payload_size;
                state_ = State::ReadingBody;

                // Reserve space for complete frame
                buffer_.reserve(expected_size_);
            } else if (state_ == State::ReadingBody) {
                // Frame complete
                state_ = State::FrameReady;

                // Check if this is a chunked frame
                if (current_header_.is_chunked()) {
                    processing_chunked_ = true;
                    chunks_received_++;
                    total_bytes_received_ += current_header_.payload_size;

                    if (current_header_.is_last_chunk()) {
                        received_last_chunk_ = true;
                        return {consumed, FrameStatus::ChunkedComplete, true, true,
                                false,    current_header_.is_error()};
                    }

                    return {consumed, FrameStatus::ChunkedFrame, true, false,
                            false,    current_header_.is_error()};
                }

                return {consumed, FrameStatus::FrameComplete, false, false,
                        false,    current_header_.is_error()};
            }
        }
    }

    if (state_ == State::FrameReady) {
        if (current_header_.is_chunked()) {
            if (current_header_.is_last_chunk()) {
                return {consumed, FrameStatus::ChunkedComplete,     true,
                        true,     current_header_.is_header_only(), current_header_.is_error()};
            }
            return {consumed, FrameStatus::ChunkedFrame,        true,
                    false,    current_header_.is_header_only(), current_header_.is_error()};
        }
        return {consumed, FrameStatus::FrameComplete,       false,
                false,    current_header_.is_header_only(), current_header_.is_error()};
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

    // Safely transfer ownership of the buffered frame without leaving the
    // internal vector in an ambiguous moved-from state prior to reset().
    // Swap ensures buffer_ becomes empty immediately, preventing any chance of
    // double-free with certain standard library behaviors when combined with
    // subsequent reset()/clear operations and later reallocations.
    std::vector<uint8_t> frame;
    frame.swap(buffer_);
    reset();
    return frame;
}

} // namespace yams::daemon
