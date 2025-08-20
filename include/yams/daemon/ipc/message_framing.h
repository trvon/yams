#pragma once

#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <bit>
#include <concepts>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

namespace yams::daemon {

// C++20 concept for framed messages
template <typename T>
concept FramedMessage = requires(T msg) {
    { msg.serialize() } -> std::convertible_to<std::vector<uint8_t>>;
    { T::deserialize(std::span<const uint8_t>{}) } -> std::same_as<Result<T>>;
};

// Message framing for reliable transport
class MessageFramer {
public:
    static constexpr uint32_t MAGIC = 0x59414D53; // "YAMS" in hex
    static constexpr uint32_t VERSION = 1;
    static constexpr size_t HEADER_SIZE = 16; // 4 * sizeof(uint32_t)

    struct FrameHeader {
        uint32_t magic = MAGIC;
        uint32_t version = VERSION;
        uint32_t payload_size = 0;
        uint32_t checksum = 0; // CRC32 of payload

        // Convert to network byte order
        void to_network() noexcept {
            if constexpr (std::endian::native != std::endian::big) {
                magic = __builtin_bswap32(magic);
                version = __builtin_bswap32(version);
                payload_size = __builtin_bswap32(payload_size);
                checksum = __builtin_bswap32(checksum);
            }
        }

        // Convert from network byte order
        void from_network() noexcept {
            if constexpr (std::endian::native != std::endian::big) {
                magic = __builtin_bswap32(magic);
                version = __builtin_bswap32(version);
                payload_size = __builtin_bswap32(payload_size);
                checksum = __builtin_bswap32(checksum);
            }
        }

        // Validate header
        [[nodiscard]] bool is_valid() const noexcept {
            return magic == MAGIC && version == VERSION;
        }
    };

    // Frame a message with header and checksum
    template <FramedMessage T> [[nodiscard]] Result<std::vector<uint8_t>> frame(const T& message) {
        // Serialize message
        auto payload = message.serialize();

        // Create header
        FrameHeader header;
        header.payload_size = static_cast<uint32_t>(payload.size());
        header.checksum = calculate_crc32(payload);
        header.to_network();

        // Combine header and payload
        std::vector<uint8_t> frame;
        frame.reserve(HEADER_SIZE + payload.size());

        // Write header
        const auto* header_bytes = reinterpret_cast<const uint8_t*>(&header);
        frame.insert(frame.end(), header_bytes, header_bytes + sizeof(header));

        // Write payload
        frame.insert(frame.end(), payload.begin(), payload.end());

        return frame;
    }

    // Parse frame header
    [[nodiscard]] Result<FrameHeader> parse_header(std::span<const uint8_t> data) {
        if (data.size() < HEADER_SIZE) {
            return Error{ErrorCode::InvalidData, "Insufficient data for frame header"};
        }

        FrameHeader header;
        std::memcpy(&header, data.data(), sizeof(header));
        header.from_network();

        if (!header.is_valid()) {
            return Error{ErrorCode::InvalidData, "Invalid frame header"};
        }

        return header;
    }

    // Unframe a message
    template <FramedMessage T> [[nodiscard]] Result<T> unframe(std::span<const uint8_t> frame) {
        // Parse header
        auto header_result = parse_header(frame);
        if (!header_result) {
            return header_result.error();
        }

        const auto& header = header_result.value();

        // Validate frame size
        if (frame.size() < HEADER_SIZE + header.payload_size) {
            return Error{ErrorCode::InvalidData, "Incomplete frame"};
        }

        // Extract payload
        auto payload = frame.subspan(HEADER_SIZE, header.payload_size);

        // Verify checksum
        uint32_t calculated_checksum = calculate_crc32(payload);
        if (calculated_checksum != header.checksum) {
            return Error{ErrorCode::InvalidData, "Checksum mismatch"};
        }

        // Deserialize message
        return T::deserialize(payload);
    }

    // Check if we have a complete frame
    [[nodiscard]] Result<size_t> get_frame_size(std::span<const uint8_t> data) {
        auto header_result = parse_header(data);
        if (!header_result) {
            return header_result.error();
        }

        return HEADER_SIZE + header_result.value().payload_size;
    }

    // Legacy interface for compatibility
    static constexpr uint32_t FRAME_MAGIC = MAGIC;
    static constexpr uint32_t FRAME_VERSION = VERSION;

    explicit MessageFramer(size_t max_message_size = MAX_MESSAGE_SIZE)
        : max_message_size_(max_message_size) {}

    Result<std::vector<uint8_t>> frame_message(const Message& message);
    Result<Message> parse_frame(const std::vector<uint8_t>& frame);

private:
    size_t max_message_size_;

    Result<std::vector<uint8_t>> serialize_message(const Message& message);
    Result<Message> deserialize_message(const std::vector<uint8_t>& data);

    // CRC32 checksum calculation
    [[nodiscard]] static uint32_t calculate_crc32(std::span<const uint8_t> data) noexcept;
};

// Buffered frame reader for streaming protocols
class FrameReader {
public:
    explicit FrameReader(size_t max_frame_size = 16 * 1024 * 1024)
        : max_frame_size_(max_frame_size) {}

    // Add data to buffer
    void append(std::span<const uint8_t> data) {
        buffer_.insert(buffer_.end(), data.begin(), data.end());
    }

    // Try to extract a complete frame
    [[nodiscard]] Result<std::vector<uint8_t>> try_read_frame() {
        if (buffer_.size() < MessageFramer::HEADER_SIZE) {
            return Error{ErrorCode::InvalidData, "Insufficient data"};
        }

        MessageFramer framer;
        auto frame_size_result = framer.get_frame_size(buffer_);
        if (!frame_size_result) {
            return frame_size_result.error();
        }

        size_t frame_size = frame_size_result.value();
        if (frame_size > max_frame_size_) {
            return Error{ErrorCode::InvalidData, "Frame too large"};
        }

        if (buffer_.size() < frame_size) {
            return Error{ErrorCode::InvalidData, "Incomplete frame"};
        }

        // Extract frame
        std::vector<uint8_t> frame(buffer_.begin(), buffer_.begin() + frame_size);
        buffer_.erase(buffer_.begin(), buffer_.begin() + frame_size);

        return frame;
    }

    // Check if buffer has data
    [[nodiscard]] bool has_data() const noexcept { return !buffer_.empty(); }

    // Clear buffer
    void clear() noexcept { buffer_.clear(); }

    // Legacy interface for implementation
    void reset();

    enum class FrameStatus { NeedMoreData, FrameComplete, InvalidFrame, FrameTooLarge };

    struct FeedResult {
        size_t consumed;
        FrameStatus status;
    };

    FeedResult feed(const uint8_t* data, size_t size);
    bool has_frame() const;
    Result<std::vector<uint8_t>> get_frame();

private:
    std::vector<uint8_t> buffer_;
    size_t max_frame_size_;

    enum class State { WaitingForHeader, ReadingBody, FrameReady };

    State state_ = State::WaitingForHeader;
    size_t expected_size_ = sizeof(MessageFramer::FrameHeader);
};

} // namespace yams::daemon