#pragma once

#include <yams/common/expected_compat.h>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <bit>
#include <concepts>
#include <cstdint>
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
    static constexpr size_t HEADER_SIZE = 20; // 5 * sizeof(uint32_t)

    struct FrameHeader {
        uint32_t magic = MAGIC;
        uint32_t version = VERSION;
        uint32_t payload_size = 0;
        uint32_t checksum = 0; // CRC32 of payload
        uint32_t flags = 0;    // Bit flags for frame properties

        // Flag bits
        static constexpr uint32_t FLAG_CHUNKED = 0x00000001;     // Chunked transfer encoding
        static constexpr uint32_t FLAG_LAST_CHUNK = 0x00000002;  // Last chunk in sequence
        static constexpr uint32_t FLAG_ERROR = 0x00000004;       // Error response
        static constexpr uint32_t FLAG_HEADER_ONLY = 0x00000008; // Header-only frame (metadata)

        // Helper methods for flag operations
        void set_chunked(bool is_chunked = true) noexcept {
            if (is_chunked)
                flags |= FLAG_CHUNKED;
            else
                flags &= ~FLAG_CHUNKED;
        }

        void set_last_chunk(bool is_last = true) noexcept {
            if (is_last)
                flags |= FLAG_LAST_CHUNK;
            else
                flags &= ~FLAG_LAST_CHUNK;
        }

        void set_error(bool is_error = true) noexcept {
            if (is_error)
                flags |= FLAG_ERROR;
            else
                flags &= ~FLAG_ERROR;
        }

        void set_header_only(bool is_header_only = true) noexcept {
            if (is_header_only)
                flags |= FLAG_HEADER_ONLY;
            else
                flags &= ~FLAG_HEADER_ONLY;
        }

        bool is_chunked() const noexcept { return (flags & FLAG_CHUNKED) != 0; }
        bool is_last_chunk() const noexcept { return (flags & FLAG_LAST_CHUNK) != 0; }
        bool is_error() const noexcept { return (flags & FLAG_ERROR) != 0; }
        bool is_header_only() const noexcept { return (flags & FLAG_HEADER_ONLY) != 0; }

        // Convert to network byte order
        void to_network() noexcept {
            if constexpr (std::endian::native != std::endian::big) {
                magic = __builtin_bswap32(magic);
                version = __builtin_bswap32(version);
                payload_size = __builtin_bswap32(payload_size);
                checksum = __builtin_bswap32(checksum);
                flags = __builtin_bswap32(flags);
            }
        }

        // Convert from network byte order
        void from_network() noexcept {
            if constexpr (std::endian::native != std::endian::big) {
                magic = __builtin_bswap32(magic);
                version = __builtin_bswap32(version);
                payload_size = __builtin_bswap32(payload_size);
                checksum = __builtin_bswap32(checksum);
                flags = __builtin_bswap32(flags);
            }
        }

        // Validate header
        [[nodiscard]] bool is_valid() const noexcept {
            return magic == MAGIC && version == VERSION;
        }
    };

    // Compile-time validation (PBI-058 Task 058-15)
    static_assert(HEADER_SIZE % 4 == 0, "Frame header must be 4-byte aligned for uint32_t fields");
    static_assert(HEADER_SIZE == sizeof(FrameHeader),
                  "HEADER_SIZE constant must match actual struct size");
    static_assert(std::is_trivially_copyable_v<FrameHeader>,
                  "FrameHeader must be trivially copyable for memcpy");
    static_assert(std::is_standard_layout_v<FrameHeader>,
                  "FrameHeader must have standard layout for binary serialization");

    // Frame a message with header and checksum
    template <FramedMessage T>
    [[nodiscard]] Result<std::vector<uint8_t>> frame(const T& message, bool chunked = false,
                                                     bool last_chunk = false,
                                                     bool header_only = false) {
        // Serialize message
        auto payload = message.serialize();

        // Create header
        FrameHeader header;
        header.payload_size = static_cast<uint32_t>(payload.size());
        header.checksum = calculate_crc32(payload);
        header.set_chunked(chunked);
        header.set_last_chunk(last_chunk);
        header.set_header_only(header_only);
        header.to_network();

        // Combine header and payload
        std::vector<uint8_t> frame;
        frame.reserve(HEADER_SIZE + payload.size());

        // Write header
        const auto* header_bytes = reinterpret_cast<const uint8_t*>(&header);
        frame.insert(frame.end(), header_bytes, header_bytes + sizeof(header));

        // Write payload if not header-only frame
        if (!header_only) {
            frame.insert(frame.end(), payload.begin(), payload.end());
        }

        return frame;
    }

    // Create a header-only frame (for streaming responses)
    template <FramedMessage T>
    [[nodiscard]] Result<std::vector<uint8_t>>
    frame_header(const T& message, uint64_t total_size = 0, bool chunked = true) {
        (void)total_size;
        return frame(message, chunked, false, true);
    }

    // Create a chunk frame (for streaming responses)
    template <FramedMessage T>
    [[nodiscard]] Result<std::vector<uint8_t>> frame_chunk(const T& message,
                                                           bool last_chunk = false) {
        return frame(message, true, last_chunk, false);
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

    explicit MessageFramer(size_t max_message_size = 16 * 1024 * 1024)
        : max_message_size_(max_message_size) {}

    Result<std::vector<uint8_t>> frame_message(const Message& message);
    Result<std::vector<uint8_t>> frame_message_header(const Message& message,
                                                      uint64_t total_size = 0);
    Result<std::vector<uint8_t>> frame_message_chunk(const Message& message,
                                                     bool last_chunk = false);

    // Append framed bytes into the provided buffer, preserving existing contents. Callers may
    // reset or reuse the buffer between invocations to avoid reallocations.
    Result<void> frame_message_into(const Message& message, std::vector<uint8_t>& buffer);
    Result<void> frame_message_header_into(const Message& message, std::vector<uint8_t>& buffer,
                                           uint64_t total_size = 0);
    Result<void> frame_message_chunk_into(const Message& message, std::vector<uint8_t>& buffer,
                                          bool last_chunk = false);
    Result<Message> parse_frame(std::span<const uint8_t> frame);

    // Stream-oriented message framing
    struct ChunkedMessageInfo {
        bool is_chunked = false;
        bool is_last_chunk = false;
        bool is_header_only = false;
        bool is_error = false;
        uint32_t payload_size = 0;
    };

    Result<ChunkedMessageInfo> get_frame_info(std::span<const uint8_t> data);

private:
    size_t max_message_size_;

    Result<std::vector<uint8_t>> serialize_message(const Message& message);
    Result<Message> deserialize_message(std::span<const uint8_t> data);

    // CRC32 checksum calculation
    [[nodiscard]] static uint32_t calculate_crc32(std::span<const uint8_t> data) noexcept;
};

// Buffered frame reader for streaming protocols
class FrameReader {
public:
    explicit FrameReader(size_t max_frame_size = 16 * 1024 * 1024)
        : max_frame_size_(max_frame_size) {}

    // Non-copyable and non-movable to avoid accidental duplication across coroutine frames
    FrameReader(const FrameReader&) = delete;
    FrameReader& operator=(const FrameReader&) = delete;
    FrameReader(FrameReader&&) = delete;
    FrameReader& operator=(FrameReader&&) = delete;

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

    enum class FrameStatus {
        NeedMoreData,
        FrameComplete,
        InvalidFrame,
        FrameTooLarge,
        ChunkedFrame,   // Valid chunk in a chunked response
        ChunkedComplete // Final chunk in a chunked response
    };

    // Information about the current frame being processed
    struct FrameInfo {
        FrameStatus status = FrameStatus::NeedMoreData;
        bool is_chunked = false;
        bool is_last_chunk = false;
        bool is_header_only = false;
        bool is_error = false;
        uint32_t payload_size = 0;
    };

    // Get information about the current frame
    [[nodiscard]] FrameInfo get_frame_info() const;

    struct FeedResult {
        size_t consumed;
        FrameStatus status;
        bool is_chunked = false;
        bool is_last_chunk = false;
        bool is_header_only = false;
        bool is_error = false;
    };

    FeedResult feed(const uint8_t* data, size_t size);
    bool has_frame() const;
    Result<std::vector<uint8_t>> get_frame();

    // Handle chunked streaming responses
    bool is_processing_chunked() const { return processing_chunked_; }
    uint64_t get_chunks_received() const { return chunks_received_; }

private:
    std::vector<uint8_t> buffer_;
    size_t max_frame_size_;

    enum class State { WaitingForHeader, ReadingBody, FrameReady };

    State state_ = State::WaitingForHeader;
    size_t expected_size_ = sizeof(MessageFramer::FrameHeader);

    // Chunked streaming state
    bool processing_chunked_ = false;
    bool received_last_chunk_ = false;
    uint64_t chunks_received_ = 0;
    uint64_t total_bytes_received_ = 0;
    uint64_t expected_total_bytes_ = 0;
    MessageFramer::FrameHeader current_header_{};
};

} // namespace yams::daemon
