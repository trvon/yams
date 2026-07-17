/**
 * @file message_framing.h
 * @brief IPC message framing utilities for YAMS daemon.
 *
 * Provides validated frame header definitions, CRC32 checksum calculation,
 * and protobuf message framing.
 *
 */
#pragma once

#include <yams/common/expected_compat.h>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <bit>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <span>
#include <type_traits>
#include <vector>

#ifdef _WIN32
#define bswap32 _byteswap_ulong
#else
#define bswap32 __builtin_bswap32
#endif

namespace yams::daemon {

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
                magic = bswap32(magic);
                version = bswap32(version);
                payload_size = bswap32(payload_size);
                checksum = bswap32(checksum);
                flags = bswap32(flags);
            }
        }

        // Convert from network byte order
        void from_network() noexcept {
            if constexpr (std::endian::native != std::endian::big) {
                magic = bswap32(magic);
                version = bswap32(version);
                payload_size = bswap32(payload_size);
                checksum = bswap32(checksum);
                flags = bswap32(flags);
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

    explicit MessageFramer(size_t max_message_size = 16 * 1024 * 1024)
        : max_message_size_(max_message_size) {}

    Result<std::vector<uint8_t>> frame_message(const Message& message) {
        std::vector<uint8_t> frame;
        auto result = frame_message_into(message, frame);
        if (!result) {
            return result.error();
        }
        return frame;
    }

    Result<std::vector<uint8_t>> frame_message_chunk(const Message& message,
                                                     bool last_chunk = false) {
        std::vector<uint8_t> frame;
        auto result = frame_message_chunk_into(message, frame, last_chunk);
        if (!result) {
            return result.error();
        }
        return frame;
    }

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

    void reset();

    enum class FrameStatus {
        NeedMoreData,
        FrameComplete,
        InvalidFrame,
        FrameTooLarge,
        ChunkedFrame,   // Valid chunk in a chunked response
        ChunkedComplete // Final chunk in a chunked response
    };

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

private:
    std::vector<uint8_t> buffer_;
    size_t max_frame_size_;

    enum class State { WaitingForHeader, ReadingBody, FrameReady };

    State state_ = State::WaitingForHeader;
    size_t expected_size_ = sizeof(MessageFramer::FrameHeader);

    MessageFramer::FrameHeader current_header_{};
};

} // namespace yams::daemon
