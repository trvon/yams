// SPDX-License-Identifier: GPL-3.0-or-later
// FrameReader unit tests — guards FrameReader::feed(), get_frame(), buffer management
// Added as part of IPC optimization work (memcpy + pre-allocate)

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/ipc/message_framing.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <vector>

using namespace yams::daemon;

namespace {

// Standalone ISO CRC32 for test frame construction (calculate_crc32 is private)
uint32_t test_crc32(const std::vector<uint8_t>& data) {
    uint32_t crc = 0xFFFFFFFF;
    for (auto byte : data) {
        crc = crc ^ byte;
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320u : 0u);
        }
    }
    return ~crc;
}

// Build a raw frame: header (network byte order) + payload
std::vector<uint8_t> make_raw_frame(const std::vector<uint8_t>& payload, uint32_t flags = 0) {
    MessageFramer::FrameHeader header;
    header.payload_size = static_cast<uint32_t>(payload.size());
    header.checksum = test_crc32(payload);
    header.flags = flags;
    header.to_network();

    std::vector<uint8_t> frame(sizeof(header) + payload.size());
    std::memcpy(frame.data(), &header, sizeof(header));
    if (!payload.empty()) {
        std::memcpy(frame.data() + sizeof(header), payload.data(), payload.size());
    }
    return frame;
}

std::vector<uint8_t> make_header_only_frame() {
    MessageFramer::FrameHeader header;
    header.payload_size = 0;
    header.checksum = 0;
    header.set_header_only(true);
    header.to_network();

    std::vector<uint8_t> frame(sizeof(header));
    std::memcpy(frame.data(), &header, sizeof(header));
    return frame;
}

} // namespace

// =============================================================================
// FrameReader feed() / get_frame() tests
// =============================================================================

TEST_CASE("FrameReader: basic roundtrip via feed", "[daemon][framing][frame-reader]") {
    FrameReader reader;

    std::vector<uint8_t> payload = {0x01, 0x02, 0x03, 0x04, 0x05};
    auto frame = make_raw_frame(payload);

    auto result = reader.feed(frame.data(), frame.size());
    REQUIRE(result.status == FrameReader::FrameStatus::FrameComplete);
    REQUIRE(result.consumed == frame.size());
    REQUIRE(reader.has_frame());

    auto frameResult = reader.get_frame();
    REQUIRE(frameResult);
    REQUIRE(frameResult.value() == frame);
}

TEST_CASE("FrameReader: byte-at-a-time feed", "[daemon][framing][frame-reader]") {
    FrameReader reader;

    std::vector<uint8_t> payload(50);
    std::iota(payload.begin(), payload.end(), static_cast<uint8_t>(0));
    auto frame = make_raw_frame(payload);

    // Feed one byte at a time — exercises incremental buffer growth
    for (size_t i = 0; i < frame.size() - 1; ++i) {
        auto result = reader.feed(frame.data() + i, 1);
        REQUIRE(result.consumed == 1);
        REQUIRE(result.status == FrameReader::FrameStatus::NeedMoreData);
        REQUIRE_FALSE(reader.has_frame());
    }

    // Final byte completes the frame
    auto result = reader.feed(frame.data() + frame.size() - 1, 1);
    REQUIRE(result.consumed == 1);
    REQUIRE(result.status == FrameReader::FrameStatus::FrameComplete);
    REQUIRE(reader.has_frame());

    auto frameResult = reader.get_frame();
    REQUIRE(frameResult);
    REQUIRE(frameResult.value() == frame);
}

TEST_CASE("FrameReader: large payload (64KB+)", "[daemon][framing][frame-reader]") {
    FrameReader reader;

    // 65536 bytes of pseudo-random data
    std::vector<uint8_t> payload(65536);
    std::mt19937 rng(42);
    std::generate(payload.begin(), payload.end(),
                  [&rng]() { return static_cast<uint8_t>(rng() & 0xFF); });
    auto frame = make_raw_frame(payload);

    auto result = reader.feed(frame.data(), frame.size());
    REQUIRE(result.status == FrameReader::FrameStatus::FrameComplete);
    REQUIRE(reader.has_frame());

    auto frameResult = reader.get_frame();
    REQUIRE(frameResult);
    REQUIRE(frameResult.value().size() == frame.size());
    REQUIRE(frameResult.value() == frame);
}

TEST_CASE("FrameReader: two back-to-back frames in single feed",
          "[daemon][framing][frame-reader]") {
    FrameReader reader;

    std::vector<uint8_t> payload1 = {0xAA, 0xBB};
    std::vector<uint8_t> payload2 = {0xCC, 0xDD, 0xEE};
    auto frame1 = make_raw_frame(payload1);
    auto frame2 = make_raw_frame(payload2);

    // Concatenate both frames
    std::vector<uint8_t> combined;
    combined.insert(combined.end(), frame1.begin(), frame1.end());
    combined.insert(combined.end(), frame2.begin(), frame2.end());

    // First feed should consume only the first frame
    auto result1 = reader.feed(combined.data(), combined.size());
    REQUIRE(result1.status == FrameReader::FrameStatus::FrameComplete);
    REQUIRE(result1.consumed <= combined.size());
    REQUIRE(reader.has_frame());

    auto frameResult1 = reader.get_frame();
    REQUIRE(frameResult1);
    REQUIRE(frameResult1.value() == frame1);

    // Feed remaining bytes for second frame
    size_t remaining = combined.size() - result1.consumed;
    if (remaining > 0) {
        auto result2 = reader.feed(combined.data() + result1.consumed, remaining);
        REQUIRE(result2.status == FrameReader::FrameStatus::FrameComplete);
        REQUIRE(reader.has_frame());

        auto frameResult2 = reader.get_frame();
        REQUIRE(frameResult2);
        REQUIRE(frameResult2.value() == frame2);
    }
}

TEST_CASE("FrameReader: corrupted header (bad magic)", "[daemon][framing][frame-reader]") {
    FrameReader reader;

    std::vector<uint8_t> payload = {0x01, 0x02};
    auto frame = make_raw_frame(payload);

    // Corrupt the magic bytes
    frame[0] = 0x00;
    frame[1] = 0x00;

    auto result = reader.feed(frame.data(), frame.size());
    REQUIRE(result.status == FrameReader::FrameStatus::InvalidFrame);
}

TEST_CASE("FrameReader: header-only frame", "[daemon][framing][frame-reader]") {
    FrameReader reader;

    auto frame = make_header_only_frame();

    auto result = reader.feed(frame.data(), frame.size());
    REQUIRE(result.status == FrameReader::FrameStatus::FrameComplete);
    REQUIRE(result.is_header_only);
    REQUIRE(reader.has_frame());
}

TEST_CASE("FrameReader: reset clears state for reuse", "[daemon][framing][frame-reader]") {
    FrameReader reader;

    // Feed a complete frame
    std::vector<uint8_t> payload = {0x01};
    auto frame = make_raw_frame(payload);
    auto result = reader.feed(frame.data(), frame.size());
    REQUIRE(result.status == FrameReader::FrameStatus::FrameComplete);

    // Reset and feed a new frame
    reader.reset();
    REQUIRE_FALSE(reader.has_frame());

    std::vector<uint8_t> payload2 = {0x02, 0x03};
    auto frame2 = make_raw_frame(payload2);
    auto result2 = reader.feed(frame2.data(), frame2.size());
    REQUIRE(result2.status == FrameReader::FrameStatus::FrameComplete);

    auto frameResult = reader.get_frame();
    REQUIRE(frameResult);
    REQUIRE(frameResult.value() == frame2);
}

TEST_CASE("FrameReader: partial header then rest", "[daemon][framing][frame-reader]") {
    FrameReader reader;

    std::vector<uint8_t> payload = {0x10, 0x20, 0x30};
    auto frame = make_raw_frame(payload);

    // Feed partial header (10 bytes, header is 20)
    auto r1 = reader.feed(frame.data(), 10);
    REQUIRE(r1.consumed == 10);
    REQUIRE(r1.status == FrameReader::FrameStatus::NeedMoreData);

    // Feed rest of header + payload
    auto r2 = reader.feed(frame.data() + 10, frame.size() - 10);
    REQUIRE(r2.status == FrameReader::FrameStatus::FrameComplete);

    auto frameResult = reader.get_frame();
    REQUIRE(frameResult);
    REQUIRE(frameResult.value() == frame);
}

TEST_CASE("FrameReader: assembled frame surfaces malformed protobuf payload",
          "[daemon][framing][frame-reader][validation]") {
    FrameReader reader;
    MessageFramer framer;

    // Truncated protobuf varint that passes transport-level checks once wrapped
    // in a valid frame header with a matching CRC.
    const std::vector<uint8_t> malformed_payload = {0x80};
    auto frame = make_raw_frame(malformed_payload);

    auto feed_result = reader.feed(frame.data(), frame.size());
    REQUIRE(feed_result.status == FrameReader::FrameStatus::FrameComplete);
    REQUIRE(reader.has_frame());

    auto frame_result = reader.get_frame();
    REQUIRE(frame_result);

    auto parse_result = framer.parse_frame(frame_result.value());
    REQUIRE_FALSE(parse_result);
    REQUIRE(parse_result.error().code == yams::ErrorCode::SerializationError);
    CHECK(parse_result.error().message.find("Failed to parse protobuf Envelope") !=
          std::string::npos);
}
