// SPDX-License-Identifier: GPL-3.0-or-later
// IPC Protocol Fuzzer - Targets message framing and deserialization

#include <cstddef>
#include <cstdint>
#include <span>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

using namespace yams::daemon;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0)
        return 0;

    // Test 1: Parse raw frame header
    {
        MessageFramer framer;
        auto header_result = framer.parse_header(std::span<const uint8_t>(data, size));
        (void)header_result;
    }

    // Test 2: Try to parse complete frame
    {
        MessageFramer framer;
        auto frame_result = framer.parse_frame(std::span<const uint8_t>(data, size));
        (void)frame_result;
    }

    // Test 3: Feed data to FrameReader incrementally
    {
        FrameReader reader;
        size_t chunk_size = size > 0 ? size / 4 : 0;
        if (chunk_size == 0 && size > 0)
            chunk_size = 1;

        size_t offset = 0;
        while (offset < size) {
            size_t remaining = size - offset;
            size_t to_feed = remaining < chunk_size ? remaining : chunk_size;
            reader.append(std::span<const uint8_t>(data + offset, to_feed));
            offset += to_feed;

            auto frame_result = reader.try_read_frame();
            if (frame_result) {
                MessageFramer framer;
                auto msg_result = framer.parse_frame(frame_result.value());
                (void)msg_result;
            }
        }
    }

    return 0;
}
