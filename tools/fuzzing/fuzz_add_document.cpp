// SPDX-License-Identifier: Apache-2.0
// AddDocumentRequest Fuzzer - Targets document ingestion deserialization

#include <cstddef>
#include <cstdint>
#include <span>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

using namespace yams::daemon;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4)
        return 0;

    // Test 1: Direct deserialization of AddDocumentRequest
    {
        auto result = AddDocumentRequest::deserialize(std::span<const uint8_t>(data, size));
        if (result) {
            const auto& req = result.value();
            [[maybe_unused]] auto paths_size = req.paths.size();
            [[maybe_unused]] auto tags_size = req.tags.size();
            [[maybe_unused]] auto content_size = req.content.size();

            for (const auto& path : req.paths) {
                [[maybe_unused]] auto path_len = path.size();
            }
        }
    }

    // Test 2: Wrap in framed message
    {
        MessageFramer framer;
        auto frame_result = framer.parse_frame(std::span<const uint8_t>(data, size));
        if (frame_result) {
            const auto& msg = frame_result.value();
            if (std::holds_alternative<Request>(msg.payload)) {
                const auto& request = std::get<Request>(msg.payload);
                if (std::holds_alternative<AddDocumentRequest>(request)) {
                    const auto& add_req = std::get<AddDocumentRequest>(request);
                    [[maybe_unused]] auto recursive = add_req.recursive;
                }
            }
        }
    }

    // Test 3: UpdateDocumentRequest
    {
        auto result = UpdateDocumentRequest::deserialize(std::span<const uint8_t>(data, size));
        if (result) {
            const auto& req = result.value();
            [[maybe_unused]] auto paths_size = req.paths.size();
        }
    }

    return 0;
}
