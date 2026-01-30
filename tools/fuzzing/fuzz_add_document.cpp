// SPDX-License-Identifier: GPL-3.0-or-later
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

    // Test framed message parsing - exercises full deserialization
    {
        MessageFramer framer;
        auto frame_result = framer.parse_frame(std::span<const uint8_t>(data, size));
        if (frame_result) {
            const auto& msg = frame_result.value();
            if (std::holds_alternative<Request>(msg.payload)) {
                const auto& request = std::get<Request>(msg.payload);

                // Exercise AddDocumentRequest fields if present
                if (std::holds_alternative<AddDocumentRequest>(request)) {
                    const auto& add_req = std::get<AddDocumentRequest>(request);
                    [[maybe_unused]] auto recursive = add_req.recursive;
                    [[maybe_unused]] auto path_size = add_req.path.size();
                    [[maybe_unused]] auto tags_size = add_req.tags.size();
                    [[maybe_unused]] auto content_size = add_req.content.size();
                    [[maybe_unused]] auto include_patterns_size = add_req.includePatterns.size();

                    for (const auto& tag : add_req.tags) {
                        [[maybe_unused]] auto tag_len = tag.size();
                    }
                    for (const auto& pattern : add_req.includePatterns) {
                        [[maybe_unused]] auto pattern_len = pattern.size();
                    }
                }

                // Exercise UpdateDocumentRequest fields if present
                if (std::holds_alternative<UpdateDocumentRequest>(request)) {
                    const auto& upd_req = std::get<UpdateDocumentRequest>(request);
                    [[maybe_unused]] auto add_tags_size = upd_req.addTags.size();
                    [[maybe_unused]] auto remove_tags_size = upd_req.removeTags.size();
                    [[maybe_unused]] auto new_content_size = upd_req.newContent.size();
                }
            }
        }
    }

    return 0;
}
