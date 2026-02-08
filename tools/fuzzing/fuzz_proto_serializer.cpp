// SPDX-License-Identifier: GPL-3.0-or-later
// ProtoSerializer Fuzzer - Targets protobuf encoding/decoding of IPC messages

#include <cstddef>
#include <cstdint>
#include <span>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>

using namespace yams::daemon;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0)
        return 0;

    // Test 1: Decode arbitrary protobuf bytes as a Message payload
    {
        auto decode_result = ProtoSerializer::decode_payload(std::span<const uint8_t>(data, size));
        // If decode succeeds, try to re-encode to verify round-trip stability
        if (decode_result) {
            auto encode_result = ProtoSerializer::encode_payload(decode_result.value());
            (void)encode_result;
        }
    }

    // Test 2: Try decode with chunked input (partial reads)
    if (size > 4) {
        size_t split_point = size / 2;
        // First chunk
        auto partial_result =
            ProtoSerializer::decode_payload(std::span<const uint8_t>(data, split_point));
        (void)partial_result;

        // Second chunk (overlap)
        auto partial_result2 = ProtoSerializer::decode_payload(
            std::span<const uint8_t>(data + split_point, size - split_point));
        (void)partial_result2;
    }

    // Test 3: Create a synthetic Message and encode with appended fuzz data
    {
        Message msg;
        msg.requestId = (size > 0) ? data[0] : 0;

        // Try various request types with fuzz-driven selection
        if (size > 1) {
            uint8_t selector = data[1] % 10;
            switch (selector) {
                case 0: {
                    SearchRequest req;
                    req.query = std::string(reinterpret_cast<const char*>(data),
                                            std::min(size, size_t(256)));
                    req.limit = (size > 2) ? data[2] : 10;
                    req.fuzzy = (size > 3) ? (data[3] & 1) : false;
                    msg.payload = req;
                    break;
                }
                case 1: {
                    ListRequest req;
                    req.limit = (size > 2) ? data[2] : 10;
                    msg.payload = req;
                    break;
                }
                case 2: {
                    GrepRequest req;
                    req.pattern = std::string(reinterpret_cast<const char*>(data),
                                              std::min(size, size_t(128)));
                    msg.payload = req;
                    break;
                }
                case 3: {
                    AddDocumentRequest req;
                    req.content = std::string(reinterpret_cast<const char*>(data),
                                              std::min(size, size_t(1024)));
                    msg.payload = req;
                    break;
                }
                case 4: {
                    DeleteRequest req;
                    req.hash = std::string(reinterpret_cast<const char*>(data),
                                           std::min(size, size_t(256)));
                    msg.payload = req;
                    break;
                }
                default: {
                    // Use PingRequest for other cases
                    msg.payload = PingRequest{};
                    break;
                }
            }
        } else {
            msg.payload = PingRequest{};
        }

        // Try to encode the synthetic message
        auto encode_result = ProtoSerializer::encode_payload(msg);
        if (encode_result) {
            // Try to decode what we just encoded
            auto round_trip = ProtoSerializer::decode_payload(encode_result.value());
            (void)round_trip;
        }
    }

    return 0;
}
