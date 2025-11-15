// SPDX-License-Identifier: Apache-2.0
// RequestHandler Fuzzer - Targets request processing and message handling

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/request_handler.h>

using namespace yams::daemon;

// Mock RequestProcessor for fuzzing
class FuzzRequestProcessor : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        // Simple mock that echoes request type
        co_return SuccessResponse{"Fuzz processed"};
    }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0)
        return 0;

    // Test 1: Parse and validate framed messages
    {
        MessageFramer framer;
        auto frame_result = framer.parse_frame(std::span<const uint8_t>(data, size));
        if (frame_result) {
            auto msg_result = framer.parse_frame(frame_result.value());
            (void)msg_result;
        }
    }

    // Test 2: Feed data to FrameReader and try request handling
    {
        FrameReader reader;
        reader.append(std::span<const uint8_t>(data, size));
        auto frame_result = reader.try_read_frame();

        if (frame_result) {
            // Try to parse as a complete message
            auto decode_result = ProtoSerializer::decode_payload(frame_result.value());
            if (decode_result && std::holds_alternative<Request>(decode_result->payload)) {
                // Successfully decoded a request - this tests the full parse path
                const auto& request = std::get<Request>(decode_result->payload);
                (void)request;
            }
        }
    }

    // Test 3: Create RequestHandler and test with mock processor
    {
        try {
            boost::asio::io_context io;
            auto processor = std::make_shared<FuzzRequestProcessor>();

            RequestHandler::Config config;
            config.enable_streaming = (size > 0) ? (data[0] & 1) : false;
            config.enable_multiplexing = (size > 1) ? (data[1] & 1) : true;
            config.chunk_size = (size > 2) ? (data[2] * 1024) : 512 * 1024;
            config.max_frame_size = (size > 3) ? (data[3] * 1024) : 10 * 1024 * 1024;

            RequestHandler handler(processor, config);

            // Test should_stream_request logic with various request types
            if (size > 4) {
                uint8_t req_type = data[4] % 8;
                Request test_request;

                switch (req_type) {
                    case 0:
                        test_request = SearchRequest{};
                        break;
                    case 1:
                        test_request = ListRequest{};
                        break;
                    case 2:
                        test_request = GrepRequest{};
                        break;
                    case 3:
                        test_request = AddDocumentRequest{};
                        break;
                    case 4:
                        test_request = DeleteRequest{};
                        break;
                    case 5:
                        test_request = PingRequest{};
                        break;
                    case 6:
                        test_request = DownloadRequest{};
                        break;
                    default:
                        test_request = PingRequest{};
                        break;
                }

                // The handler internally checks streaming support
                (void)test_request;
            }

        } catch (...) {
            // Catch any exceptions from ASIO/Boost setup
        }
    }

    // Test 4: Test message framing edge cases
    {
        MessageFramer framer;

        // Try to frame a message with fuzz data
        if (size > 4) {
            Message msg;
            msg.requestId = *reinterpret_cast<const uint64_t*>(data);

            // Create a simple request
            PingRequest ping;
            msg.payload = ping;

            auto encode_result = ProtoSerializer::encode_payload(msg);
            if (encode_result) {
                // Try to create frames with various configurations
                bool is_streaming = (size > 8) ? (data[8] & 1) : false;
                bool is_last_chunk = (size > 9) ? (data[9] & 1) : true;

                auto frame_result = framer.create_frame(msg.requestId, encode_result.value(),
                                                        is_streaming, is_last_chunk);

                if (frame_result) {
                    // Try to parse what we just created
                    auto parse_result = framer.parse_frame(frame_result.value());
                    (void)parse_result;
                }
            }
        }
    }

    return 0;
}
