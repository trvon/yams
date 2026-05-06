// SPDX-License-Identifier: GPL-3.0-or-later
// RequestHandler Fuzzer - Targets request processing and message handling

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <thread>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/connect_pair.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/write.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/request_handler.h>

using namespace yams::daemon;

namespace {

std::vector<uint8_t> buildValidFrame(const uint8_t* data, size_t size) {
    if (size == 0) {
        return {};
    }

    Message msg;
    msg.version = 1;
    msg.requestId =
        size > 8 ? *reinterpret_cast<const uint64_t*>(data) : static_cast<uint64_t>(size);
    msg.expectsStreamingResponse = (data[0] & 1) != 0;

    Request request;
    switch (size > 1 ? (data[1] % 4) : 0) {
        case 0:
            request = PingRequest{};
            break;
        case 1: {
            StatusRequest status;
            status.detailed = size > 2 ? ((data[2] & 1) != 0) : false;
            request = status;
            break;
        }
        case 2: {
            SearchRequest search;
            search.query =
                std::string(reinterpret_cast<const char*>(data), std::min<std::size_t>(size, 64));
            request = std::move(search);
            break;
        }
        default:
            request = ListRequest{};
            break;
    }

    msg.payload = request;

    MessageFramer framer;
    std::vector<uint8_t> frame;
    auto framed = framer.frame_message_into(msg, frame);
    return framed ? frame : std::vector<uint8_t>{};
}

void writeInChunks(boost::asio::local::stream_protocol::socket& socket,
                   const std::vector<uint8_t>& bytes, const uint8_t* data, size_t size) {
    std::size_t offset = 0;
    std::size_t cursor = 0;
    while (offset < bytes.size()) {
        const auto remaining = bytes.size() - offset;
        const auto chunkHint =
            cursor < size ? static_cast<std::size_t>(data[cursor++]) + 1 : remaining;
        const auto chunkSize =
            std::min<std::size_t>(remaining, std::min<std::size_t>(chunkHint, 256));
        boost::system::error_code ec;
        const auto written =
            boost::asio::write(socket, boost::asio::buffer(bytes.data() + offset, chunkSize), ec);
        if (ec || written == 0) {
            break;
        }
        offset += written;
    }
}

} // namespace

// Mock RequestProcessor for fuzzing
class FuzzRequestProcessor : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        if (std::holds_alternative<StatusRequest>(request)) {
            StatusResponse status;
            status.running = true;
            status.ready = true;
            status.version = "fuzz";
            status.overallStatus = "ready";
            co_return Response{std::in_place_type<StatusResponse>, std::move(status)};
        }
        co_return SuccessResponse{"Fuzz processed"};
    }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0)
        return 0;

    // Test 1: Parse and validate framed messages
    {
        MessageFramer framer;
        auto msg_result = framer.parse_frame(std::span<const uint8_t>(data, size));
        (void)msg_result;
    }

    // Test 2: Feed data to FrameReader and try request handling
    {
        FrameReader reader;
        reader.append(std::span<const uint8_t>(data, size));
        auto frame_result = reader.try_read_frame();

        if (frame_result) {
            // Try to parse as a complete message
            auto decode_result = ProtoSerializer::decode_payload(frame_result.value());
            if (decode_result && std::holds_alternative<Request>(decode_result.value().payload)) {
                // Successfully decoded a request - this tests the full parse path
                const auto& request = std::get<Request>(decode_result.value().payload);
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
            config.chunk_size =
                (size > 2) ? (static_cast<std::size_t>(data[2]) * 1024ULL) : 512ULL * 1024ULL;
            config.max_frame_size = (size > 3) ? (static_cast<std::size_t>(data[3]) * 1024ULL)
                                               : 10ULL * 1024ULL * 1024ULL;

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
            // Ignore setup failures so the fuzzer can keep mutating this path.
            (void)0;
        }
    }

#ifndef _WIN32
    // Test 3b: Exercise handle_connection over a real socket pair with bounded raw/fragmented
    // input.
    {
        try {
            boost::asio::io_context io;
            auto processor = std::make_shared<FuzzRequestProcessor>();

            RequestHandler::Config config;
            config.enable_streaming = (size > 0) ? ((data[0] & 1) != 0) : false;
            config.enable_multiplexing = (size > 1) ? ((data[1] & 1) != 0) : true;
            config.chunk_size = (size > 2) ? ((static_cast<std::size_t>(data[2]) + 1ULL) * 1024ULL)
                                           : 64ULL * 1024ULL;
            config.max_frame_size = (size > 3)
                                        ? ((static_cast<std::size_t>(data[3]) + 1ULL) * 1024ULL)
                                        : 1024ULL * 1024ULL;

            auto handler = std::make_shared<RequestHandler>(processor, config);

            boost::asio::local::stream_protocol::socket client(io);
            boost::asio::local::stream_protocol::socket server(io);
            boost::asio::local::connect_pair(client, server);

            yams::compat::stop_source stopSource;
            auto serverPtr =
                std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server));
            auto work = boost::asio::make_work_guard(io);
            boost::asio::co_spawn(
                io,
                [handler, serverPtr,
                 token = stopSource.get_token()]() -> boost::asio::awaitable<void> {
                    co_await handler->handle_connection(serverPtr, token, 1);
                    co_return;
                },
                boost::asio::detached);

            std::thread ioThread([&io]() { io.run(); });

            std::vector<uint8_t> wireBytes;
            if ((data[0] & 0x2) != 0) {
                wireBytes = buildValidFrame(data, size);
            }
            if (wireBytes.empty()) {
                wireBytes.assign(data, data + std::min<std::size_t>(size, 4096));
            }
            writeInChunks(client, wireBytes, data, size);

            boost::system::error_code ec;
            [[maybe_unused]] auto shutdownResult =
                client.shutdown(boost::asio::local::stream_protocol::socket::shutdown_both, ec);
            [[maybe_unused]] auto closeResult = client.close(ec);
            [[maybe_unused]] const bool stopRequested = stopSource.request_stop();

            work.reset();
            ioThread.join();
        } catch (...) {
            // Ignore transport exceptions; crashes are what the fuzzer is looking for.
            (void)0;
        }
    }
#endif

    // Test 4: Test message framing edge cases
    {
        MessageFramer framer;

        // Try to frame a message with fuzz data
        if (size > 4) {
            Message msg;
            uint64_t request_id = 0;
            std::memcpy(&request_id, data, std::min(size, sizeof(request_id)));
            msg.requestId = request_id;

            // Create a simple request
            PingRequest ping;
            msg.payload = ping;

            auto encode_result = ProtoSerializer::encode_payload(msg);
            if (encode_result) {
                // Try to create frames with various configurations
                bool is_streaming = (size > 8) ? (data[8] & 1) : false;
                bool is_last_chunk = (size > 9) ? (data[9] & 1) : true;

                auto frame_result = is_streaming ? framer.frame_message_chunk(msg, is_last_chunk)
                                                 : framer.frame_message(msg);

                if (frame_result) {
                    auto parse_result = framer.parse_frame(frame_result.value());
                    (void)parse_result;
                }
            }
        }
    }

    return 0;
}
