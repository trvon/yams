// SPDX-License-Identifier: GPL-3.0-or-later
// IPC Roundtrip Fuzzer - Targets CLI<->daemon framing and payload encode/decode

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/request_handler.h>

using namespace yams::daemon;

namespace {
class FuzzRoundtripProcessor : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        if (std::holds_alternative<PingRequest>(request)) {
            PongResponse pong;
            pong.serverTime = std::chrono::steady_clock::now();
            pong.roundTrip = std::chrono::milliseconds{0};
            co_return Response{std::in_place_type<PongResponse>, std::move(pong)};
        }
        if (std::holds_alternative<StatusRequest>(request)) {
            StatusResponse status{};
            status.running = true;
            status.ready = true;
            status.version = "fuzz";
            co_return Response{std::in_place_type<StatusResponse>, std::move(status)};
        }
        co_return SuccessResponse{"fuzz: ok"};
    }
};

std::vector<uint8_t> run_handler_roundtrip(std::span<const uint8_t> payload,
                                           const RequestHandler::Config& config) {
    boost::asio::io_context io;
    auto processor = std::make_shared<FuzzRoundtripProcessor>();
    RequestHandler handler(processor, config);

    auto fut = boost::asio::co_spawn(
        io, handler.handle_request(std::vector<uint8_t>(payload.begin(), payload.end()),
                                   yams::compat::stop_token{}),
        boost::asio::use_future);
    io.run();
    return fut.get();
}
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < MessageFramer::HEADER_SIZE) {
        return 0;
    }

    FrameReader reader;
    MessageFramer framer;

    const size_t chunk = std::max<size_t>(1, size / 4);
    size_t offset = 0;

    while (offset < size) {
        const size_t remaining = size - offset;
        const size_t to_feed = std::min(remaining, chunk);
        reader.append(std::span<const uint8_t>(data + offset, to_feed));
        offset += to_feed;

        while (true) {
            auto frame_result = reader.try_read_frame();
            if (!frame_result) {
                break;
            }

            const auto& frame = frame_result.value();
            if (frame.size() < MessageFramer::HEADER_SIZE) {
                continue;
            }

            auto header_result = framer.parse_header(frame);
            if (!header_result) {
                continue;
            }

            const auto payload_size = header_result.value().payload_size;
            if (payload_size == 0 || frame.size() < MessageFramer::HEADER_SIZE + payload_size) {
                continue;
            }

            const auto payload = std::span<const uint8_t>(
                frame.data() + MessageFramer::HEADER_SIZE, payload_size);

            RequestHandler::Config config;
            config.enable_streaming = (data[0] & 1) != 0;
            config.enable_multiplexing = (data[0] & 2) != 0;
            config.max_frame_size = 1 * 1024 * 1024;

            std::vector<uint8_t> response_payload;
            try {
                response_payload = run_handler_roundtrip(payload, config);
            } catch (...) {
                continue;
            }

            auto response_msg = ProtoSerializer::decode_payload(response_payload);
            if (!response_msg) {
                continue;
            }

            auto response_frame = framer.frame_message(response_msg.value());
            if (!response_frame) {
                continue;
            }

            auto parsed_response = framer.parse_frame(response_frame.value());
            (void)parsed_response;
        }
    }

    return 0;
}
