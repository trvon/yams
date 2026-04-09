// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/request_handler.h>

#include <boost/asio.hpp>

#include <array>
#include <memory>
#include <thread>
#include <vector>

using namespace yams::daemon;

namespace {

class StatusOnlyProcessor final : public RequestProcessor {
public:
    boost::asio::awaitable<Response> process(const Request& request) override {
        if (!std::holds_alternative<StatusRequest>(request)) {
            co_return Response{
                std::in_place_type<ErrorResponse>,
                ErrorResponse{yams::ErrorCode::InvalidArgument, "unexpected request type"}};
        }

        StatusResponse response{};
        response.running = true;
        response.ready = true;
        response.version = "test";
        response.overallStatus = "ready";
        co_return Response{std::in_place_type<StatusResponse>, std::move(response)};
    }
};

Message read_message(boost::asio::local::stream_protocol::socket& socket, MessageFramer& framer) {
    std::array<uint8_t, MessageFramer::HEADER_SIZE> header_buf{};
    boost::asio::read(socket, boost::asio::buffer(header_buf));

    auto header_result =
        framer.parse_header(std::span<const uint8_t>(header_buf.data(), header_buf.size()));
    REQUIRE(header_result);

    std::vector<uint8_t> frame(header_buf.begin(), header_buf.end());
    const auto payload_size = header_result.value().payload_size;
    if (payload_size > 0) {
        std::vector<uint8_t> payload(payload_size);
        boost::asio::read(socket, boost::asio::buffer(payload));
        frame.insert(frame.end(), payload.begin(), payload.end());
    }

    auto message_result = framer.parse_frame(frame);
    REQUIRE(message_result);
    return std::move(message_result.value());
}

} // namespace

TEST_CASE("RequestHandler: admission rejection keeps persistent session reusable",
          "[daemon][ipc][admission][reuse]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    boost::asio::io_context io;

    boost::asio::local::stream_protocol::socket client_sock(io);
    boost::asio::local::stream_protocol::socket server_sock(io);
    boost::asio::local::connect_pair(client_sock, server_sock);

    RequestHandler::Config cfg;
    cfg.enable_multiplexing = false;
    cfg.enable_streaming = false;
    cfg.admission_control =
        [](const Request& request) -> std::optional<RequestHandler::Config::AdmissionDecision> {
        if (std::holds_alternative<PingRequest>(request)) {
            return RequestHandler::Config::AdmissionDecision{
                .code = yams::ErrorCode::ResourceExhausted,
                .message = "busy",
                .retry = ErrorResponse::RetryInfo{125, "overload"},
            };
        }
        return std::nullopt;
    };

    auto processor = std::make_shared<StatusOnlyProcessor>();
    auto handler = std::make_shared<RequestHandler>(processor, cfg);

    yams::compat::stop_source stop_source;
    auto server_sock_ptr =
        std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(server_sock));
    boost::asio::co_spawn(
        io,
        [handler, server_sock_ptr,
         token = stop_source.get_token()]() -> boost::asio::awaitable<void> {
            co_await handler->handle_connection(server_sock_ptr, token, 1);
            co_return;
        },
        boost::asio::detached);

    auto work = boost::asio::make_work_guard(io);
    std::thread io_thread([&io]() { io.run(); });

    MessageFramer framer(1024 * 1024);

    Message rejected_request;
    rejected_request.version = 1;
    rejected_request.requestId = 10;
    rejected_request.payload = Request{std::in_place_type<PingRequest>, PingRequest{}};

    std::vector<uint8_t> rejected_frame;
    REQUIRE(framer.frame_message_into(rejected_request, rejected_frame));
    boost::asio::write(client_sock, boost::asio::buffer(rejected_frame));

    auto rejected_response = read_message(client_sock, framer);
    REQUIRE(rejected_response.requestId == 10);
    auto* rejected_payload = std::get_if<Response>(&rejected_response.payload);
    REQUIRE(rejected_payload != nullptr);
    auto* rejected_error = std::get_if<ErrorResponse>(rejected_payload);
    REQUIRE(rejected_error != nullptr);
    REQUIRE(rejected_error->code == yams::ErrorCode::ResourceExhausted);
    REQUIRE(rejected_error->message == "busy");
    REQUIRE(rejected_error->retry.has_value());
    CHECK(rejected_error->retry->retryAfterMs == 125);
    CHECK(rejected_error->retry->reason == "overload");

    Message accepted_request;
    accepted_request.version = 1;
    accepted_request.requestId = 11;
    accepted_request.payload =
        Request{std::in_place_type<StatusRequest>, StatusRequest{.detailed = false}};

    std::vector<uint8_t> accepted_frame;
    REQUIRE(framer.frame_message_into(accepted_request, accepted_frame));
    boost::asio::write(client_sock, boost::asio::buffer(accepted_frame));

    auto accepted_response = read_message(client_sock, framer);
    REQUIRE(accepted_response.requestId == 11);
    auto* accepted_payload = std::get_if<Response>(&accepted_response.payload);
    REQUIRE(accepted_payload != nullptr);
    auto* status = std::get_if<StatusResponse>(accepted_payload);
    REQUIRE(status != nullptr);
    REQUIRE(status->ready);
    REQUIRE(status->overallStatus == "ready");

    stop_source.request_stop();
    {
        boost::system::error_code ec;
        client_sock.close(ec);
    }
    work.reset();
    io_thread.join();
}
