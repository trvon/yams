#include <unistd.h>
#include <gtest/gtest.h>
#include <sys/socket.h>

#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/daemon/ipc/request_handler.h>

using namespace yams::daemon;

// Minimal RequestProcessor that routes Ping to Pong without ServiceManager
struct MiniProcessor : public RequestProcessor {
    Task<Response> process(const Request& req) override {
        if (std::holds_alternative<PingRequest>(req)) {
            PongResponse pong{};
            pong.serverTime = std::chrono::system_clock::now();
            co_return Response{pong};
        }
        co_return ErrorResponse{ErrorCode::NotImplemented, "mini"};
    }
};

TEST(RequestHandlerSocketpair, PingPongEndToEnd) {
    int sv[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0) << strerror(errno);

    AsyncIOContext io;
    AsyncSocket<AsyncIOContext> server(sv[0], io);
    AsyncSocket<AsyncIOContext> client(sv[1], io);

    // Launch the handler coroutine (fire-and-forget)
    auto proc = std::make_shared<MiniProcessor>();
    RequestHandler::Config cfg;
    cfg.enable_streaming = false;
    RequestHandler handler(proc, cfg);

    // Client writes a framed Ping request
    Message m;
    m.version = PROTOCOL_VERSION;
    m.requestId = 42;
    m.payload = Request{PingRequest{}};
    MessageFramer framer;
    auto frame = framer.frame_message(m);
    ASSERT_TRUE(frame) << frame.error().message;
    ASSERT_TRUE(client.async_write_all(frame.value()).get());

    // Server handles connection: should parse and respond with Pong
    (void)handler.handle_connection(std::move(server), std::stop_token{});

    // Read response header and payload from client side
    MessageFramer::FrameReader reader(MessageFramer::MAX_MESSAGE_SIZE);
    std::array<uint8_t, 4096> buf{};
    for (int i = 0; i < 5 && !reader.has_frame(); ++i) {
        auto r = client.async_read(buf.data(), buf.size()).get();
        ASSERT_TRUE(r);
        auto fed = reader.feed(buf.data(), r.value());
        ASSERT_NE(fed.status, MessageFramer::FrameReader::FrameStatus::InvalidFrame);
        ASSERT_NE(fed.status, MessageFramer::FrameReader::FrameStatus::FrameTooLarge);
    }
    ASSERT_TRUE(reader.has_frame());
    auto f = reader.get_frame();
    ASSERT_TRUE(f);
    auto parsed = framer.parse_frame(f.value());
    ASSERT_TRUE(parsed);
    auto out = parsed.value();
    ASSERT_TRUE(std::holds_alternative<Response>(out.payload));
    auto resp = std::get<Response>(out.payload);
    ASSERT_TRUE(std::holds_alternative<PongResponse>(resp));
}
