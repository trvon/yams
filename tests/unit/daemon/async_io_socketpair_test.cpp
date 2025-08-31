#include <unistd.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <yams/daemon/ipc/async_socket.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

using namespace yams::daemon;

TEST(AsyncIoSocketpairTest, FrameRoundTripOverSocketpair) {
    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0) << strerror(errno);

    AsyncIOContext io;
    DefaultAsyncSocket a(sv[0], io);
    DefaultAsyncSocket b(sv[1], io);

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = 1;
    msg.payload =
        Response{PongResponse{std::chrono::steady_clock::now(), std::chrono::milliseconds{0}}};

    MessageFramer framer;
    auto frame = framer.frame_message(msg);
    ASSERT_TRUE(frame) << frame.error().message;

    auto w = a.async_write_all(frame.value()).get();
    ASSERT_TRUE(w) << w.error().message;

    FrameReader reader; // default max size
    std::vector<uint8_t> buf(4096);
    for (int i = 0; i < 3 && !reader.has_frame(); ++i) {
        auto r = b.async_read(buf.data(), buf.size()).get();
        ASSERT_TRUE(r) << r.error().message;
        auto fed = reader.feed(buf.data(), r.value());
        ASSERT_NE(fed.status, FrameReader::FrameStatus::InvalidFrame);
        ASSERT_NE(fed.status, FrameReader::FrameStatus::FrameTooLarge);
        if (reader.has_frame())
            break;
    }
    ASSERT_TRUE(reader.has_frame());
    auto got = reader.get_frame();
    ASSERT_TRUE(got) << got.error().message;

    auto parsed = framer.parse_frame(got.value());
    ASSERT_TRUE(parsed) << parsed.error().message;

    auto out = parsed.value();
    ASSERT_TRUE(std::holds_alternative<Response>(out.payload));
    auto resp = std::get<Response>(out.payload);
    ASSERT_TRUE(std::holds_alternative<PongResponse>(resp));
}
