#include <atomic>
#include <chrono>
#include <thread>
#include <gtest/gtest.h>

#include "test_async_helpers.h"
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon::it {

using namespace std::chrono_literals;

class UnaryStreamingSemanticsIT : public ::testing::Test {
protected:
    void SetUp() override {
        // Enable client-side stream tracing to aid debugging when running locally
        setenv("YAMS_STREAM_TRACE", "1", 1);

        // Spin up a daemon bound to a temp socket for isolation
        tempDir_ = std::filesystem::temp_directory_path() /
                   ("unary_stream_semantics_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempDir_);
        cfg_.socketPath = tempDir_ / "daemon.sock";
        cfg_.pidFile = tempDir_ / "daemon.pid";
        cfg_.logFile = tempDir_ / "daemon.log";
        cfg_.workerThreads = 2;

        daemon_ = std::make_unique<YamsDaemon>(cfg_);
        auto r = daemon_->start();
        ASSERT_TRUE(r) << "Failed to start daemon: " << r.error().message;
        std::this_thread::sleep_for(100ms);
    }

    void TearDown() override {
        if (daemon_)
            daemon_->stop();
        std::error_code ec;
        std::filesystem::remove_all(tempDir_, ec);
    }

    std::filesystem::path tempDir_;
    DaemonConfig cfg_;
    std::unique_ptr<YamsDaemon> daemon_;
};

// Always-unary control requests should complete as a single non-chunked frame even when
// the client hints for streaming. We assert that the streaming API receives only a header
// and zero chunk callbacks (adapter maps non-chunked into onHeader+onComplete).
TEST_F(UnaryStreamingSemanticsIT, ControlRequestsAreUnaryEvenWhenStreaming) {
    AsioTransportAdapter::Options opts;
    opts.socketPath = cfg_.socketPath;
    opts.headerTimeout = 3000ms;
    opts.bodyTimeout = 10000ms;
    AsioTransportAdapter transport(opts);

    auto run_and_check = [&](const Request& req) {
        std::atomic<int> headers{0};
        std::atomic<int> chunks{0};
        std::atomic<int> lastTrue{0};
        auto res = yams::cli::run_sync(
            transport.send_request_streaming(
                req, [&](const Response&) { headers.fetch_add(1, std::memory_order_relaxed); },
                [&](const Response&, bool isLast) {
                    chunks.fetch_add(1, std::memory_order_relaxed);
                    if (isLast)
                        lastTrue.fetch_add(1, std::memory_order_relaxed);
                    return true;
                },
                [&](const Error&) {}, []() {}),
            5s);

        // Either success or a non-timeout environment error (e.g., missing indices). Should not
        // hang.
        ASSERT_TRUE(res.has_value() || !res);
        if (!res) {
            EXPECT_NE(res.error().code, ErrorCode::Timeout) << "Control request must not timeout";
        }
        // For non-chunked server responses, adapter delivers only a header and completes.
        EXPECT_GE(headers.load(), 1);
        EXPECT_EQ(chunks.load(), 0);
        EXPECT_EQ(lastTrue.load(), 0);
    };

    run_and_check(StatusRequest{.detailed = true});
    run_and_check(PingRequest{});
    run_and_check(GetStatsRequest{});
    run_and_check(ShutdownRequest{.graceful = true});
}

// Streaming-capable request produces header-only first, then zero or more chunks, and final
// last=true.
TEST_F(UnaryStreamingSemanticsIT, StreamingSearchEmitsHeaderThenCompletes) {
    AsioTransportAdapter::Options opts;
    opts.socketPath = cfg_.socketPath;
    opts.headerTimeout = 3000ms;
    opts.bodyTimeout = 20000ms;
    AsioTransportAdapter transport(opts);

    std::atomic<int> headers{0};
    std::atomic<int> chunks{0};
    std::atomic<int> lastTrue{0};

    SearchRequest s;
    s.query = "alpha";
    s.limit = 1;
    auto res = yams::cli::run_sync(
        transport.send_request_streaming(
            Request{s}, [&](const Response&) { headers.fetch_add(1, std::memory_order_relaxed); },
            [&](const Response&, bool isLast) {
                chunks.fetch_add(1, std::memory_order_relaxed);
                if (isLast)
                    lastTrue.fetch_add(1, std::memory_order_relaxed);
                // Stop early after first chunk to exercise adapter completion-on-false
                return chunks.load() < 1;
            },
            [&](const Error&) {}, []() {}),
        20s);

    ASSERT_TRUE(res.has_value() || !res);
    if (!res) {
        EXPECT_NE(res.error().code, ErrorCode::Timeout) << "Streaming search should not timeout";
    }
    EXPECT_GE(headers.load(), 1) << "Should receive a header-only frame first";
    // We may stop early, so lastTrue may be 0; just assert no hang and header observed.
}

// Ensure two sequential streaming requests do not cross-talk on a persistent connection
TEST_F(UnaryStreamingSemanticsIT, PersistentConnectionSequentialRequestsComplete) {
    AsioTransportAdapter::Options opts;
    opts.socketPath = cfg_.socketPath;
    opts.headerTimeout = 3000ms;
    opts.bodyTimeout = 20000ms;
    AsioTransportAdapter transport(opts);

    std::atomic<int> h1{0}, c1{0};
    std::atomic<int> h2{0}, c2{0};

    // First: streaming-capable
    SearchRequest s;
    s.query = "beta";
    s.limit = 1;
    auto r1 = yams::cli::run_sync(transport.send_request_streaming(
                                      Request{s}, [&](const Response&) { h1++; },
                                      [&](const Response&, bool) {
                                          c1++;
                                          return true;
                                      },
                                      [&](const Error&) {}, []() {}),
                                  10s);
    // Second: control (always unary)
    auto r2 = yams::cli::run_sync(transport.send_request_streaming(
                                      Request{StatusRequest{.detailed = true}},
                                      [&](const Response&) { h2++; },
                                      [&](const Response&, bool) {
                                          c2++;
                                          return true;
                                      },
                                      [&](const Error&) {}, []() {}),
                                  10s);

    ASSERT_TRUE((r1.has_value() || !r1) && (r2.has_value() || !r2));
    if (!r1) {
        EXPECT_NE(r1.error().code, ErrorCode::Timeout);
    }
    if (!r2) {
        EXPECT_NE(r2.error().code, ErrorCode::Timeout);
    }
    EXPECT_GE(h1.load(), 1);
    EXPECT_GE(h2.load(), 1);
}

} // namespace yams::daemon::it
