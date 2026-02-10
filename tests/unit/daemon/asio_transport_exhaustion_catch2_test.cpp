// AsioTransportAdapter unit tests for maxInflight exhaustion behavior

#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/use_future.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <thread>

#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#ifndef _WIN32
#include <sys/un.h>
#endif

using namespace yams::daemon;
using yams::ErrorCode;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

fs::path makeTempRuntimeDir(const std::string& name) {
    auto base = fs::temp_directory_path();
#ifndef _WIN32
    constexpr std::size_t maxUnixPath = sizeof(sockaddr_un::sun_path) - 1;
    auto candidate = base / "yams-transport-tests" / name / "ipc.sock";
    if (candidate.native().size() >= maxUnixPath) {
        base = fs::path("/tmp");
    }
#endif
    auto dir = base / "yams-transport-tests" / name;
    std::error_code ec;
    fs::create_directories(dir, ec);
    return dir;
}

std::string randomSuffix() {
    return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

} // namespace

TEST_CASE("AsioTransportAdapter returns ResourceExhausted when maxInflight exceeded",
          "[daemon][transport][unit]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("max-inflight-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::atomic<bool> stop{false};
    std::promise<void> accepted;
    auto acceptedFuture = accepted.get_future();

    std::thread server_thread([&] {
        try {
            boost::system::error_code bec;
            boost::asio::local::stream_protocol::socket sock(server_io);
            acceptor.accept(sock, bec);
            if (bec) {
                accepted.set_exception(
                    std::make_exception_ptr(std::runtime_error("accept failed: " + bec.message())));
                return;
            }

            // Only allow a single client connection. This forces all client requests to reuse the
            // same underlying socket, so maxInflight is exercised on one connection.
            acceptor.close(bec);

            accepted.set_value();
            while (!stop.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(5ms);
            }
            sock.close(bec);
        } catch (...) {
            try {
                accepted.set_exception(std::current_exception());
            } catch (...) {
            }
        }
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.poolEnabled = true;
    opts.maxInflight = 2;
    opts.requestTimeout = 10s;
    opts.headerTimeout = 60s;
    opts.bodyTimeout = 60s;

    AsioTransportAdapter adapter(opts);

    // Launch two requests that will occupy the in-flight slots (server never responds).
    const Request req{StatusRequest{}};
    auto fut1 = boost::asio::co_spawn(GlobalIOContext::global_executor(), adapter.send_request(req),
                                      boost::asio::use_future);

    REQUIRE(acceptedFuture.wait_for(2s) == std::future_status::ready);
    REQUIRE_NOTHROW(acceptedFuture.get());

    // Give request #1 time to connect + write and release the connection for reuse.
    std::this_thread::sleep_for(50ms);
    auto fut2 = boost::asio::co_spawn(GlobalIOContext::global_executor(), adapter.send_request(req),
                                      boost::asio::use_future);
    std::this_thread::sleep_for(50ms);

    // Third request should fail immediately with ResourceExhausted due to maxInflight.
    auto fut3 = boost::asio::co_spawn(GlobalIOContext::global_executor(), adapter.send_request(req),
                                      boost::asio::use_future);
    REQUIRE(fut3.wait_for(1s) == std::future_status::ready);
    auto r3 = fut3.get();
    REQUIRE_FALSE(r3.has_value());
    REQUIRE(r3.error().code == ErrorCode::ResourceExhausted);

    // Unblock the first two requests by closing the server-side socket.
    stop.store(true, std::memory_order_relaxed);
    server_thread.join();

    REQUIRE(fut1.wait_for(2s) == std::future_status::ready);
    REQUIRE(fut2.wait_for(2s) == std::future_status::ready);
    auto r1 = fut1.get();
    auto r2 = fut2.get();
    REQUIRE_FALSE(r1.has_value());
    REQUIRE_FALSE(r2.has_value());

    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}
