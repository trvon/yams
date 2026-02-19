// AsioTransportAdapter unit tests for IPC failure prefix preservation

#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <filesystem>
#include <future>
#include <thread>

#include <yams/core/types.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/client/ipc_failure.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#ifndef _WIN32
#include <sys/un.h>
#endif

using namespace yams::daemon;
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

bool hasIpcPrefix(const std::string& message) {
    return message.rfind(std::string(kIpcFailurePrefix), 0) == 0;
}

} // namespace

TEST_CASE("AsioTransportAdapter preserves [ipc:*] prefix when server closes with inflight requests",
          "[daemon][transport][unit][ipc]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("inflight-close-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::promise<void> accepted;
    auto acceptedFuture = accepted.get_future();
    std::promise<void> closeNow;

    std::thread server_thread([&] {
        boost::system::error_code bec;
        boost::asio::local::stream_protocol::socket sock(server_io);
        acceptor.accept(sock, bec);
        if (bec) {
            accepted.set_exception(
                std::make_exception_ptr(std::runtime_error("accept failed: " + bec.message())));
            return;
        }

        // Only allow a single client connection to ensure all requests share the same underlying
        // socket/handlers map.
        acceptor.close(bec);

        accepted.set_value();
        closeNow.get_future().wait();

        // Close without sending any response frames. This should surface as EOF in the client's
        // read loop and must preserve the [ipc:*] error prefix.
        sock.close(bec);
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.poolEnabled = true;
    opts.maxInflight = 8;
    opts.requestTimeout = 10s;
    opts.headerTimeout = 2s;
    opts.bodyTimeout = 2s;

    AsioTransportAdapter adapter(opts);

    const Request req{StatusRequest{}};
    constexpr int kNumInflight = 4;
    std::vector<std::future<yams::Result<Response>>> futures;
    futures.reserve(kNumInflight);

    for (int i = 0; i < kNumInflight; ++i) {
        futures.push_back(boost::asio::co_spawn(GlobalIOContext::global_executor(),
                                                adapter.send_request(req),
                                                boost::asio::use_future));
        if (i == 0) {
            REQUIRE(acceptedFuture.wait_for(2s) == std::future_status::ready);
            REQUIRE_NOTHROW(acceptedFuture.get());
        }
        // Stagger starts so each request can connect/write and release the connection for reuse.
        std::this_thread::sleep_for(50ms);
    }

    closeNow.set_value();
    server_thread.join();

    bool sawEof = false;
    for (auto& f : futures) {
        REQUIRE(f.wait_for(3s) == std::future_status::ready);
        auto r = f.get();
        REQUIRE_FALSE(r.has_value());
        REQUIRE(hasIpcPrefix(r.error().message));
        auto kind = parseIpcFailureKind(r.error().message);
        REQUIRE(kind.has_value());
        if (*kind == IpcFailureKind::Eof) {
            sawEof = true;
        }
    }
    REQUIRE(sawEof);

    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}
