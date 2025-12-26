// AsioConnectionPool unit tests for stale socket detection

#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <filesystem>
#include <future>
#include <thread>

#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/global_io_context.h>

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
    auto candidate = base / "yams-connection-pool-tests" / name / "ipc.sock";
    if (candidate.native().size() >= maxUnixPath) {
        base = fs::path("/tmp");
    }
#endif
    auto dir = base / "yams-connection-pool-tests" / name;
    std::error_code ec;
    fs::create_directories(dir, ec);
    return dir;
}

std::string randomSuffix() {
    return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

} // namespace

TEST_CASE("AsioConnectionPool drops closed socket on reuse", "[daemon][connection-pool][unit]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("stale-socket-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::promise<void> accepted1;
    std::promise<void> accepted2;
    std::promise<void> close1;
    std::atomic<bool> stop{false};

    std::thread server_thread([&] {
        boost::system::error_code bec;
        boost::asio::local::stream_protocol::socket sock1(server_io);
        acceptor.accept(sock1);
        accepted1.set_value();
        close1.get_future().wait();
        sock1.close(bec);

        boost::asio::local::stream_protocol::socket sock2(server_io);
        acceptor.accept(sock2);
        accepted2.set_value();
        while (!stop.load()) {
            std::this_thread::sleep_for(10ms);
        }
        sock2.close(bec);
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.requestTimeout = 500ms;
    opts.poolEnabled = true;

    auto pool = AsioConnectionPool::get_or_create(opts);
    auto fut1 = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                      boost::asio::use_future);
    REQUIRE(fut1.wait_for(1s) == std::future_status::ready);
    auto conn1 = fut1.get();
    REQUIRE(conn1);
    REQUIRE(accepted1.get_future().wait_for(1s) == std::future_status::ready);
    pool->release(conn1);

    close1.set_value();
    std::this_thread::sleep_for(50ms);

    auto fut2 = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                      boost::asio::use_future);
    REQUIRE(fut2.wait_for(1s) == std::future_status::ready);
    auto conn2 = fut2.get();
    REQUIRE(conn2);
    REQUIRE(accepted2.get_future().wait_for(1s) == std::future_status::ready);
    REQUIRE(conn1.get() != conn2.get());
    pool->release(conn2);

    stop.store(true);
    server_thread.join();
    boost::system::error_code bec;
    acceptor.close(bec);
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}
