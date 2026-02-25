// AsioConnectionPool unit tests for stale socket detection

#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <thread>

#include "../../common/test_helpers_catch2.h"
#include <yams/compat/unistd.h>
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/client/ipc_failure.h>

#ifndef _WIN32
#include <sys/socket.h>
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
    auto conn1_res = fut1.get();
    REQUIRE(conn1_res);
    auto conn1 = conn1_res.value();
    REQUIRE(conn1);
    REQUIRE(accepted1.get_future().wait_for(1s) == std::future_status::ready);
    pool->release(conn1);

    close1.set_value();
    std::this_thread::sleep_for(50ms);

    auto fut2 = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                      boost::asio::use_future);
    REQUIRE(fut2.wait_for(1s) == std::future_status::ready);
    auto conn2_res = fut2.get();
    REQUIRE(conn2_res);
    auto conn2 = conn2_res.value();
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

TEST_CASE("AsioConnectionPool handles server idle close", "[daemon][connection-pool][unit]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    // This test simulates the daemon's idle timeout behavior:
    // Server closes connection after short idle period, client should detect this
    // and create a new connection on next acquire()

    auto runtimeDir = makeTempRuntimeDir("idle-close-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::atomic<int> connections_accepted{0};
    std::promise<void> first_accepted;
    std::promise<void> second_accepted;
    std::atomic<bool> stop{false};

    // Server that closes connections after 100ms of idle
    std::thread server_thread([&] {
        boost::system::error_code bec;

        // Accept first connection
        boost::asio::local::stream_protocol::socket sock1(server_io);
        acceptor.accept(sock1, bec);
        if (!bec) {
            connections_accepted.fetch_add(1);
            first_accepted.set_value();
            // Simulate server idle timeout: close after 100ms
            std::this_thread::sleep_for(100ms);
            sock1.close(bec);
        }

        // Accept second connection
        if (!stop.load()) {
            boost::asio::local::stream_protocol::socket sock2(server_io);
            acceptor.accept(sock2, bec);
            if (!bec) {
                connections_accepted.fetch_add(1);
                second_accepted.set_value();
                // Keep socket open until test completes
                while (!stop.load()) {
                    std::this_thread::sleep_for(10ms);
                }
                sock2.close(bec);
            }
        }
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.requestTimeout = 50ms; // Very short timeout
    opts.headerTimeout = 200ms; // Short header timeout
    opts.poolEnabled = true;

    auto pool = AsioConnectionPool::get_or_create(opts);

    // First connection
    auto fut1 = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                      boost::asio::use_future);
    REQUIRE(fut1.wait_for(2s) == std::future_status::ready);
    auto conn1_res = fut1.get();
    REQUIRE(conn1_res);
    auto conn1 = conn1_res.value();
    REQUIRE(conn1);
    REQUIRE(first_accepted.get_future().wait_for(1s) == std::future_status::ready);
    REQUIRE(connections_accepted.load() == 1);
    pool->release(conn1);

    // Wait longer than server idle timeout
    std::this_thread::sleep_for(200ms);

    // Second connection - should detect stale and create new
    auto fut2 = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                      boost::asio::use_future);
    REQUIRE(fut2.wait_for(2s) == std::future_status::ready);
    auto conn2_res = fut2.get();
    REQUIRE(conn2_res);
    auto conn2 = conn2_res.value();
    REQUIRE(conn2);
    REQUIRE(second_accepted.get_future().wait_for(1s) == std::future_status::ready);
    // Should be a different connection (stale one was detected and replaced)
    REQUIRE(connections_accepted.load() == 2);
    pool->release(conn2);

    stop.store(true);
    boost::system::error_code bec;
    acceptor.close(bec);
    server_thread.join();
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}

TEST_CASE("AsioConnectionPool handles EOF during read", "[daemon][connection-pool][unit]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    // This test simulates the case where server closes connection
    // while client is in the middle of a read loop

    auto runtimeDir = makeTempRuntimeDir("eof-read-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::atomic<bool> stop{false};
    std::promise<void> connected;
    std::promise<void> close_now;

    std::thread server_thread([&] {
        boost::system::error_code bec;
        boost::asio::local::stream_protocol::socket sock(server_io);
        acceptor.accept(sock);
        connected.set_value();

        // Wait for signal to close
        close_now.get_future().wait();

        // Close without sending anything - simulates sudden disconnect
        sock.close(bec);
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.requestTimeout = 50ms;
    opts.headerTimeout = 100ms;
    opts.bodyTimeout = 100ms;
    opts.poolEnabled = true;

    auto pool = AsioConnectionPool::get_or_create(opts);
    auto fut = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                     boost::asio::use_future);
    REQUIRE(fut.wait_for(1s) == std::future_status::ready);
    auto conn_res = fut.get();
    REQUIRE(conn_res);
    auto conn = conn_res.value();
    REQUIRE(conn);
    REQUIRE(connected.get_future().wait_for(1s) == std::future_status::ready);

    // Start the read loop on the connection
    auto readLoopFut =
        boost::asio::co_spawn(GlobalIOContext::global_executor(),
                              pool->ensure_read_loop_started(conn), boost::asio::use_future);

    // Wait for read loop to start
    std::this_thread::sleep_for(50ms);

    // Signal server to close the connection
    close_now.set_value();

    // Wait a bit for the read loop to detect EOF
    std::this_thread::sleep_for(200ms);

    // Connection should be marked as dead
    CHECK_FALSE(conn->alive.load());

    pool->release(conn);
    stop.store(true);
    boost::system::error_code bec;
    acceptor.close(bec);
    server_thread.join();
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}

TEST_CASE("AsioConnectionPool handles read timeout", "[daemon][connection-pool][unit]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("read-timeout-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::atomic<bool> stop{false};
    std::promise<void> connected;

    std::thread server_thread([&] {
        boost::system::error_code bec;
        boost::asio::local::stream_protocol::socket sock(server_io);
        acceptor.accept(sock);
        connected.set_value();
        while (!stop.load()) {
            std::this_thread::sleep_for(10ms);
        }
        sock.close(bec);
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.requestTimeout = 50ms;
    opts.headerTimeout = 50ms;
    opts.bodyTimeout = 50ms;
    opts.poolEnabled = true;

    auto pool = AsioConnectionPool::get_or_create(opts);
    auto fut = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                     boost::asio::use_future);
    REQUIRE(fut.wait_for(1s) == std::future_status::ready);
    auto conn_res = fut.get();
    REQUIRE(conn_res);
    auto conn = conn_res.value();
    REQUIRE(conn);
    REQUIRE(connected.get_future().wait_for(1s) == std::future_status::ready);

    auto readLoopFut =
        boost::asio::co_spawn(GlobalIOContext::global_executor(),
                              pool->ensure_read_loop_started(conn), boost::asio::use_future);
    REQUIRE(readLoopFut.wait_for(1s) == std::future_status::ready);

    std::this_thread::sleep_for(200ms);
    CHECK_FALSE(conn->alive.load());

    pool->release(conn);
    stop.store(true);
    boost::system::error_code bec;
    acceptor.close(bec);
    server_thread.join();
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}

TEST_CASE("Non-shared pool connection stays alive via pool_keepalive",
          "[daemon][connection-pool][unit]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    // This test verifies that when poolEnabled=false (non-shared pool),
    // the pool_keepalive reference in the connection prevents the pool
    // from being destroyed prematurely.

    auto runtimeDir = makeTempRuntimeDir("non-shared-pool-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::atomic<bool> stop{false};
    std::promise<void> connected;

    std::thread server_thread([&] {
        boost::system::error_code bec;
        boost::asio::local::stream_protocol::socket sock(server_io);
        acceptor.accept(sock);
        connected.set_value();

        // Keep connection open until test completes
        while (!stop.load()) {
            std::this_thread::sleep_for(10ms);
        }
        sock.close(bec);
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.requestTimeout = 500ms;
    opts.poolEnabled = false; // Non-shared pool - this is the key setting

    // Create pool and acquire connection in a scope that lets pool go out of scope
    std::shared_ptr<AsioConnection> conn;
    {
        auto pool = AsioConnectionPool::get_or_create(opts);
        REQUIRE_FALSE(pool->is_shared()); // Verify it's a non-shared pool

        auto fut = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                         boost::asio::use_future);
        REQUIRE(fut.wait_for(2s) == std::future_status::ready);
        auto conn_res = fut.get();
        REQUIRE(conn_res);
        conn = conn_res.value();
        REQUIRE(conn);
        REQUIRE(connected.get_future().wait_for(1s) == std::future_status::ready);

        // Verify pool_keepalive is set for non-shared pools
        REQUIRE(conn->pool_keepalive != nullptr);

        // Pool goes out of scope here, but connection should still be alive
        // because pool_keepalive holds a reference
    }

    // Connection should still be alive after pool variable went out of scope
    REQUIRE(conn->alive.load());
    REQUIRE(conn->socket);
    REQUIRE(conn->socket->is_open());

    // Clean up
    conn->pool_keepalive.reset(); // Release the keepalive reference
    conn.reset();

    stop.store(true);
    boost::system::error_code bec;
    acceptor.close(bec);
    server_thread.join();
    fs::remove(socketPath, ec);
}

TEST_CASE("Shared pool does not set pool_keepalive", "[daemon][connection-pool][unit]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    // Shared pools are stored in the global registry, so they don't need
    // the keepalive mechanism. Verify pool_keepalive is not set.

    auto runtimeDir = makeTempRuntimeDir("shared-pool-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::atomic<bool> stop{false};
    std::promise<void> connected;

    std::thread server_thread([&] {
        boost::system::error_code bec;
        boost::asio::local::stream_protocol::socket sock(server_io);
        acceptor.accept(sock);
        connected.set_value();

        while (!stop.load()) {
            std::this_thread::sleep_for(10ms);
        }
        sock.close(bec);
    });

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.requestTimeout = 500ms;
    opts.poolEnabled = true; // Shared pool

    auto pool = AsioConnectionPool::get_or_create(opts);
    REQUIRE(pool->is_shared()); // Verify it's a shared pool

    auto fut = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                     boost::asio::use_future);
    REQUIRE(fut.wait_for(2s) == std::future_status::ready);
    auto conn_res = fut.get();
    REQUIRE(conn_res);
    auto conn = conn_res.value();
    REQUIRE(conn);
    REQUIRE(connected.get_future().wait_for(1s) == std::future_status::ready);

    // Shared pools should NOT set pool_keepalive (they're in the registry)
    REQUIRE(conn->pool_keepalive == nullptr);

    pool->release(conn);
    stop.store(true);
    boost::system::error_code bec;
    acceptor.close(bec);
    server_thread.join();
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}

TEST_CASE("AsioConnectionPool does not double-wrap IPC failure prefixes",
          "[daemon][connection-pool][unit][ipc]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("ipc-prefix-refused-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

#ifndef _WIN32
    // Create a stale AF_UNIX socket path (bound but not listening) to induce ECONNREFUSED.
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    const auto sockStr = socketPath.string();
    REQUIRE(sockStr.size() < sizeof(addr.sun_path));
    std::strncpy(addr.sun_path, sockStr.c_str(), sizeof(addr.sun_path) - 1);

    REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    REQUIRE(::close(fd) == 0);
#endif

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.poolEnabled = false;
    opts.requestTimeout = 500ms;

    auto pool = AsioConnectionPool::get_or_create(opts);
    auto fut = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                     boost::asio::use_future);
    REQUIRE(fut.wait_for(2s) == std::future_status::ready);
    auto conn_res = fut.get();
    REQUIRE_FALSE(conn_res);

    const auto& msg = conn_res.error().message;
    REQUIRE(msg.rfind(std::string(kIpcFailurePrefix), 0) == 0);
    REQUIRE(parseIpcFailureKind(msg).has_value());
    CHECK(msg.find(std::string(kIpcFailurePrefix), 1) == std::string::npos);

    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}

TEST_CASE("AsioConnectionPool shutdown short-circuits in one-shot CLI mode",
          "[daemon][connection-pool][unit][lifecycle]") {
#ifdef _WIN32
    SKIP("Unix domain socket tests skipped on Windows");
#endif

    auto runtimeDir = makeTempRuntimeDir("one-shot-shutdown-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";
    std::error_code ec;
    fs::remove(socketPath, ec);

    boost::asio::io_context server_io;
    boost::asio::local::stream_protocol::acceptor acceptor(
        server_io, boost::asio::local::stream_protocol::endpoint(socketPath.string()));

    std::atomic<bool> stop{false};
    std::promise<void> connected;
    std::thread server_thread([&] {
        boost::system::error_code bec;
        boost::asio::local::stream_protocol::socket sock(server_io);
        acceptor.accept(sock, bec);
        if (!bec) {
            connected.set_value();
            while (!stop.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(10ms);
            }
            sock.close(bec);
        }
    });

    yams::test::ScopedEnvVar oneShot("YAMS_CLI_ONE_SHOT", std::string("1"));

    TransportOptions opts;
    opts.socketPath = socketPath;
    opts.requestTimeout = 500ms;
    opts.poolEnabled = true;

    auto pool = AsioConnectionPool::get_or_create(opts);
    auto fut = boost::asio::co_spawn(GlobalIOContext::global_executor(), pool->acquire(),
                                     boost::asio::use_future);
    REQUIRE(fut.wait_for(2s) == std::future_status::ready);
    auto conn_res = fut.get();
    REQUIRE(conn_res);
    auto conn = conn_res.value();
    REQUIRE(conn);
    REQUIRE(connected.get_future().wait_for(1s) == std::future_status::ready);

    // Inject a valid but unresolved future to emulate a stuck read loop.
    std::promise<void> pendingReadLoop;
    conn->read_loop_future = pendingReadLoop.get_future();
    pool->release(conn);

    const auto start = std::chrono::steady_clock::now();
    pool->shutdown(500ms);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    CHECK(elapsed < 150ms);

    pendingReadLoop.set_value();
    stop.store(true, std::memory_order_release);
    boost::system::error_code bec;
    acceptor.close(bec);
    server_thread.join();
    fs::remove(socketPath, ec);
    AsioConnectionPool::shutdown_all(100ms);
}
