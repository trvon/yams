// SocketServer + IOCoordinator teardown ordering.
//
// Connection handlers are coroutines on the IOCoordinator io_context and hold raw pointers into
// the SocketServer that spawned them (cleanup guards) and into its proxy slot semaphore.
// SocketServer::stop() only waits a bounded time for them to drain, so the teardown must keep
// those alive until every handler frame is gone. Destroying the server before the io_context
// released its pending handler frames was an ASAN heap-use-after-free.

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/IOCoordinator.h>
#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WorkCoordinator.h>

#include <boost/asio/post.hpp>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <latch>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif

using namespace yams::daemon;
using namespace std::chrono_literals;

#ifndef _WIN32
namespace {

std::filesystem::path makeScratchDir() {
    std::random_device rd;
    const auto dir = std::filesystem::temp_directory_path() /
                     ("yams_sst_" + std::to_string(rd()) + std::to_string(rd()));
    std::filesystem::create_directories(dir);
    return dir;
}

int connectRawUnixSocket(const std::filesystem::path& socketPath) {
    const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    const auto pathString = socketPath.string();
    if (pathString.size() >= sizeof(addr.sun_path)) {
        ::close(fd);
        return -1;
    }
    std::memcpy(addr.sun_path, pathString.data(), pathString.size());
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

template <typename Pred> bool waitFor(std::chrono::milliseconds timeout, Pred pred) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

struct ScratchDirGuard {
    std::filesystem::path dir;
    ~ScratchDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
};

} // namespace

TEST_CASE("SocketServer actual acceptance publishes and drains connection occupancy",
          "[daemon][socket_server][activity][acceptor-control][catch2]") {
    ScratchDirGuard scratch{makeScratchDir()};
    WorkCoordinator work;
    StateComponent state;
    IOCoordinator::Config ioConfig;
    ioConfig.num_threads = 1;
    auto io = std::make_unique<IOCoordinator>(ioConfig);
    SocketServer::Config config;
    config.socketPath = scratch.dir / "a.sock";
    auto server = std::make_unique<SocketServer>(config, io.get(), &work, nullptr, &state);
    struct TeardownGuard {
        std::unique_ptr<SocketServer>& server;
        std::unique_ptr<IOCoordinator>& io;
        WorkCoordinator& work;
        ~TeardownGuard() {
            teardownSocketServer(server, io);
            work.stop();
            work.join();
        }
    } teardown{server, io, work};

    work.start(2);
    REQUIRE(server->start());
    io->start();
    struct ClientGuard {
        int fd;
        ~ClientGuard() {
            if (fd >= 0)
                ::close(fd);
        }
        void close() {
            if (fd >= 0) {
                ::close(fd);
                fd = -1;
            }
        }
    } client{connectRawUnixSocket(config.socketPath)};
    REQUIRE(client.fd >= 0);

    // Public atomics expose no acceptance callback. Bounded 10ms observation uses the existing
    // helper; unlike seeded occupancy, this requires the real private-path accept loop to run.
    REQUIRE(waitFor(1s, [&] {
        return server->totalConnections() == 1 && server->activeConnections() == 1 &&
               state.stats.activeConnections.load(std::memory_order_relaxed) == 1;
    }));
    CHECK(state.stats.healthCheckConnections.load(std::memory_order_relaxed) == 0);
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 0);

    client.close();
    REQUIRE(waitFor(1s, [&] {
        return server->activeConnections() == 0 &&
               state.stats.activeConnections.load(std::memory_order_relaxed) == 0 &&
               state.stats.ipcTasksPending.load(std::memory_order_relaxed) == 0 &&
               state.stats.ipcTasksActive.load(std::memory_order_relaxed) == 0;
    }));
    CHECK(server->totalConnections() == 1);
    // No request was sent: this is an acceptance/cleanup control, not evidence that the server
    // wired executing-request counters or delivered a tuning notification.
    CHECK(state.stats.healthCheckConnections.load(std::memory_order_relaxed) == 0);
    CHECK(state.stats.activeNonHealthRequests.load(std::memory_order_relaxed) == 0);
}

TEST_CASE("SocketServer teardown outlives connections still draining on the IO threads",
          "[daemon][socket_server][shutdown][catch2]") {
    ScratchDirGuard scratch{makeScratchDir()};

    WorkCoordinator work;
    work.start(2);

    StateComponent state;
    IOCoordinator::Config ioConfig;
    ioConfig.num_threads = 2;
    auto io = std::make_unique<IOCoordinator>(ioConfig);

    SocketServer::Config config;
    config.socketPath = scratch.dir / "d.sock";
    config.proxySocketPath = scratch.dir / "p.sock";
    auto server = std::make_unique<SocketServer>(config, io.get(), &work, nullptr, &state);
    REQUIRE(server->start());
    io->start();

    // Mix main and proxy sessions: they take different handler paths (lifetime timer vs proxy
    // connection-slot guard) and both must survive the teardown.
    constexpr std::size_t kConnections = 16;
    std::vector<int> clientFds;
    clientFds.reserve(kConnections);
    for (std::size_t i = 0; i < kConnections; ++i) {
        const int fd =
            connectRawUnixSocket((i % 2 == 0) ? config.socketPath : config.proxySocketPath);
        REQUIRE(fd >= 0);
        clientFds.push_back(fd);
    }
    auto* rawServer = server.get();
    REQUIRE(waitFor(5s, [&] { return rawServer->activeConnections() >= kConnections; }));

    // Park every I/O thread, then stop the io_context underneath them: once released, the threads
    // leave run() without executing another handler. Every connection handler (and both accept
    // loops) is therefore still pending when SocketServer::stop() gives up draining, which is the
    // state the teardown sees whenever a client outlives the drain window.
    auto& ioContext = *io->getIOContext(); // no extra owner: teardown must free it
    const auto ioThreads = static_cast<std::ptrdiff_t>(io->getThreadCount());
    REQUIRE(ioThreads > 0);
    std::latch parked(ioThreads);
    std::promise<void> release;
    auto released = release.get_future().share();
    for (std::ptrdiff_t i = 0; i < ioThreads; ++i) {
        boost::asio::post(ioContext, [&parked, released] {
            parked.count_down();
            released.wait();
        });
    }
    parked.wait();
    ioContext.stop();
    release.set_value();

    teardownSocketServer(server, io);

    CHECK(server == nullptr);
    CHECK(io == nullptr);
    CHECK(state.stats.ipcTasksPending.load() == 0);
    CHECK(state.stats.ipcTasksActive.load() == 0);

    work.stop();
    work.join();
    for (const int fd : clientFds) {
        ::close(fd);
    }
}
#endif
