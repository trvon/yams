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
