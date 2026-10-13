// SPDX-License-Identifier: GPL-3.0-or-later
#include "../../common/service_manager_test_helper.h"
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <spdlog/spdlog.h>
#include <yams/daemon/components/IOCoordinator.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/daemon_lifecycle.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/metadata/metadata_repository.h>

#include <boost/asio.hpp>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#ifndef _WIN32
#include <sys/un.h>
#include <yams/daemon/components/SocketServer.h>

using namespace yams::daemon;
using namespace std::chrono_literals;

namespace {
// Only the shutdown callback is controlled. Readiness is the real FSM/core-service state;
// this adapter never shuts down a host daemon or fabricates a Ready snapshot.
class ObservedLifecycle final : public IDaemonLifecycle {
public:
    explicit ObservedLifecycle(DaemonLifecycleFsm& fsm) : fsm_(fsm) {}
    LifecycleSnapshot getLifecycleSnapshot() const override { return fsm_.snapshot(); }
    void setSubsystemDegraded(const std::string& name, bool degraded,
                              const std::string& reason) override {
        fsm_.setSubsystemDegraded(name, degraded, reason);
    }
    void onDocumentRemoved(const std::string&) override {}
    void requestShutdown(bool graceful, bool) override {
        std::unique_lock lock(mutex_);
        ++calls_;
        graceful_ = graceful;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_; });
    }
    bool waitForEntry() {
        std::unique_lock lock(mutex_);
        return cv_.wait_for(lock, 2s, [&] { return calls_ == 1; });
    }
    bool graceful() const {
        std::lock_guard lock(mutex_);
        return graceful_;
    }
    void release() {
        std::lock_guard lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

private:
    DaemonLifecycleFsm& fsm_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    unsigned calls_{0};
    bool graceful_{false};
    bool released_{false};
};

struct ProxyActivityFixture {
    struct LogGuard {
        spdlog::level::level_enum previous{spdlog::get_level()};
        LogGuard() { spdlog::set_level(spdlog::level::warn); }
        ~LogGuard() { spdlog::set_level(previous); }
    } logGuard;
    yams::test::TempDirGuard directory{"spa_"};
    const std::shared_ptr<const TuningSnapshot> savedSnapshot{
        TuningSnapshotRegistry::instance().get()};
    DaemonConfig config;
    StateComponent state;
    DaemonLifecycleFsm lifecycle;
    ObservedLifecycle callback{lifecycle};
    std::shared_ptr<ServiceManager> services;
    std::unique_ptr<RequestDispatcher> dispatcher;
    std::unique_ptr<IOCoordinator> io;
    std::unique_ptr<SocketServer> server;

    // Buffers outlive clientIo, including canceled pending reads on failed REQUIREs.
    std::vector<uint8_t> requestFrame;
    std::array<uint8_t, MessageFramer::HEADER_SIZE> replyHeader{};
    std::vector<uint8_t> replyFrame;
    boost::asio::io_context clientIo;
    boost::asio::local::stream_protocol::socket client{clientIo};
    MessageFramer framer;

    ProxyActivityFixture() {
        config.dataDir = directory.path();
        config.socketPath = directory.path() / "d.sock";
        config.pidFile = directory.path() / "daemon.pid";
        config.logFile = directory.path() / "daemon.log";
        config.enableModelProvider = false;
        config.modelProviderRequired = false;
        config.useMockModelProvider = false;
        config.autoLoadPlugins = false;
        config.enableAutoRepair = false;
        config.autoRebuildOnDimMismatch = false;
        config.embeddedOneShot = true;
        config.searchMaintenance.automaticRebuildsEnabled = false;
        config.searchMaintenance.automaticRebuildsSource = "typed:test";
        config.memorySync.enabled = false;
        config.downloadPolicy.enable = false;
    }
    ~ProxyActivityFixture() {
        // The proxy path awaits unary dispatch inline on the connection's I/O context.
        // Release before stop/join even if REQUIRE failed while that I/O thread was blocked.
        callback.release();
        closeClient();
        teardownSocketServer(server, io);
        // All proxy connection/request frames are gone before borrowed dispatch/executors.
        dispatcher.reset();
        if (services)
            services->shutdown();
        services.reset();
        TuningSnapshotRegistry::instance().set(savedSnapshot);
    }
    void closeClient() {
        boost::system::error_code ignored;
        client.close(ignored);
    }
    template <typename Predicate> bool observe(Predicate predicate) {
        // Public occupancy/activity atomics have no subscription. This tight bounded
        // observation is a correctness deadline, not a latency/notification measurement.
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        do {
            if (predicate())
                return true;
            std::this_thread::sleep_for(10ms);
        } while (std::chrono::steady_clock::now() < deadline);
        return predicate();
    }
    template <typename T> T complete(std::future<T>& future) {
        clientIo.run_for(2s);
        REQUIRE(future.wait_for(0ms) == std::future_status::ready);
        return future.get();
    }
    void initialize() {
        // Initialization is outside the constructor: failure still invokes release/teardown.
        services = std::make_shared<ServiceManager>(config, state, lifecycle);
        REQUIRE(yams::test::initializeServiceManagerFully(services, 30s, false));
        REQUIRE(state.readiness.contentStoreReady.load());
        REQUIRE(state.readiness.metadataRepoReady.load());
        auto repo = services->getMetadataRepo();
        REQUIRE(repo != nullptr);
        // Real repository query; unavailable FTS5 can legitimately return an empty set.
        REQUIRE(repo->getFts5IndexedRowIdSet().has_value());
        auto* coordinator = services->getWorkCoordinator();
        REQUIRE(coordinator != nullptr);
        REQUIRE(coordinator->isRunning());
        REQUIRE(coordinator->getWorkerCount() >= 2);
        dispatcher = std::make_unique<RequestDispatcher>(&callback, services.get(), &state);
        IOCoordinator::Config ioConfig;
        ioConfig.num_threads = 1;
        io = std::make_unique<IOCoordinator>(ioConfig);
        SocketServer::Config socketConfig;
        socketConfig.socketPath = config.socketPath;
        socketConfig.proxySocketPath = directory.path() / "p.sock";
        socketConfig.connectionTimeout = 5s;
        REQUIRE(socketConfig.proxySocketPath.string().size() < sizeof(sockaddr_un{}.sun_path));
        server = std::make_unique<SocketServer>(socketConfig, io.get(), coordinator,
                                                dispatcher.get(), &state);
        const auto started = server->start();
        REQUIRE(started);
        io->start();
        boost::system::error_code error;
        client.connect(
            boost::asio::local::stream_protocol::endpoint{socketConfig.proxySocketPath.string()},
            error);
        REQUIRE_FALSE(error);
        REQUIRE(observe([&] {
            return server->totalConnections() == 1 && server->activeConnections() == 1 &&
                   state.stats.activeConnections.load() == 1;
        }));
        REQUIRE(state.stats.healthCheckConnections.load() == 0);
        REQUIRE(state.stats.activeNonHealthRequests.load() == 0);
    }
    void sendShutdown() {
        Message message{};
        message.requestId = 701;
        message.timestamp = std::chrono::steady_clock::now();
        message.payload = Request{ShutdownRequest{}};
        const auto framed = framer.frame_message_into(message, requestFrame);
        REQUIRE(framed);
        clientIo.restart();
        auto sent = boost::asio::async_write(client, boost::asio::buffer(requestFrame),
                                             boost::asio::use_future);
        REQUIRE(complete(sent) == requestFrame.size());
        // Positive control: actual framing, acceptance, readiness and dispatch reached the
        // downstream public callback. A provider/ABI/transport failure is not behavioral RED.
        REQUIRE(callback.waitForEntry());
        REQUIRE(callback.graceful());
    }
    void readSuccess() {
        clientIo.restart();
        auto headerRead = boost::asio::async_read(client, boost::asio::buffer(replyHeader),
                                                  boost::asio::use_future);
        REQUIRE(complete(headerRead) == replyHeader.size());
        const auto header = framer.parse_header(std::span<const uint8_t>{replyHeader});
        REQUIRE(header);
        REQUIRE_FALSE(header.value().is_chunked());
        REQUIRE_FALSE(header.value().is_error());
        REQUIRE(header.value().payload_size > 0);
        REQUIRE(header.value().payload_size <= 4096);
        replyFrame.assign(replyHeader.begin(), replyHeader.end());
        replyFrame.resize(replyHeader.size() + header.value().payload_size);
        clientIo.restart();
        auto bodyRead =
            boost::asio::async_read(client,
                                    boost::asio::buffer(replyFrame.data() + replyHeader.size(),
                                                        header.value().payload_size),
                                    boost::asio::use_future);
        REQUIRE(complete(bodyRead) == header.value().payload_size);
        const auto parsed = framer.parse_frame(replyFrame);
        REQUIRE(parsed);
        REQUIRE(parsed.value().requestId == 701);
        const auto* response = std::get_if<Response>(&parsed.value().payload);
        REQUIRE(response != nullptr);
        REQUIRE(std::holds_alternative<SuccessResponse>(*response));
    }
};
} // namespace

TEST_CASE_METHOD(ProxyActivityFixture,
                 "SocketServer proxy wires executing requests and balances completion/disconnect",
                 "[daemon][socket_server][proxy][activity][slow][regression]") {
    initialize();
    sendShutdown();
    // No seeded activity: acceptance and the real handler must publish into this same State.
    REQUIRE(state.stats.activeConnections.load() == 1);
    REQUIRE(state.stats.healthCheckConnections.load() == 0);
    REQUIRE(state.stats.activeNonHealthRequests.load() == 1);

    SECTION("successful response finishes request while the connection stays accepted") {
        callback.release();
        readSuccess();
        REQUIRE(observe([&] { return state.stats.activeNonHealthRequests.load() == 0; }));
        CHECK(state.stats.activeConnections.load() == 1);
        CHECK(server->activeConnections() == 1);
        closeClient();
    }
    SECTION("disconnect during execution still releases the request guard") {
        closeClient();
        callback.release();
    }
    REQUIRE(observe([&] {
        return server->activeConnections() == 0 && state.stats.activeConnections.load() == 0 &&
               state.stats.activeNonHealthRequests.load() == 0 &&
               state.stats.healthCheckConnections.load() == 0 &&
               state.stats.ipcTasksPending.load() == 0 && state.stats.ipcTasksActive.load() == 0;
    }));
    CHECK(server->totalConnections() == 1);
    // Proxy is deliberately serial: this does not validate detached main-socket mux lifetime,
    // overlapping health requests, continuous tuning, cancellation latency or CPU idleness.
}
#endif
