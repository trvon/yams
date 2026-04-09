#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include <yams/compat/unistd.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/daemon.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

bool isSocketPermissionDenied(const yams::Error& error) {
    const std::string_view message{error.message};
    return message.find("Operation not permitted") != std::string_view::npos ||
           message.find("Permission denied") != std::string_view::npos;
}

bool waitFor(const std::function<bool()>& predicate, std::chrono::milliseconds timeout,
             std::chrono::milliseconds step = 25ms) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(step);
    }
    return predicate();
}

} // namespace

TEST_CASE("DaemonBench: idle control wakeup remains responsive", "[daemon][.bench][.slow]") {
    const char* enable = ::getenv("YAMS_ENABLE_DAEMON_IDLE_BENCH");
    if (!enable || std::string_view(enable) != "1") {
        SKIP("Bench disabled (set YAMS_ENABLE_DAEMON_IDLE_BENCH=1 to run)");
    }

    fs::path runtimeRoot =
        fs::temp_directory_path() / ("ydidle_" + std::to_string(::getpid()) + "_" +
                                     std::to_string(static_cast<unsigned long>(
                                         reinterpret_cast<uintptr_t>(&runtimeRoot) & 0xffff)));

    yams::daemon::DaemonConfig cfg;
    cfg.dataDir = runtimeRoot / "data";
    cfg.socketPath = runtimeRoot / "sock";
    cfg.pidFile = runtimeRoot / "daemon.pid";
    cfg.logFile = runtimeRoot / "daemon.log";
    cfg.maxMemoryGb = 1;
    cfg.enableModelProvider = false;
    cfg.autoLoadPlugins = false;

    ::setenv("YAMS_DB_OPEN_TIMEOUT_MS", "1000", 1);
    ::setenv("YAMS_DB_MIGRATE_TIMEOUT_MS", "1500", 1);
    ::setenv("YAMS_SEARCH_BUILD_TIMEOUT_MS", "1000", 1);
    ::setenv("YAMS_DISABLE_VECTORS", "1", 1);

    std::error_code se;
    fs::create_directories(cfg.dataDir, se);

    std::unique_ptr<yams::daemon::YamsDaemon> daemon =
        std::make_unique<yams::daemon::YamsDaemon>(cfg);
    auto start = daemon->start();
    if (!start) {
        if (isSocketPermissionDenied(start.error())) {
            SKIP("UNIX domain sockets not permitted: " + start.error().message);
        }
        FAIL("Failed to start daemon: " + start.error().message);
    }

    const bool metadataReady =
        waitFor([&]() { return daemon->getState().readiness.metadataRepoReady.load(); }, 20s, 50ms);
    if (!metadataReady) {
        daemon->stop();
        SKIP("Daemon did not reach metadata-ready state within benchmark window");
    }

    const bool reachedIdle = waitFor(
        []() {
            auto snap = yams::daemon::TuningSnapshotRegistry::instance().get();
            return snap && snap->daemonIdle;
        },
        20s, 50ms);
    if (!reachedIdle) {
        daemon->stop();
        SKIP("Daemon did not reach idle state within benchmark window");
    }

    auto channel =
        yams::daemon::InternalEventBus::instance()
            .get_or_create_channel<yams::daemon::InternalEventBus::StoreDocumentTask>(
                "store_document_tasks", yams::daemon::TuneAdvisor::storeDocumentChannelCapacity());

    yams::daemon::InternalEventBus::StoreDocumentTask task;
    task.request.name = "idle-control-bench.txt";
    task.request.content = "idle wake benchmark\n";
    task.request.noEmbeddings = true;
    REQUIRE(channel->try_push(std::move(task)));

    const auto wakeStart = std::chrono::steady_clock::now();
    yams::daemon::TuningManager::notifyWakeup();
    const bool wokeActive = waitFor(
        []() {
            auto snap = yams::daemon::TuningSnapshotRegistry::instance().get();
            return snap && !snap->daemonIdle;
        },
        2s, 5ms);
    REQUIRE(wokeActive);

    const auto wakeMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - wakeStart)
                            .count();
    INFO("Controller wake latency: " << wakeMs << "ms");
    REQUIRE(wakeMs < 500);

    const auto drainStart = std::chrono::steady_clock::now();
    const bool returnedIdle = waitFor(
        []() {
            auto snap = yams::daemon::TuningSnapshotRegistry::instance().get();
            return snap && snap->daemonIdle;
        },
        15s);
    REQUIRE(returnedIdle);

    const auto drainMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - drainStart)
                             .count();
    INFO("Controller return-to-idle latency: " << drainMs << "ms");

    daemon->stop();

    std::error_code ec;
    fs::remove_all(runtimeRoot, ec);
}
