// Catch2 migration of daemon_test.cpp
// Migration: yams-3s4 (daemon unit tests)
// Unit tests for YamsDaemon - creation, start/stop, signal handling, error paths

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <thread>

#include <nlohmann/json.hpp>
using nlohmann::json;

#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_index_manager.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <process.h>
#include <share.h>
using pid_t = int;
#define getpid _getpid
static int setenv(const char* name, const char* value, int overwrite) {
    return _putenv_s(name, value);
}
#else
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#endif

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace yams;
using namespace yams::daemon;

namespace yams::daemon::test {

// Test fixture for Daemon tests
struct DaemonFixture {
    DaemonConfig config_;
    std::unique_ptr<YamsDaemon> daemon_;
    fs::path runtime_root_;
    std::thread runLoopThread_;

    DaemonFixture() {
#ifdef _WIN32
        fs::path tmp = fs::temp_directory_path() / "yams_test";
#else
        fs::path tmp{"/tmp"};
#endif
        auto unique_suffix =
            std::to_string(::getpid()) + "_" +
            std::to_string(static_cast<unsigned long>(reinterpret_cast<uintptr_t>(this) & 0xffff));

        runtime_root_ = tmp / ("ydtest_" + unique_suffix);

        config_.dataDir = runtime_root_ / "data";
        config_.socketPath = runtime_root_ / "sock";
        config_.pidFile = runtime_root_ / "daemon.pid";
        config_.logFile = runtime_root_ / "daemon.log";
        config_.maxMemoryGb = 1.0;

        config_.enableModelProvider = true;
        config_.autoLoadPlugins = true;
        config_.modelPoolConfig.lazyLoading = true;
        config_.modelPoolConfig.preloadModels.clear();

        ::setenv("YAMS_DB_OPEN_TIMEOUT_MS", "1500", 1);
        ::setenv("YAMS_DB_MIGRATE_TIMEOUT_MS", "2000", 1);
        ::setenv("YAMS_SEARCH_BUILD_TIMEOUT_MS", "1500", 1);
        ::setenv("YAMS_DISABLE_VECTORS", "1", 1);
        ::setenv("YAMS_DISABLE_SESSION_WATCHER", "1", 1);

        std::error_code se;
        fs::create_directories(config_.dataDir, se);

        ::setenv("YAMS_RUNTIME_DIR", runtime_root_.string().c_str(), 1);
        ::setenv("YAMS_SOCKET_PATH", config_.socketPath.string().c_str(), 1);
        ::setenv("YAMS_PID_FILE", config_.pidFile.string().c_str(), 1);
    }

    ~DaemonFixture() {
        stopRunLoop();
        if (daemon_) {
            daemon_->stop();
            daemon_.reset();
        }
        cleanupDaemonFiles();
    }

    void startRunLoop() {
        if (!daemon_ || runLoopThread_.joinable()) {
            return;
        }
        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });
        std::this_thread::sleep_for(50ms);
    }

    void stopRunLoop() {
        if (daemon_ && daemon_->isRunning()) {
            daemon_->requestStop();
        }
        if (runLoopThread_.joinable()) {
            runLoopThread_.join();
        }
    }

    void cleanupDaemonFiles() {
        std::error_code ec;
        if (!runtime_root_.empty()) {
            fs::remove_all(runtime_root_, ec);
        }
    }

    static bool isSocketPermissionDenied(const yams::Error& error) {
        const std::string_view message{error.message};
        return message.find("Operation not permitted") != std::string_view::npos ||
               message.find("Permission denied") != std::string_view::npos;
    }

    bool waitForDaemonReady(std::chrono::milliseconds timeout = 5000ms) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto& state = daemon_->getState();
            if (state.readiness.contentStoreReady.load()) {
                return true;
            }
            std::this_thread::sleep_for(50ms);
        }
        return false;
    }
};

#ifdef _WIN32
#define SKIP_ON_WINDOWS() SKIP("Daemon IPC tests unstable on Windows")
#else
#define SKIP_ON_WINDOWS() ((void)0)
#endif

TEST_CASE_METHOD(DaemonFixture, "Daemon creation and destruction", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    REQUIRE(daemon_ != nullptr);
    REQUIRE_FALSE(daemon_->isRunning());
}

TEST_CASE_METHOD(DaemonFixture, "Daemon start and stop", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto startResult = daemon_->start();
    if (!startResult && isSocketPermissionDenied(startResult.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(startResult);
    REQUIRE(daemon_->isRunning());
    REQUIRE(fs::exists(config_.pidFile));

    startRunLoop();
    REQUIRE(waitForDaemonReady());

    stopRunLoop();

    auto result = daemon_->stop();
    REQUIRE(result);
    REQUIRE_FALSE(daemon_->isRunning());
    REQUIRE_FALSE(fs::exists(config_.pidFile));
}

TEST_CASE_METHOD(DaemonFixture, "Daemon single instance enforcement", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

    auto daemon2 = std::make_unique<YamsDaemon>(config_);
    auto result2 = daemon2->start();
    REQUIRE_FALSE(result2);
    REQUIRE(result2.error().code == ErrorCode::InvalidState);

    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "Daemon rejects pid file pointing to current process",
                 "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    json payload = json::object();
    payload["pid"] = static_cast<std::int64_t>(getpid());
    payload["start_ns"] = 1;
    payload["token"] = "test";

    std::ofstream pidFile(config_.pidFile);
    pidFile << payload.dump();
    pidFile.close();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    REQUIRE_FALSE(result);
    REQUIRE(result.error().code == ErrorCode::InvalidState);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon refuses pid file pointing to unrelated running process",
                 "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    pid_t child = fork();
    if (child < 0) {
        SKIP("fork not available");
    }
    if (child == 0) {
        for (;;) {
            ::pause();
        }
    }

    json payload = json::object();
    payload["pid"] = static_cast<std::int64_t>(child);
    payload["start_ns"] = 1;
    payload["token"] = "test";

    std::ofstream pidFile(config_.pidFile);
    pidFile << payload.dump();
    pidFile.close();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    REQUIRE_FALSE(result);
    REQUIRE(result.error().code == ErrorCode::InvalidState);

    ::kill(child, SIGKILL);
    int status = 0;
    (void)::waitpid(child, &status, 0);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon restart", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

    result = daemon_->stop();
    REQUIRE(result);

    result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted on restart");
    }
    REQUIRE(result);
    REQUIRE(daemon_->isRunning());

    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "Daemon path resolution with empty config", "[daemon][config]") {
    SKIP_ON_WINDOWS();

    DaemonConfig autoConfig;
    daemon_ = std::make_unique<YamsDaemon>(autoConfig);
    REQUIRE(daemon_ != nullptr);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon signal handling and PID file", "[daemon][signals]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

#ifndef _WIN32
    std::ifstream pidFile(config_.pidFile);
    std::string content;
    std::getline(pidFile, content, '\0');
    pidFile.close();
    auto parsed = json::parse(content, nullptr, false);
    REQUIRE_FALSE(parsed.is_discarded());
    REQUIRE(parsed.value("pid", 0) == getpid());
#endif

    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "Daemon stale PID file cleanup", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    std::ofstream pidFile(config_.pidFile);
    pidFile << "99999999\n";
    pidFile.close();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);
    REQUIRE(daemon_->isRunning());

    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "Daemon invalid socket path configuration", "[daemon][config]") {
    SKIP_ON_WINDOWS();

    config_.socketPath = "/root/cannot_write_here.sock";
    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    REQUIRE_FALSE(result);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon concurrent start attempts", "[daemon][concurrency]") {
    SKIP_ON_WINDOWS();

    {
        YamsDaemon probe(config_);
        auto result = probe.start();
        if (!result && isSocketPermissionDenied(result.error())) {
            SKIP("UNIX domain sockets not permitted");
        }
        REQUIRE(result);
        probe.stop();
    }

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};

    std::once_flag startedFlag;
    std::promise<void> startedPromise;
    auto startedFuture = startedPromise.get_future().share();
    std::promise<void> releasePromise;
    auto releaseFuture = releasePromise.get_future().share();

    std::vector<std::thread> threads;
    constexpr int kAttempts = 5;
    threads.reserve(kAttempts);

    for (int i = 0; i < kAttempts; ++i) {
        threads.emplace_back([this, &successCount, &failCount, &startedFlag, startedFuture,
                              &startedPromise, releaseFuture]() {
            YamsDaemon local(config_);
            auto result = local.start();
            if (result) {
                successCount.fetch_add(1, std::memory_order_relaxed);
                std::call_once(startedFlag, [&]() { startedPromise.set_value(); });
                releaseFuture.wait();
                local.stop();
            } else {
                startedFuture.wait();
                failCount.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    if (startedFuture.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        startedPromise.set_value();
    }
    releasePromise.set_value();

    for (auto& t : threads) {
        t.join();
    }

    REQUIRE(successCount.load() == 1);
    REQUIRE(failCount.load() == kAttempts - 1);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon stats tracking", "[daemon][stats]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

    [[maybe_unused]] const auto& state = daemon_->getState();

    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "Daemon hybrid search smoke test", "[daemon][search]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

    SearchRequest req;
    req.query = "seed";
    req.limit = 5;
    req.fuzzy = false;
    req.literalText = false;
    req.similarity = 0.7;

    Message msg;
    msg.version = 1;
    msg.requestId = 42;
    msg.payload = Request{req};

    auto enc = ProtoSerializer::encode_payload(msg);
    REQUIRE(enc);
    auto parsed = ProtoSerializer::decode_payload(enc.value());
    REQUIRE(parsed);

    REQUIRE(daemon_->isRunning());

    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "Daemon fallback search when hybrid unavailable",
                 "[daemon][search]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

    SearchRequest req;
    req.query = "fallback";
    req.limit = 3;
    req.fuzzy = true;
    req.similarity = 0.6;

    Message msg;
    msg.version = 1;
    msg.requestId = 7;
    msg.payload = Request{req};

    auto enc = ProtoSerializer::encode_payload(msg);
    REQUIRE(enc);
    auto parsed = ProtoSerializer::decode_payload(enc.value());
    REQUIRE(parsed);

    REQUIRE(daemon_->isRunning());

    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "StatusResponse serialization includes ready field",
                 "[daemon][serialization]") {
    StatusResponse sr;
    sr.running = true;
    sr.ready = true;
    sr.uptimeSeconds = 0;
    sr.requestsProcessed = 0;
    sr.activeConnections = 0;
    sr.memoryUsageMb = 0.0;
    sr.cpuUsagePercent = 0.0;
    sr.version = "test";

    Message msg;
    msg.version = 1;
    msg.requestId = 0;
    msg.payload = Response{sr};

    auto enc = ProtoSerializer::encode_payload(msg);
    REQUIRE(enc);
    auto des = ProtoSerializer::decode_payload(enc.value());
    REQUIRE(des);

    Message out = des.value();
    auto respVariant = std::get<Response>(out.payload);
    REQUIRE(std::holds_alternative<StatusResponse>(respVariant));
    auto outSr = std::get<StatusResponse>(respVariant);

    REQUIRE(outSr.ready == true);
    REQUIRE(outSr.running == true);
    REQUIRE(outSr.version == "test");
}

TEST_CASE_METHOD(DaemonFixture, "Daemon start fails with invalid data directory",
                 "[daemon][config]") {
    SKIP_ON_WINDOWS();

    DaemonConfig bad = config_;
    bad.dataDir = "/root/forbidden_yams_test";
    daemon_ = std::make_unique<YamsDaemon>(bad);

    auto result = daemon_->start();
    REQUIRE_FALSE(result);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon PID file cannot be created in read-only directory",
                 "[daemon][errors]") {
    SKIP_ON_WINDOWS();

    fs::path readonly_dir = runtime_root_ / "readonly";
    fs::create_directories(readonly_dir);
    fs::permissions(readonly_dir, fs::perms::owner_read | fs::perms::owner_exec,
                    fs::perm_options::replace);

    DaemonConfig bad_config = config_;
    bad_config.pidFile = readonly_dir / "yams.pid";
    daemon_ = std::make_unique<YamsDaemon>(bad_config);

    auto result = daemon_->start();
    REQUIRE_FALSE(result);
    if (!result) {
        REQUIRE_FALSE(result.error().message.empty());
    }

    fs::permissions(readonly_dir, fs::perms::owner_all, fs::perm_options::replace);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon PID file cleaned up on start failure", "[daemon][errors]") {
    SKIP_ON_WINDOWS();

    DaemonConfig bad_config = config_;
    bad_config.dataDir = "/nonexistent/forbidden_path";

    daemon_ = std::make_unique<YamsDaemon>(bad_config);
    auto result = daemon_->start();
    REQUIRE_FALSE(result);

    REQUIRE_FALSE(fs::exists(bad_config.pidFile));
}

TEST_CASE_METHOD(DaemonFixture, "Daemon stop when not started is safe", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    REQUIRE_NOTHROW(daemon_->stop());
    REQUIRE_NOTHROW(daemon_->stop());
}

TEST_CASE_METHOD(DaemonFixture, "Daemon double stop is idempotent", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

    daemon_->stop();
    REQUIRE_NOTHROW(daemon_->stop());
}

TEST_CASE_METHOD(DaemonFixture, "Daemon fails with missing socket directory", "[daemon][config]") {
    SKIP_ON_WINDOWS();

    DaemonConfig bad_config = config_;
    bad_config.socketPath = "/nonexistent_dir/subdir/yams.sock";

    daemon_ = std::make_unique<YamsDaemon>(bad_config);
    auto result = daemon_->start();
    REQUIRE_FALSE(result);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon socket cleanup after stop", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

    REQUIRE(fs::exists(config_.socketPath));

    daemon_->stop();

    REQUIRE_FALSE(fs::exists(config_.socketPath));
}

TEST_CASE_METHOD(DaemonFixture, "Daemon restart with existing socket", "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result1 = daemon_->start();
    if (!result1 && isSocketPermissionDenied(result1.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result1);
    daemon_->stop();

    auto result2 = daemon_->start();
    REQUIRE(result2);
    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "Daemon empty data directory handling", "[daemon][config]") {
    SKIP_ON_WINDOWS();

    DaemonConfig bad_config = config_;
    bad_config.dataDir = "";

    daemon_ = std::make_unique<YamsDaemon>(bad_config);
    auto result = daemon_->start();

    if (result) {
        daemon_->stop();
    } else {
        REQUIRE_FALSE(result.error().message.empty());
    }
}

TEST_CASE_METHOD(DaemonFixture, "Daemon status query on stopped daemon", "[daemon][state]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);
    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);

    const auto& state_running = daemon_->getState();
    REQUIRE(state_running.readiness.ipcServerReady.load());

    daemon_->stop();

    const auto& state_stopped = daemon_->getState();
    REQUIRE_FALSE(state_stopped.readiness.ipcServerReady.load());
}

TEST_CASE_METHOD(DaemonFixture, "Daemon async init barrier prevents race condition",
                 "[daemon][concurrency]") {
    SKIP_ON_WINDOWS();

    if (const char* disable_vectors = std::getenv("YAMS_DISABLE_VECTORS")) {
        if (std::string_view(disable_vectors) == "1") {
            SKIP("Vector DB disabled via YAMS_DISABLE_VECTORS=1");
        }
    }

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    if (!result && isSocketPermissionDenied(result.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(result);
    REQUIRE(daemon_->isRunning());

    std::atomic<bool> runLoopEntered{false};
    std::atomic<bool> runLoopExited{false};

    std::thread runLoopThread([this, &runLoopEntered, &runLoopExited]() {
        runLoopEntered.store(true);
        daemon_->runLoop();
        runLoopExited.store(true);
    });

    std::this_thread::sleep_for(100ms);

    REQUIRE(runLoopEntered.load());

    daemon_->requestStop();

    if (runLoopThread.joinable()) {
        runLoopThread.join();
    }

    REQUIRE(runLoopExited.load());

    daemon_->stop();
}

TEST_CASE_METHOD(DaemonFixture, "Daemon async init barrier stress test",
                 "[daemon][concurrency][.slow]") {
    SKIP_ON_WINDOWS();

    constexpr int kIterations = 5;

    for (int i = 0; i < kIterations; ++i) {
        INFO("Iteration " << i);

        daemon_ = std::make_unique<YamsDaemon>(config_);

        auto result = daemon_->start();
        if (!result && isSocketPermissionDenied(result.error())) {
            SKIP("UNIX domain sockets not permitted");
        }
        if (!result) {
            std::this_thread::sleep_for(100ms);
            result = daemon_->start();
            if (!result && isSocketPermissionDenied(result.error())) {
                SKIP("UNIX domain sockets not permitted");
            }
            REQUIRE(result);
        }

        REQUIRE(daemon_->isRunning());

        std::atomic<bool> crashed{false};
        std::thread runLoopThread([this, &crashed]() {
            try {
                daemon_->runLoop();
            } catch (...) {
                crashed.store(true);
            }
        });

        std::this_thread::sleep_for(50ms);

        daemon_->requestStop();
        if (runLoopThread.joinable()) {
            runLoopThread.join();
        }

        REQUIRE_FALSE(crashed.load());

        daemon_->stop();
        daemon_.reset();

        std::this_thread::sleep_for(50ms);
    }
}

} // namespace yams::daemon::test
