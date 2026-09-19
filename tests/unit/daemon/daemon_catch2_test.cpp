// Catch2 migration of daemon_test.cpp
// Migration: yams-3s4 (daemon unit tests)
// Unit tests for YamsDaemon - creation, start/stop, signal handling, error paths

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <thread>

#include <nlohmann/json.hpp>
using nlohmann::json;

#include <yams/compat/unistd.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/daemon_lifecycle.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

#include "tests/common/test_helpers_catch2.h"

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <share.h>
using pid_t = int;
#else
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
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
    std::vector<yams::test::ScopedEnvVar> environment_;

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

        // When ABI plugins are disabled, no model provider can be adopted,
        // so disable the requirement to avoid init failure.
        if (const auto value = yams::config::getenv_nonempty("YAMS_DISABLE_ABI_PLUGINS");
            value && *value == "1") {
            config_.enableModelProvider = false;
        } else {
            config_.enableModelProvider = true;
        }
        config_.autoLoadPlugins = true;
        config_.modelPoolConfig.lazyLoading = true;
        config_.modelPoolConfig.preloadModels.clear();

        environment_.emplace_back("YAMS_DB_OPEN_TIMEOUT_MS", std::string{"1500"});
        environment_.emplace_back("YAMS_DB_MIGRATE_TIMEOUT_MS", std::string{"2000"});
        environment_.emplace_back("YAMS_SEARCH_BUILD_TIMEOUT_MS", std::string{"1500"});
        environment_.emplace_back("YAMS_DISABLE_VECTORS", std::string{"1"});
        environment_.emplace_back("YAMS_DISABLE_SESSION_WATCHER", std::string{"1"});

        std::error_code se;
        fs::create_directories(config_.dataDir, se);

        environment_.emplace_back("YAMS_RUNTIME_DIR", runtime_root_.string());
        environment_.emplace_back("YAMS_SOCKET_PATH", config_.socketPath.string());
        environment_.emplace_back("YAMS_PID_FILE", config_.pidFile.string());
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

    static bool waitForCondition(std::chrono::milliseconds timeout,
                                 const std::function<bool()>& predicate) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(10ms);
        }
        return predicate();
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

TEST_CASE_METHOD(DaemonFixture, "Daemon starts repair service while provider-degraded",
                 "[daemon][lifecycle][repair]") {
    SKIP_ON_WINDOWS();

    yams::test::ScopedEnvVar skipModelLoading("YAMS_SKIP_MODEL_LOADING", "1");
    yams::test::ScopedEnvVar embeddingBackend("YAMS_EMBED_BACKEND", "daemon");
    config_.enableModelProvider = true;
    config_.modelProviderRequired = false;
    config_.autoLoadPlugins = false;
    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto startResult = daemon_->start();
    if (!startResult && isSocketPermissionDenied(startResult.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(startResult);
    startRunLoop();

    REQUIRE(waitForCondition(5s, [&] {
        return !daemon_->getState().readiness.modelProviderReady.load(std::memory_order_acquire);
    }));
    REQUIRE(waitForCondition(
        2s, [&] { return daemon_->getServiceManager()->getRepairServiceShared() != nullptr; }));
}

TEST_CASE_METHOD(DaemonFixture, "Daemon restores compatibility path environment",
                 "[daemon][lifecycle][environment]") {
    yams::test::ScopedEnvVar storage("YAMS_STORAGE", "/before/storage");
    yams::test::ScopedEnvVar data("YAMS_DATA_DIR", "/before/data");
    yams::test::ScopedEnvVar config("YAMS_CONFIG", "/before/config.toml");
    const auto compatibilityConfig = runtime_root_ / "before-compatibility.toml";
    std::ofstream(compatibilityConfig) << "[daemon]\nmode = \"old\"\n";
    yams::test::ScopedEnvVar configCompatibility("YAMS_CONFIG_PATH", compatibilityConfig.string());
    yams::test::ScopedEnvVar socket("YAMS_DAEMON_SOCKET", "/before/socket.sock");
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", "before");

    config_.configFilePath = runtime_root_ / "config.toml";
    daemon_ = std::make_unique<YamsDaemon>(config_);
    CHECK((yams::config::getenv_copy("YAMS_STORAGE") == config_.dataDir.string()));
    CHECK((yams::config::getenv_copy("YAMS_DATA_DIR") == config_.dataDir.string()));
    CHECK((yams::config::getenv_copy("YAMS_CONFIG") == config_.configFilePath.string()));
    CHECK((yams::config::getenv_copy("YAMS_CONFIG_PATH") == config_.configFilePath.string()));
    CHECK((yams::config::get_config_path() == config_.configFilePath));
    CHECK((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == config_.socketPath.string()));
    CHECK((yams::config::getenv_copy("YAMS_IN_DAEMON") == "1"));

    daemon_.reset();
    CHECK((yams::config::getenv_copy("YAMS_STORAGE") == "/before/storage"));
    CHECK((yams::config::getenv_copy("YAMS_DATA_DIR") == "/before/data"));
    CHECK((yams::config::getenv_copy("YAMS_CONFIG") == "/before/config.toml"));
    CHECK((yams::config::getenv_copy("YAMS_CONFIG_PATH") == compatibilityConfig.string()));
    CHECK((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == "/before/socket.sock"));
    CHECK((yams::config::getenv_copy("YAMS_IN_DAEMON") == "before"));
}

TEST_CASE_METHOD(DaemonFixture, "Daemon environment lease preserves newer writers",
                 "[daemon][lifecycle][environment][ownership]") {
    yams::test::ScopedEnvVar socket{"YAMS_DAEMON_SOCKET", std::string{"/before/socket.sock"}};

    daemon_ = std::make_unique<YamsDaemon>(config_);
    REQUIRE((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == config_.socketPath.string()));

    socket.set("/newer/owner.sock");
    daemon_.reset();

    CHECK((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == "/newer/owner.sock"));
}

TEST_CASE_METHOD(DaemonFixture, "Daemon environment lease detects same-value newer writers",
                 "[daemon][lifecycle][environment][ownership]") {
    yams::test::ScopedEnvVar socket{"YAMS_DAEMON_SOCKET", std::string{"/before/socket.sock"}};

    daemon_ = std::make_unique<YamsDaemon>(config_);
    const auto installed = config_.socketPath.string();
    REQUIRE((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == installed));

    socket.set(installed);
    daemon_.reset();

    CHECK((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == installed));
}

TEST_CASE_METHOD(DaemonFixture, "Nested daemons restore every runtime environment lease",
                 "[daemon][lifecycle][environment][ownership][nested]") {
    std::vector<yams::test::ScopedEnvVar> original;
    original.reserve(6);
    original.emplace_back("YAMS_IN_DAEMON", std::string{"before-daemon"});
    original.emplace_back("YAMS_STORAGE", std::string{"/before/storage"});
    original.emplace_back("YAMS_DATA_DIR", std::string{"/before/data"});
    original.emplace_back("YAMS_CONFIG", std::string{"/before/config"});
    original.emplace_back("YAMS_CONFIG_PATH", std::string{"/before/config-path"});
    original.emplace_back("YAMS_DAEMON_SOCKET", std::string{"/before/socket"});

    auto outerConfig = config_;
    outerConfig.dataDir = runtime_root_ / "outer-data";
    outerConfig.configFilePath = runtime_root_ / "outer.toml";
    outerConfig.socketPath = runtime_root_ / "outer.sock";
    outerConfig.pidFile = runtime_root_ / "outer.pid";

    auto innerConfig = config_;
    innerConfig.dataDir = runtime_root_ / "inner-data";
    innerConfig.configFilePath = runtime_root_ / "inner.toml";
    innerConfig.socketPath = runtime_root_ / "inner.sock";
    innerConfig.pidFile = runtime_root_ / "inner.pid";

    const auto checkInstalled = [](const DaemonConfig& expected) {
        CHECK((yams::config::getenv_copy("YAMS_IN_DAEMON") == "1"));
        CHECK((yams::config::getenv_copy("YAMS_STORAGE") == expected.dataDir.string()));
        CHECK((yams::config::getenv_copy("YAMS_DATA_DIR") == expected.dataDir.string()));
        CHECK((yams::config::getenv_copy("YAMS_CONFIG") == expected.configFilePath.string()));
        CHECK((yams::config::getenv_copy("YAMS_CONFIG_PATH") == expected.configFilePath.string()));
        CHECK((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == expected.socketPath.string()));
    };
    const auto checkOriginal = [] {
        CHECK((yams::config::getenv_copy("YAMS_IN_DAEMON") == "before-daemon"));
        CHECK((yams::config::getenv_copy("YAMS_STORAGE") == "/before/storage"));
        CHECK((yams::config::getenv_copy("YAMS_DATA_DIR") == "/before/data"));
        CHECK((yams::config::getenv_copy("YAMS_CONFIG") == "/before/config"));
        CHECK((yams::config::getenv_copy("YAMS_CONFIG_PATH") == "/before/config-path"));
        CHECK((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == "/before/socket"));
    };

    SECTION("LIFO destruction resumes the outer daemon") {
        auto outer = std::make_unique<YamsDaemon>(outerConfig);
        checkInstalled(outerConfig);
        auto inner = std::make_unique<YamsDaemon>(innerConfig);
        checkInstalled(innerConfig);

        inner.reset();
        checkInstalled(outerConfig);
        outer.reset();
        checkOriginal();
    }

    SECTION("non-LIFO destruction rebases the inner daemon onto the original state") {
        auto outer = std::make_unique<YamsDaemon>(outerConfig);
        auto inner = std::make_unique<YamsDaemon>(innerConfig);
        checkInstalled(innerConfig);

        outer.reset();
        checkInstalled(innerConfig);
        inner.reset();
        checkOriginal();
    }
}

TEST_CASE_METHOD(DaemonFixture, "Daemon construction failure releases acquired environment leases",
                 "[daemon][lifecycle][environment][ownership][failure]") {
    std::vector<yams::test::ScopedEnvVar> original;
    original.reserve(6);
    original.emplace_back("YAMS_IN_DAEMON", std::string{"before-daemon"});
    original.emplace_back("YAMS_STORAGE", std::string{"/before/storage"});
    original.emplace_back("YAMS_DATA_DIR", std::string{"/before/data"});
    original.emplace_back("YAMS_CONFIG", std::string{"/before/config"});
    original.emplace_back("YAMS_CONFIG_PATH", std::string{"/before/config-path"});
    original.emplace_back("YAMS_DAEMON_SOCKET", std::string{"/before/socket"});

    config_.configFilePath = runtime_root_ / "failure.toml";
    yams::config::testing_fail_owned_environment_lease_after(2);
    try {
        auto unexpected = std::make_unique<YamsDaemon>(config_);
        FAIL("YamsDaemon construction unexpectedly succeeded");
    } catch (const std::runtime_error& error) {
        CHECK(std::string_view{error.what()}.starts_with(
            "YamsDaemon failed to lease environment key YAMS_DATA_DIR"));
    }

    CHECK((yams::config::getenv_copy("YAMS_IN_DAEMON") == "before-daemon"));
    CHECK((yams::config::getenv_copy("YAMS_STORAGE") == "/before/storage"));
    CHECK((yams::config::getenv_copy("YAMS_DATA_DIR") == "/before/data"));
    CHECK((yams::config::getenv_copy("YAMS_CONFIG") == "/before/config"));
    CHECK((yams::config::getenv_copy("YAMS_CONFIG_PATH") == "/before/config-path"));
    CHECK((yams::config::getenv_copy("YAMS_DAEMON_SOCKET") == "/before/socket"));
}

TEST_CASE_METHOD(DaemonFixture, "Daemon tuning reload revokes removed overrides coherently",
                 "[daemon][tuning][reload]") {
    SKIP_ON_WINDOWS();

    yams::test::ScopedEnvVar ipcCompatibility{"YAMS_IPC_TIMEOUT_MS", std::nullopt};
    yams::test::ScopedEnvVar admissionCompatibility{"YAMS_ADMISSION_CONTROL", std::nullopt};
    yams::test::ScopedEnvVar memoryCompatibility{"YAMS_MEMORY_WARNING_PCT", std::nullopt};
    yams::test::ScopedEnvVar postIngestCompatibility{"YAMS_POST_INGEST_RPC_QUEUE_MAX",
                                                     std::nullopt};
    const auto configPath = runtime_root_ / "config.toml";
    const auto writeOverrides = [&](bool enabled) {
        std::ofstream out(configPath, std::ios::trunc);
        REQUIRE(out.is_open());
        if (!enabled) {
            out << "# runtime tuning overrides removed\n";
            return;
        }
        out << "[tuning]\n";
        out << "target_cpu_percent = 321\n";
        out << "[tuning.ipc]\n";
        out << "timeout_ms = 4321\n";
        out << "[tuning.resource]\n";
        out << "admission_control = false\n";
        out << "memory_warning_threshold = 0.91\n";
        out << "[tuning.post_ingest]\n";
        out << "rpc_queue_max = 333\n";
    };
    writeOverrides(true);

    config_.configFilePath = configPath;
    daemon_ = std::make_unique<YamsDaemon>(config_);
    REQUIRE(daemon_ != nullptr);
    auto activeTuning = daemon_->serviceManager_->getTuningConfig();
    activeTuning.topologyAlgorithm = "exact";
    daemon_->serviceManager_->setTuningConfig(activeTuning);

    daemon_->reloadTuningConfig();
    CHECK((daemon_->config_.tuning.targetCpuPercent == 321u));
    CHECK((TuneAdvisor::ipcTimeoutMs() == 4321u));
    CHECK_FALSE(TuneAdvisor::enableAdmissionControl());
    CHECK((TuneAdvisor::memoryWarningThreshold() == 0.91));
    CHECK((TuneAdvisor::postIngestRpcQueueMax() == 333u));
    CHECK((daemon_->config_.tuning.provenance.at("tuning.ipc.timeout_ms") ==
           "config:tuning.ipc.timeout_ms"));

    writeOverrides(false);
    const auto versionBeforeRemoval = TuneAdvisor::configuredOverridesVersion();
    daemon_->reloadTuningConfig();

    CHECK((TuneAdvisor::configuredOverridesVersion() == versionBeforeRemoval + 2));
    CHECK((daemon_->config_.tuning.targetCpuPercent == 200u));
    CHECK((daemon_->config_.tuning.topologyAlgorithm == "exact"));
    CHECK(daemon_->config_.tuning.provenance.empty());
    CHECK((TuneAdvisor::ipcTimeoutMs() == 15000u));
    CHECK(TuneAdvisor::enableAdmissionControl());
    CHECK((TuneAdvisor::memoryWarningThreshold() == 0.75));
    CHECK((TuneAdvisor::postIngestRpcQueueMax() == 256u));

    const auto status = daemon_->serviceManager_->getRuntimeTuningStatus();
    CHECK((status.at("ipc.timeout_ms") == "15000"));
    CHECK((status.at("ipc.timeout_ms.source") == "default"));
    CHECK((status.at("resource.admission_control") == "1"));
    CHECK((status.at("resource.admission_control.source") == "default"));
    CHECK((status.at("resource.memory_warning_threshold") == "0.750000"));
    CHECK((status.at("resource.memory_warning_threshold.source") == "default"));
    CHECK((status.at("post_ingest.rpc_queue_max") == "256"));
    CHECK((status.at("post_ingest.rpc_queue_max.source") == "default"));
}

TEST_CASE_METHOD(DaemonFixture, "Daemon tuning reload rejects multiple active lifecycles",
                 "[daemon][tuning][reload][lifecycle]") {
    SKIP_ON_WINDOWS();

    yams::test::ScopedEnvVar ipcCompatibility{"YAMS_IPC_TIMEOUT_MS", std::nullopt};
    const auto configPath = runtime_root_ / "config.toml";
    config_.configFilePath = configPath;
    daemon_ = std::make_unique<YamsDaemon>(config_);
    REQUIRE(daemon_ != nullptr);
    REQUIRE((TuneAdvisor::ipcTimeoutMs() == 15000u));

    auto nestedConfig = config_;
    nestedConfig.dataDir = runtime_root_ / "nested-data";
    nestedConfig.socketPath = runtime_root_ / "nested.sock";
    nestedConfig.pidFile = runtime_root_ / "nested.pid";
    nestedConfig.logFile = runtime_root_ / "nested.log";
    YamsDaemon nested(nestedConfig);

    {
        std::ofstream out(configPath, std::ios::trunc);
        REQUIRE(out.is_open());
        out << "[tuning.ipc]\n";
        out << "timeout_ms = 4321\n";
    }
    const auto versionBefore = TuneAdvisor::configuredOverridesVersion();
    daemon_->reloadTuningConfig();

    CHECK((TuneAdvisor::configuredOverridesVersion() == versionBefore));
    CHECK((TuneAdvisor::ipcTimeoutMs() == 15000u));
    CHECK((daemon_->config_.tuning.provenance.count("tuning.ipc.timeout_ms") == 0));
    CHECK((daemon_->serviceManager_->getRuntimeTuningStatus().at("ipc.timeout_ms") == "15000"));
}

TEST_CASE_METHOD(DaemonFixture, "Lifecycle shutdown waits for owner-thread stop",
                 "[daemon][lifecycle][shutdown-request]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto startResult = daemon_->start();
    if (!startResult && isSocketPermissionDenied(startResult.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(startResult);

    startRunLoop();
    REQUIRE(waitForCondition(
        2s, [&] { return daemon_->runLoopStarted_.load(std::memory_order_acquire); }));

    DaemonLifecycleAdapter lifecycle(daemon_.get());
    lifecycle.requestShutdown(true, true);

    REQUIRE(waitForCondition(2s, [&] {
        return daemon_->stopRequested_.load(std::memory_order_acquire) &&
               daemon_->shutdownThreadActive_.load(std::memory_order_acquire);
    }));

    std::this_thread::sleep_for(100ms);
    CHECK(daemon_->shutdownThreadActive_.load(std::memory_order_acquire));

    stopRunLoop();
    auto stopResult = daemon_->stop();
    REQUIRE(stopResult);

    REQUIRE(waitForCondition(
        2s, [&] { return !daemon_->shutdownThreadActive_.load(std::memory_order_acquire); }));
    daemon_->reapCompletedShutdownThread();
}

TEST_CASE_METHOD(DaemonFixture,
                 "Lifecycle shutdown request does not cancel an in-flight worker task",
                 "[daemon][lifecycle][shutdown-request][cancellation]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto startResult = daemon_->start();
    if (!startResult && isSocketPermissionDenied(startResult.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(startResult);

    startRunLoop();
    REQUIRE(waitForCondition(
        2s, [&] { return daemon_->runLoopStarted_.load(std::memory_order_acquire); }));

    auto* serviceManager = daemon_->getServiceManager();
    REQUIRE((serviceManager != nullptr));

    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool allowExit = false;
    std::atomic<bool> workerEntered{false};
    std::atomic<bool> workerFinished{false};

    boost::asio::post(serviceManager->getWorkerExecutor(), [&]() {
        {
            std::lock_guard<std::mutex> lk(gateMutex);
            workerEntered.store(true, std::memory_order_release);
        }
        gateCv.notify_all();

        std::unique_lock<std::mutex> lk(gateMutex);
        gateCv.wait(lk, [&] { return allowExit; });
        workerFinished.store(true, std::memory_order_release);
    });

    REQUIRE(waitForCondition(2s, [&] { return workerEntered.load(std::memory_order_acquire); }));

    DaemonLifecycleAdapter lifecycle(daemon_.get());
    lifecycle.requestShutdown(true, true);

    REQUIRE(waitForCondition(2s, [&] {
        return daemon_->stopRequested_.load(std::memory_order_acquire) &&
               daemon_->shutdownThreadActive_.load(std::memory_order_acquire);
    }));

    std::this_thread::sleep_for(150ms);
    CHECK(daemon_->shutdownThreadActive_.load(std::memory_order_acquire));
    CHECK_FALSE(workerFinished.load(std::memory_order_acquire));

    {
        std::lock_guard<std::mutex> lk(gateMutex);
        allowExit = true;
    }
    gateCv.notify_all();

    REQUIRE(waitForCondition(2s, [&] { return workerFinished.load(std::memory_order_acquire); }));

    stopRunLoop();
    auto stopResult = daemon_->stop();
    REQUIRE(stopResult);

    REQUIRE(waitForCondition(
        2s, [&] { return !daemon_->shutdownThreadActive_.load(std::memory_order_acquire); }));
    daemon_->reapCompletedShutdownThread();
}

TEST_CASE_METHOD(DaemonFixture,
                 "Lifecycle shutdown completion stays pending while worker teardown is blocked",
                 "[daemon][lifecycle][shutdown-request][budget]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto startResult = daemon_->start();
    if (!startResult && isSocketPermissionDenied(startResult.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(startResult);

    startRunLoop();
    REQUIRE(waitForCondition(
        2s, [&] { return daemon_->runLoopStarted_.load(std::memory_order_acquire); }));

    auto* serviceManager = daemon_->getServiceManager();
    REQUIRE((serviceManager != nullptr));

    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool allowExit = false;
    std::atomic<bool> workerEntered{false};

    boost::asio::post(serviceManager->getWorkerExecutor(), [&]() {
        {
            std::lock_guard<std::mutex> lk(gateMutex);
            workerEntered.store(true, std::memory_order_release);
        }
        gateCv.notify_all();

        std::unique_lock<std::mutex> lk(gateMutex);
        gateCv.wait(lk, [&] { return allowExit; });
    });

    REQUIRE(waitForCondition(2s, [&] { return workerEntered.load(std::memory_order_acquire); }));

    DaemonLifecycleAdapter lifecycle(daemon_.get());
    lifecycle.requestShutdown(true, true);

    REQUIRE(waitForCondition(2s, [&] {
        return daemon_->stopRequested_.load(std::memory_order_acquire) &&
               daemon_->shutdownThreadActive_.load(std::memory_order_acquire);
    }));

    CHECK_FALSE(daemon_->waitForStopCompletion(100ms));
    std::this_thread::sleep_for(150ms);
    CHECK(daemon_->shutdownThreadActive_.load(std::memory_order_acquire));
    CHECK_FALSE(daemon_->waitForStopCompletion(100ms));

    {
        std::lock_guard<std::mutex> lk(gateMutex);
        allowExit = true;
    }
    gateCv.notify_all();

    stopRunLoop();
    auto stopResult = daemon_->stop();
    REQUIRE(stopResult);
    REQUIRE(daemon_->waitForStopCompletion(2s));
    daemon_->reapCompletedShutdownThread();
}

TEST_CASE_METHOD(DaemonFixture,
                 "Lifecycle shutdown timeout floor covers work coordinator join budget",
                 "[daemon][lifecycle][shutdown-request][budget]") {
    SKIP_ON_WINDOWS();

    yams::test::ScopedEnvVar shutdownBudget("YAMS_SHUTDOWN_FORCE_EXIT_MS", std::string("1000"));

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto startResult = daemon_->start();
    if (!startResult && isSocketPermissionDenied(startResult.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(startResult);

    startRunLoop();
    REQUIRE(waitForCondition(
        2s, [&] { return daemon_->runLoopStarted_.load(std::memory_order_acquire); }));

    auto* serviceManager = daemon_->getServiceManager();
    REQUIRE((serviceManager != nullptr));

    std::mutex gateMutex;
    std::condition_variable gateCv;
    bool allowExit = false;
    std::atomic<bool> workerEntered{false};

    boost::asio::post(serviceManager->getWorkerExecutor(), [&]() {
        {
            std::lock_guard<std::mutex> lk(gateMutex);
            workerEntered.store(true, std::memory_order_release);
        }
        gateCv.notify_all();

        std::unique_lock<std::mutex> lk(gateMutex);
        gateCv.wait(lk, [&] { return allowExit; });
    });

    REQUIRE(waitForCondition(2s, [&] { return workerEntered.load(std::memory_order_acquire); }));

    DaemonLifecycleAdapter lifecycle(daemon_.get());
    lifecycle.requestShutdown(true, true);

    REQUIRE(waitForCondition(2s, [&] {
        return daemon_->stopRequested_.load(std::memory_order_acquire) &&
               daemon_->shutdownThreadActive_.load(std::memory_order_acquire);
    }));

    std::this_thread::sleep_for(1200ms);
    CHECK(daemon_->shutdownThreadActive_.load(std::memory_order_acquire));
    CHECK_FALSE(daemon_->waitForStopCompletion(100ms));

    {
        std::lock_guard<std::mutex> lk(gateMutex);
        allowExit = true;
    }
    gateCv.notify_all();

    stopRunLoop();
    auto stopResult = daemon_->stop();
    REQUIRE(stopResult);
    REQUIRE(daemon_->waitForStopCompletion(2s));
    daemon_->reapCompletedShutdownThread();
}

TEST_CASE_METHOD(DaemonFixture, "Lifecycle shutdown falls back to direct stop without run loop",
                 "[daemon][lifecycle][shutdown-request][fallback]") {
    SKIP_ON_WINDOWS();

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto startResult = daemon_->start();
    if (!startResult && isSocketPermissionDenied(startResult.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(startResult);
    REQUIRE_FALSE(daemon_->runLoopStarted_.load(std::memory_order_acquire));

    DaemonLifecycleAdapter lifecycle(daemon_.get());
    lifecycle.requestShutdown(true, true);

    REQUIRE(waitForCondition(2s, [&] {
        return !daemon_->isRunning() &&
               !daemon_->shutdownThreadActive_.load(std::memory_order_acquire);
    }));
    CHECK(daemon_->getLifecycle().snapshot().state == LifecycleState::Stopped);
    if (const auto* serviceManager = daemon_->getServiceManager()) {
        CHECK(serviceManager->getServiceManagerFsmSnapshot().state == ServiceManagerState::Stopped);
    }
    daemon_->reapCompletedShutdownThread();
}

TEST_CASE_METHOD(DaemonFixture,
                 "Lifecycle shutdown avoids forced exit for foreground-managed daemon",
                 "[daemon][lifecycle][shutdown-request][foreground-managed]") {
    SKIP_ON_WINDOWS();

    yams::test::ScopedEnvVar foregroundManaged("YAMS_DAEMON_FOREGROUND", std::string("1"));

    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto startResult = daemon_->start();
    if (!startResult && isSocketPermissionDenied(startResult.error())) {
        SKIP("UNIX domain sockets not permitted");
    }
    REQUIRE(startResult);

    startRunLoop();
    REQUIRE(waitForCondition(
        2s, [&] { return daemon_->runLoopStarted_.load(std::memory_order_acquire); }));

    DaemonLifecycleAdapter lifecycle(daemon_.get());
    lifecycle.requestShutdown(true, false);

    REQUIRE(waitForCondition(2s, [&] {
        return daemon_->stopRequested_.load(std::memory_order_acquire) &&
               daemon_->shutdownThreadActive_.load(std::memory_order_acquire);
    }));

    stopRunLoop();
    auto stopResult = daemon_->stop();
    REQUIRE(stopResult);

    REQUIRE(waitForCondition(
        2s, [&] { return !daemon_->shutdownThreadActive_.load(std::memory_order_acquire); }));
    daemon_->reapCompletedShutdownThread();
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
    REQUIRE(daemon_->getLifecycle().snapshot().state == LifecycleState::Stopped);
    if (const auto* serviceManager = daemon_->getServiceManager()) {
        REQUIRE(serviceManager->getServiceManagerFsmSnapshot().state ==
                ServiceManagerState::Stopped);
    }
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

#ifndef _WIN32

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

#else
    SKIP("POSIX process APIs not available on Windows");
#endif
}

TEST_CASE_METHOD(DaemonFixture, "Daemon does not terminate unverified data-dir lock holder",
                 "[daemon][lifecycle]") {
    SKIP_ON_WINDOWS();

#ifndef _WIN32
    // Pre-compute fork-safe inputs in the parent: the child must not allocate
    // (a fork in a multithreaded process can leave the malloc lock held by a
    // vanished thread), so build the lock path here and format the payload with
    // snprintf into a stack buffer in the child.
    const std::string lockPathStr = (config_.dataDir / ".yams-lock").string();

    int readyPipe[2] = {-1, -1};
    REQUIRE(::pipe(readyPipe) == 0);
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        ::close(readyPipe[0]);
        const int lockFd = ::open(lockPathStr.c_str(), O_CREAT | O_RDWR, 0644);
        if (lockFd < 0 || ::flock(lockFd, LOCK_EX) != 0) {
            const char failed = '2';
            (void)::write(readyPipe[1], &failed, 1);
            _exit(2);
        }
        char payload[128];
        const int payloadLen = ::snprintf(payload, sizeof(payload),
                                          "{\"pid\":%d,\"socket\":\"/tmp/not-a-daemon.sock\"}",
                                          static_cast<int>(::getpid()));
        if (payloadLen > 0 && static_cast<size_t>(payloadLen) < sizeof(payload)) {
            (void)::ftruncate(lockFd, 0);
            (void)::write(lockFd, payload, static_cast<size_t>(payloadLen));
        }
        const char ready = '1';
        (void)::write(readyPipe[1], &ready, 1);
        for (;;) {
            ::pause();
        }
    }

    ::close(readyPipe[1]);
    struct ChildGuard {
        pid_t pid;
        int readyFd;
        ~ChildGuard() {
            ::close(readyFd);
            (void)::kill(pid, SIGKILL);
            int status = 0;
            (void)::waitpid(pid, &status, 0);
        }
    } childGuard{child, readyPipe[0]};

    char ready = 0;
    ssize_t readyRead = -1;
    do {
        readyRead = ::read(readyPipe[0], &ready, 1);
    } while (readyRead < 0 && errno == EINTR);
    INFO("lock holder child status byte=" << static_cast<int>(ready) << " read=" << readyRead);
    REQUIRE(readyRead == 1);
    REQUIRE(ready == '1');

    daemon_ = std::make_unique<YamsDaemon>(config_);
    const auto result = daemon_->start();

    REQUIRE_FALSE(result);
    CHECK(result.error().code == ErrorCode::InvalidState);
    CHECK(::kill(child, 0) == 0);
#else
    SKIP("POSIX process APIs not available on Windows");
#endif
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

TEST_CASE_METHOD(DaemonFixture, "Daemon invalid socket parent configuration", "[daemon][config]") {
    SKIP_ON_WINDOWS();

    const auto notADirectory = runtime_root_ / "socket-parent-file";
    fs::create_directories(runtime_root_);
    std::ofstream(notADirectory) << "not a directory\n";

    config_.socketPath = notADirectory / "cannot-create.sock";
    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    REQUIRE_FALSE(result);
}

TEST_CASE_METHOD(DaemonFixture,
                 "Daemon socket preflight does not clean unrelated stale PID artifacts",
                 "[daemon][config][lifecycle]") {
    SKIP_ON_WINDOWS();

    // Seed a stale PID artifact that this process does not own.
    {
        std::ofstream pidFile(config_.pidFile);
        pidFile << "99999999\n";
    }
    REQUIRE(fs::exists(config_.pidFile));

    // Force an AF_UNIX-overlong socket path so start fails in preflight, before lifecycle init.
    config_.socketPath = runtime_root_ / std::string(140, 'x') / "daemon.sock";
    daemon_ = std::make_unique<YamsDaemon>(config_);

    auto result = daemon_->start();
    REQUIRE_FALSE(result);
    CHECK(result.error().message.find("Socket path too long") != std::string::npos);

    // Preflight failure must not mutate stale PID artifacts we did not create.
    CHECK(fs::exists(config_.pidFile));
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
    std::promise<void> attemptGatePromise;
    auto attemptGateFuture = attemptGatePromise.get_future().share();
    std::mutex readyMutex;
    std::condition_variable readyCv;
    int readyCount = 0;

    std::vector<std::thread> threads;
    constexpr int kAttempts = 5;
    threads.reserve(kAttempts);

    for (int i = 0; i < kAttempts; ++i) {
        threads.emplace_back([this, &successCount, &failCount, &startedFlag, startedFuture,
                              &startedPromise, releaseFuture, attemptGateFuture, &readyMutex,
                              &readyCv, &readyCount]() {
            {
                std::lock_guard<std::mutex> lock(readyMutex);
                ++readyCount;
            }
            readyCv.notify_one();
            attemptGateFuture.wait();

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

    {
        std::unique_lock<std::mutex> lock(readyMutex);
        REQUIRE(readyCv.wait_for(lock, std::chrono::seconds(1),
                                 [&] { return readyCount == kAttempts; }));
    }
    attemptGatePromise.set_value();

    if (startedFuture.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
        std::call_once(startedFlag, [&]() { startedPromise.set_value(); });
    }
    const auto failDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (failCount.load(std::memory_order_relaxed) < kAttempts - 1 &&
           std::chrono::steady_clock::now() < failDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

TEST_CASE_METHOD(DaemonFixture, "Daemon start fails when data directory path is a file",
                 "[daemon][config]") {
    SKIP_ON_WINDOWS();

    const auto notADirectory = runtime_root_ / "data-file";
    fs::create_directories(runtime_root_);
    std::ofstream(notADirectory) << "not a directory\n";

    DaemonConfig bad = config_;
    bad.dataDir = notADirectory;
    daemon_ = std::make_unique<YamsDaemon>(bad);

    auto result = daemon_->start();
    REQUIRE_FALSE(result);
}

TEST_CASE_METHOD(DaemonFixture, "Daemon PID file cannot be created below a file path",
                 "[daemon][errors]") {
    SKIP_ON_WINDOWS();

    const auto notADirectory = runtime_root_ / "pid-parent-file";
    fs::create_directories(runtime_root_);
    std::ofstream(notADirectory) << "not a directory\n";

    DaemonConfig bad_config = config_;
    bad_config.pidFile = notADirectory / "yams.pid";
    daemon_ = std::make_unique<YamsDaemon>(bad_config);

    auto result = daemon_->start();
    REQUIRE_FALSE(result);
    if (!result) {
        REQUIRE_FALSE(result.error().message.empty());
    }
}

TEST_CASE_METHOD(DaemonFixture, "Daemon PID file cleaned up on start failure", "[daemon][errors]") {
    SKIP_ON_WINDOWS();

    const auto notADirectory = runtime_root_ / "data-file-for-pid-cleanup";
    fs::create_directories(runtime_root_);
    std::ofstream(notADirectory) << "not a directory\n";

    DaemonConfig bad_config = config_;
    bad_config.dataDir = notADirectory;

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

TEST_CASE_METHOD(DaemonFixture, "Daemon fails when socket parent is a file", "[daemon][config]") {
    SKIP_ON_WINDOWS();

    const auto notADirectory = runtime_root_ / "socket-parent-file-2";
    fs::create_directories(runtime_root_);
    std::ofstream(notADirectory) << "not a directory\n";

    DaemonConfig bad_config = config_;
    bad_config.socketPath = notADirectory / "subdir" / "yams.sock";

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
