// Daemon components test suite (Catch2)
// Consolidates: SocketServer lifecycle, socket utils, resource pool
// Covers: IPC infrastructure, socket path resolution, resource lifecycle management

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <yams/compat/unistd.h>


#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/ipc/socket_utils.h>
#include <yams/daemon/resource/resource_pool.h>

#ifdef _WIN32
inline int geteuid() {
    return 1000;
}
inline int getuid() {
    return 1000;
}
#endif

using namespace yams::daemon;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

#ifndef _WIN32
#include <sys/un.h>
#endif

// =============================================================================
// Test Helpers
// =============================================================================

namespace {
fs::path makeTempRuntimeDir(const std::string& name) {
    auto base = fs::temp_directory_path();
#ifndef _WIN32
    constexpr std::size_t maxUnixPath = sizeof(sockaddr_un::sun_path) - 1;
    auto candidate = base / "yams-component-tests" / name;
    auto candidateWithSocket = candidate / "ipc.sock";
    std::cerr << "candidate path: " << candidateWithSocket
              << " len=" << candidateWithSocket.native().size() << " limit=" << maxUnixPath << "\n";
    if (candidateWithSocket.native().size() >= maxUnixPath) {
        base = fs::path("/tmp");
        candidate = base / "yams-component-tests" / name;
        candidateWithSocket = candidate / "ipc.sock";
        std::cerr << "fallback path: " << candidateWithSocket
                  << " len=" << candidateWithSocket.native().size() << "\n";
    }
#endif
    auto dir = base / "yams-component-tests" / name;
    std::error_code ec;
    fs::create_directories(dir, ec);
    return dir;
}

std::string randomSuffix() {
    return std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
}

template <typename ResultLike> bool isPermissionDenied(const ResultLike& result) {
    if (result) {
        return false;
    }
    std::string_view message{result.error().message};
    return message.find("Operation not permitted") != std::string_view::npos ||
           message.find("Permission denied") != std::string_view::npos;
}

// Test resource for pool testing
class TestResource {
public:
    explicit TestResource(int id) : id_(id), valid_(true) { creationCount_++; }

    ~TestResource() { destructionCount_++; }

    int getId() const { return id_; }
    bool isValid() const { return valid_; }
    void invalidate() { valid_ = false; }

    void doWork() const {
        workCount_++;
        std::this_thread::sleep_for(1ms);
    }

    static void resetCounters() {
        creationCount_ = 0;
        destructionCount_ = 0;
        workCount_ = 0;
    }

    static int getCreationCount() { return creationCount_; }
    static int getDestructionCount() { return destructionCount_; }
    static int getWorkCount() { return workCount_; }

private:
    int id_;
    bool valid_;
    static std::atomic<int> creationCount_;
    static std::atomic<int> destructionCount_;
    static std::atomic<int> workCount_;
};

std::atomic<int> TestResource::creationCount_{0};
std::atomic<int> TestResource::destructionCount_{0};
std::atomic<int> TestResource::workCount_{0};

// Environment variable RAII helper
class EnvGuard {
public:
    explicit EnvGuard(const char* name) : name_(name) {
        if (const char* val = std::getenv(name)) {
            saved_ = std::string(val);
        }
    }

    ~EnvGuard() {
        if (saved_.has_value()) {
            setenv(name_.c_str(), saved_.value().c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

private:
    std::string name_;
    std::optional<std::string> saved_;
};
} // namespace

// =============================================================================
// SocketServer Lifecycle Tests
// =============================================================================

TEST_CASE("SocketServer: Lifecycle management", "[daemon][components][socket]") {
    auto runtimeDir = makeTempRuntimeDir("socket-lifecycle-" + randomSuffix());
    auto socketPath = runtimeDir / "ipc.sock";

    SocketServer::Config config;
    config.socketPath = socketPath;
    config.connectionTimeout = 1500ms;

    StateComponent state;
    state.stats.startTime = std::chrono::steady_clock::now();

    SECTION("Start and stop clears stopping flag") {
        WorkCoordinator coordinator;
        coordinator.start(2);
        SocketServer server(config, &coordinator, nullptr, &state);

        auto first = server.start();
        if (!first) {
            UNSCOPED_INFO(first.error().message);
        }
        if (!first && isPermissionDenied(first)) {
            SKIP("UNIX domain sockets not permitted on this system");
        }
        REQUIRE(first);

        auto stopped = server.stop();
        REQUIRE(stopped);

        // Second start should work (stopping flag cleared)
        auto second = server.start();
        if (!second) {
            UNSCOPED_INFO(second.error().message);
        }
        if (!second && isPermissionDenied(second)) {
            SKIP("UNIX domain sockets not permitted on this system");
        }
        REQUIRE(second);

        REQUIRE(server.stop());
        coordinator.stop();
        coordinator.join();
    }

    SECTION("Multiple start attempts are safe") {
        WorkCoordinator coordinator;
        coordinator.start(2);
        SocketServer server(config, &coordinator, nullptr, &state);

        auto first = server.start();
        if (!first) {
            UNSCOPED_INFO(first.error().message);
        }
        if (!first && isPermissionDenied(first)) {
            SKIP("UNIX domain sockets not permitted on this system");
        }
        REQUIRE(first);

        // Second start while running should fail gracefully
        auto second = server.start();
        REQUIRE(!second); // Already running

        REQUIRE(server.stop());
        coordinator.stop();
        coordinator.join();
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(runtimeDir, ec);
}

// =============================================================================
// Socket Path Resolution Tests
// =============================================================================

TEST_CASE("Socket path resolution: Priority ordering", "[daemon][components][socket][utils]") {
    auto tempDir = makeTempRuntimeDir("socket-utils-" + randomSuffix());
    auto configBase = tempDir / "config";
    auto configDir = configBase / "yams";
    fs::create_directories(configDir);

    // Save environment
    EnvGuard yamsSockGuard("YAMS_DAEMON_SOCKET");
    EnvGuard xdgRuntimeGuard("XDG_RUNTIME_DIR");
    EnvGuard xdgConfigGuard("XDG_CONFIG_HOME");
    EnvGuard homeGuard("HOME");

    SECTION("Environment variable has highest priority") {
        auto testPath = "/tmp/custom-daemon.sock";
        setenv("YAMS_DAEMON_SOCKET", testPath, 1);

        auto result = socket_utils::resolve_socket_path();
        REQUIRE(result.string() == testPath);

        auto resultConfigFirst = socket_utils::resolve_socket_path_config_first();
        REQUIRE(resultConfigFirst.string() == testPath);
    }

    SECTION("XDG_RUNTIME_DIR used when env var not set") {
        unsetenv("YAMS_DAEMON_SOCKET");

        auto xdgDir = tempDir / "runtime";
        fs::create_directories(xdgDir);
        setenv("XDG_RUNTIME_DIR", xdgDir.string().c_str(), 1);

        auto result = socket_utils::resolve_socket_path();
        REQUIRE(result.parent_path() == xdgDir);
        REQUIRE(result.filename() == "yams-daemon.sock");
    }

    SECTION("/tmp fallback for non-root when XDG not available") {
        unsetenv("YAMS_DAEMON_SOCKET");
        unsetenv("XDG_RUNTIME_DIR");

        if (::geteuid() != 0) {
            auto result = socket_utils::resolve_socket_path();
            REQUIRE(result.parent_path() == "/tmp");
            // Socket name includes UID for non-root users
            auto uid = std::to_string(::getuid());
            REQUIRE(result.filename() == "yams-daemon-" + uid + ".sock");
        }
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

TEST_CASE("Socket path resolution: Config file reading", "[daemon][components][socket][utils]") {
    auto tempDir = makeTempRuntimeDir("socket-config-" + randomSuffix());
    auto configBase = tempDir / "config";
    auto configDir = configBase / "yams";
    fs::create_directories(configDir);

    auto configFile = configDir / "config.toml";

    EnvGuard xdgConfigGuard("XDG_CONFIG_HOME");
    EnvGuard yamsSockGuard("YAMS_DAEMON_SOCKET");

    SECTION("Config file socket_path is used") {
        std::ofstream out(configFile);
        out << "[daemon]\n";
        out << "socket_path = \"/opt/yams/daemon.sock\"\n";
        out.close();

        setenv("XDG_CONFIG_HOME", configBase.string().c_str(), 1);
        unsetenv("YAMS_DAEMON_SOCKET");

        auto result = socket_utils::resolve_socket_path_config_first();
        REQUIRE(result.string() == "/opt/yams/daemon.sock");
    }

    SECTION("Invalid config falls back to default") {
        std::ofstream out(configFile);
        out << "invalid toml content ][[\n";
        out.close();

        setenv("XDG_CONFIG_HOME", configBase.string().c_str(), 1);
        unsetenv("YAMS_DAEMON_SOCKET");

        // Should fall back to default resolution (doesn't crash)
        auto result = socket_utils::resolve_socket_path_config_first();
        REQUIRE(!result.empty());
    }

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
}

// =============================================================================
// Resource Pool Tests
// =============================================================================

TEST_CASE("ResourcePool: Basic lifecycle", "[daemon][components][pool]") {
    TestResource::resetCounters();
    std::atomic<int> resourceIdCounter{0};

    PoolConfig<TestResource> config;
    config.minSize = 3;
    config.maxSize = 5;
    config.maxIdle = 3;
    config.idleTimeout = 1s;
    config.acquisitionTimeout = 1s;
    config.validateOnAcquire = true;
    config.preCreateResources = true;

    SECTION("Pool pre-creates minimum resources") {
        using namespace yams;
        auto pool = std::make_unique<ResourcePool<TestResource>>(
            config,
            [&resourceIdCounter](const std::string&) -> Result<std::shared_ptr<TestResource>> {
                return Result<std::shared_ptr<TestResource>>(
                    std::make_shared<TestResource>(++resourceIdCounter));
            },
            [](const TestResource& r) { return r.isValid(); });

        std::this_thread::sleep_for(50ms); // Allow async creation
        REQUIRE(TestResource::getCreationCount() >= static_cast<int>(config.minSize));
    }

    TestResource::resetCounters();
    resourceIdCounter = 0;

    SECTION("Resource acquisition and release") {
        using namespace yams;
        auto pool = std::make_unique<ResourcePool<TestResource>>(
            config,
            [&resourceIdCounter](const std::string&) -> Result<std::shared_ptr<TestResource>> {
                return Result<std::shared_ptr<TestResource>>(
                    std::make_shared<TestResource>(++resourceIdCounter));
            },
            [](const TestResource& r) { return r.isValid(); });

        std::this_thread::sleep_for(50ms);

        // Acquire resource (no ID parameter, timeout is optional)
        auto handleResult = pool->acquire();
        REQUIRE(handleResult.has_value());
        auto& handle = handleResult.value();
        REQUIRE(handle.isValid());
        REQUIRE(handle->isValid());

        // Release by destroying handle (handle goes out of scope)
    }

    TestResource::resetCounters();
    resourceIdCounter = 0;

    SECTION("Invalid resources are replaced") {
        using namespace yams;
        auto pool = std::make_unique<ResourcePool<TestResource>>(
            config,
            [&resourceIdCounter](const std::string&) -> Result<std::shared_ptr<TestResource>> {
                return Result<std::shared_ptr<TestResource>>(
                    std::make_shared<TestResource>(++resourceIdCounter));
            },
            [](const TestResource& r) { return r.isValid(); });

        std::this_thread::sleep_for(50ms);

        // Acquire and invalidate
        {
            auto handleResult = pool->acquire();
            REQUIRE(handleResult.has_value());
            auto& handle = handleResult.value();
            handle->invalidate();
            // handle goes out of scope and releases
        }

        // Next acquisition should get valid resource (invalid one discarded)
        auto handle2Result = pool->acquire();
        REQUIRE(handle2Result.has_value());
        auto& handle2 = handle2Result.value();
        REQUIRE(handle2->isValid());
    }
}

TEST_CASE("ResourcePool: Concurrency", "[daemon][components][pool][concurrent]") {
    TestResource::resetCounters();
    std::atomic<int> resourceIdCounter{0};

    PoolConfig<TestResource> config;
    config.minSize = 2;
    config.maxSize = 10;
    config.maxIdle = 5;
    config.idleTimeout = std::chrono::seconds(1);
    config.acquisitionTimeout = std::chrono::seconds(1);
    config.validateOnAcquire = true;
    config.preCreateResources = true;

    using namespace yams;
    auto pool = std::make_unique<ResourcePool<TestResource>>(
        config,
        [&resourceIdCounter](const std::string&) -> Result<std::shared_ptr<TestResource>> {
            return Result<std::shared_ptr<TestResource>>(
                std::make_shared<TestResource>(++resourceIdCounter));
        },
        [](const TestResource& r) { return r.isValid(); });

    std::this_thread::sleep_for(50ms);

    SECTION("Concurrent acquisition and release") {
        std::vector<std::thread> threads;
        std::atomic<int> successCount{0};

        for (int i = 0; i < 20; ++i) {
            threads.emplace_back([&pool, &successCount]() {
                auto handleResult = pool->acquire();
                if (handleResult.has_value()) {
                    auto& handle = handleResult.value();
                    handle->doWork();
                    successCount++;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        // All threads should successfully acquire resources
        REQUIRE(successCount == 20);
        REQUIRE(TestResource::getWorkCount() == 20);
    }
}

#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/global_io_context.h>
