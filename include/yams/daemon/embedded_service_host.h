#pragma once

#include <filesystem>
#include <memory>
#ifdef YAMS_TESTING
#include <functional>
#include <optional>
#endif

#if __has_include(<boost/asio/any_io_executor.hpp>)
#include <boost/asio/any_io_executor.hpp>
#elif __has_include("boost/asio/any_io_executor.hpp")
#include "boost/asio/any_io_executor.hpp"
#else
namespace boost {
namespace asio {
class any_io_executor;
} // namespace asio
} // namespace boost
#endif

#if __has_include(<yams/core/types.h>)
#include <yams/core/types.h>
#elif __has_include("yams/core/types.h")
#include "yams/core/types.h"
#else
#include "../core/types.h"
#endif

namespace yams::daemon {

class RequestDispatcher;
class ServiceManager;
struct StateComponent;
class IDaemonLifecycle;

class EmbeddedServiceHost {
public:
    struct Options {
        std::filesystem::path dataDir;
        std::size_t ioThreads{2};
        bool enableAutoRepair{false};
        bool autoLoadPlugins{false};
        bool enableModelProvider{false};
        bool oneShot{false};
        int initTimeoutSeconds{120};
    };

    static Result<std::shared_ptr<EmbeddedServiceHost>> getOrCreate(const Options& options);

    ~EmbeddedServiceHost();

#ifdef YAMS_TESTING
    enum class TestingShutdownPhase {
        BeforeThreadJoin,
        AfterThreadJoin,
    };

    struct TestingShutdownSnapshot {
        TestingShutdownPhase phase;
        bool dispatcherAlive{false};
        bool serviceManagerAlive{false};
        bool lifecycleAlive{false};
        bool ioContextAlive{false};
        std::size_t ioThreadCount{0};
    };

    using TestingShutdownHook = std::function<void(const TestingShutdownSnapshot&)>;

    void testing_setShutdownHook(TestingShutdownHook hook);
    [[nodiscard]] std::optional<TestingShutdownSnapshot>
    testing_getShutdownSnapshot(TestingShutdownPhase phase) const;
#endif

    EmbeddedServiceHost(const EmbeddedServiceHost&) = delete;
    EmbeddedServiceHost& operator=(const EmbeddedServiceHost&) = delete;

    boost::asio::any_io_executor getExecutor() const;
    RequestDispatcher* getDispatcher() const;
    ServiceManager* getServiceManager() const;
    StateComponent* getState() const;
    IDaemonLifecycle* getLifecycle() const;

    Result<void> shutdown();

private:
    explicit EmbeddedServiceHost(const Options& options);

    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace yams::daemon
