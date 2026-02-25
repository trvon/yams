#pragma once

#include <filesystem>
#include <memory>

#include <boost/asio/any_io_executor.hpp>

#include <yams/core/types.h>

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
        int initTimeoutSeconds{120};
    };

    static Result<std::shared_ptr<EmbeddedServiceHost>> getOrCreate(const Options& options);

    ~EmbeddedServiceHost();

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
