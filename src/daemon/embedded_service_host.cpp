#include <yams/daemon/embedded_service_host.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/system_executor.hpp>

#include <yams/config/config_helpers.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/ServiceManagerFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/daemon_lifecycle.h>

namespace yams::daemon {
namespace {

class EmbeddedLifecycle final : public IDaemonLifecycle {
public:
    explicit EmbeddedLifecycle(DaemonLifecycleFsm* fsm,
                               std::function<void(bool, bool)> shutdownCallback)
        : fsm_(fsm), shutdownCallback_(std::move(shutdownCallback)) {}

    LifecycleSnapshot getLifecycleSnapshot() const override {
        if (!fsm_) {
            return LifecycleSnapshot{};
        }
        return fsm_->snapshot();
    }

    void setSubsystemDegraded(const std::string& subsystem, bool degraded,
                              const std::string& reason) override {
        if (!fsm_) {
            return;
        }
        fsm_->setSubsystemDegraded(subsystem, degraded, reason);
        if (degraded) {
            fsm_->dispatch(DegradedEvent{});
        } else {
            auto snap = fsm_->snapshot();
            if (snap.state == LifecycleState::Degraded) {
                fsm_->dispatch(HealthyEvent{});
            }
        }
    }

    void onDocumentRemoved(const std::string& /*hash*/) override {
        // No-op in embedded mode; repair service hooks are disabled by default.
    }

    void requestShutdown(bool graceful, bool inTestMode) override {
        (void)graceful;
        (void)inTestMode;
        if (shutdownCallback_) {
            shutdownCallback_(graceful, inTestMode);
        }
    }

private:
    DaemonLifecycleFsm* fsm_{nullptr};
    std::function<void(bool, bool)> shutdownCallback_;
};

std::string normalize_key(const std::filesystem::path& path) {
    if (path.empty()) {
        return "<default>";
    }
    std::error_code ec;
    auto normalized = std::filesystem::weakly_canonical(path, ec);
    if (ec) {
        normalized = path.lexically_normal();
    }
    return normalized.string();
}

} // namespace

class EmbeddedServiceHost::Impl {
public:
    explicit Impl(Options options) : options_(std::move(options)) {}

    Result<void> ensureStarted() {
        std::lock_guard<std::mutex> lk(mutex_);
        if (started_) {
            return Result<void>();
        }

        if (options_.dataDir.empty()) {
            options_.dataDir = yams::config::resolve_data_dir_from_config();
        }

        config_.dataDir = options_.dataDir;
        config_.enableAutoRepair = options_.enableAutoRepair;
        config_.autoLoadPlugins = options_.autoLoadPlugins;
        config_.enableModelProvider = options_.enableModelProvider;

        ioContext_ = std::make_unique<boost::asio::io_context>();
        workGuard_.emplace(boost::asio::make_work_guard(*ioContext_));

        const std::size_t threadCount = std::max<std::size_t>(1, options_.ioThreads);
        ioThreads_.reserve(threadCount);
        for (std::size_t i = 0; i < threadCount; ++i) {
            ioThreads_.emplace_back([ctx = ioContext_.get()]() { ctx->run(); });
        }

        lifecycleFsm_.reset();
        lifecycleFsm_.dispatch(BootstrappedEvent{});
        state_.stats.startTime = std::chrono::steady_clock::now();

        serviceManager_ = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);
        lifecycle_ = std::make_unique<EmbeddedLifecycle>(
            &lifecycleFsm_, [this](bool /*graceful*/, bool /*inTestMode*/) {
                auto result = shutdown();
                if (!result) {
                    spdlog::warn("EmbeddedServiceHost shutdown from lifecycle hook failed: {}",
                                 result.error().message);
                }
            });

        dispatcher_ =
            std::make_unique<RequestDispatcher>(lifecycle_.get(), serviceManager_.get(), &state_);

        if (auto initResult = serviceManager_->initialize(); !initResult) {
            lifecycleFsm_.dispatch(FailureEvent{initResult.error().message});
            return initResult;
        }

        try {
            serviceManager_->startBackgroundTasks();
        } catch (const std::exception& e) {
            spdlog::warn("EmbeddedServiceHost background tasks failed to start: {}", e.what());
        }

        serviceManager_->startAsyncInit();

        auto snap = serviceManager_->waitForServiceManagerTerminalState(
            std::max(5, options_.initTimeoutSeconds));
        if (snap.state == ServiceManagerState::Ready) {
            lifecycleFsm_.dispatch(HealthyEvent{});
        } else if (snap.state == ServiceManagerState::Failed) {
            lifecycleFsm_.dispatch(FailureEvent{snap.lastError});
            return Error{ErrorCode::NotInitialized,
                         snap.lastError.empty() ? "Embedded ServiceManager initialization failed"
                                                : snap.lastError};
        }

        started_ = true;
        return Result<void>();
    }

    Result<void> shutdown() {
        std::lock_guard<std::mutex> lk(mutex_);
        if (stopped_) {
            return Result<void>();
        }

        if (dispatcher_) {
            dispatcher_.reset();
        }
        if (serviceManager_) {
            serviceManager_->shutdown();
            serviceManager_.reset();
        }
        lifecycle_.reset();
        lifecycleFsm_.dispatch(StoppedEvent{});

        if (workGuard_) {
            workGuard_->reset();
            workGuard_.reset();
        }
        if (ioContext_) {
            ioContext_->stop();
        }
        for (auto& t : ioThreads_) {
            if (t.joinable()) {
                t.join();
            }
        }
        ioThreads_.clear();
        ioContext_.reset();

        started_ = false;
        stopped_ = true;
        return Result<void>();
    }

    boost::asio::any_io_executor getExecutor() const {
        if (!ioContext_) {
            return boost::asio::system_executor();
        }
        return ioContext_->get_executor();
    }

    RequestDispatcher* getDispatcher() const { return dispatcher_.get(); }
    ServiceManager* getServiceManager() const { return serviceManager_.get(); }
    StateComponent* getState() { return &state_; }
    IDaemonLifecycle* getLifecycle() const { return lifecycle_.get(); }

private:
    Options options_;
    DaemonConfig config_{};
    StateComponent state_{};
    DaemonLifecycleFsm lifecycleFsm_{};

    std::shared_ptr<ServiceManager> serviceManager_;
    std::unique_ptr<EmbeddedLifecycle> lifecycle_;
    std::unique_ptr<RequestDispatcher> dispatcher_;

    std::unique_ptr<boost::asio::io_context> ioContext_;
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
        workGuard_;
    std::vector<std::thread> ioThreads_;

    bool started_{false};
    bool stopped_{false};
    mutable std::mutex mutex_;
};

EmbeddedServiceHost::EmbeddedServiceHost(const Options& options)
    : impl_(std::make_unique<Impl>(options)) {}

EmbeddedServiceHost::~EmbeddedServiceHost() {
    if (impl_) {
        (void)impl_->shutdown();
    }
}

Result<std::shared_ptr<EmbeddedServiceHost>>
EmbeddedServiceHost::getOrCreate(const Options& options) {
    static std::mutex sMutex;
    static std::unordered_map<std::string, std::weak_ptr<EmbeddedServiceHost>> sHosts;

    Options resolved = options;
    if (resolved.dataDir.empty()) {
        resolved.dataDir = yams::config::resolve_data_dir_from_config();
    }
    const std::string key = normalize_key(resolved.dataDir);

    {
        std::lock_guard<std::mutex> lk(sMutex);
        auto it = sHosts.find(key);
        if (it != sHosts.end()) {
            if (auto existing = it->second.lock()) {
                return existing;
            }
        }
    }

    auto host = std::shared_ptr<EmbeddedServiceHost>(new EmbeddedServiceHost(resolved));
    if (auto started = host->impl_->ensureStarted(); !started) {
        return started.error();
    }

    {
        std::lock_guard<std::mutex> lk(sMutex);
        sHosts[key] = host;
    }

    return host;
}

boost::asio::any_io_executor EmbeddedServiceHost::getExecutor() const {
    return impl_->getExecutor();
}

RequestDispatcher* EmbeddedServiceHost::getDispatcher() const {
    return impl_->getDispatcher();
}

ServiceManager* EmbeddedServiceHost::getServiceManager() const {
    return impl_->getServiceManager();
}

StateComponent* EmbeddedServiceHost::getState() const {
    return impl_->getState();
}

IDaemonLifecycle* EmbeddedServiceHost::getLifecycle() const {
    return impl_->getLifecycle();
}

Result<void> EmbeddedServiceHost::shutdown() {
    return impl_->shutdown();
}

} // namespace yams::daemon
