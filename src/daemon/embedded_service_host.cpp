#include "yams/daemon/embedded_service_host.h"

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

#include <atomic>
#include <cstdlib>

#include <yams/config/config_helpers.h>
#include <yams/core/assert.hpp>
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
        config_.configFilePath = yams::config::get_config_path();
        config_.enableAutoRepair = options_.enableAutoRepair;
        config_.autoLoadPlugins = options_.autoLoadPlugins;
        config_.enableModelProvider = options_.enableModelProvider;
        config_.embeddedOneShot = options_.oneShot;

        ioContext_ = std::make_unique<boost::asio::io_context>();
        workGuard_.emplace(boost::asio::make_work_guard(*ioContext_));

        const std::size_t threadCount = std::max<std::size_t>(1, options_.ioThreads);
        ioThreads_.reserve(threadCount);

        // Barrier ensures every IO thread is running before we post any work.
        // On Windows, std::thread can return before the thread has started
        // executing; without this, early async work can execute on a
        // not-yet-running executor and dereference null internal state.
        // Uses an atomic counter rather than std::barrier to avoid
        // TSan false positives in libc++'s barrier implementation.
        {
            std::atomic<std::size_t> threadsStarted{0};
            for (std::size_t i = 0; i < threadCount; ++i) {
                ioThreads_.emplace_back([ctx = ioContext_.get(), &threadsStarted]() {
                    threadsStarted.fetch_add(1, std::memory_order_release);
                    ctx->run();
                });
            }
            while (threadsStarted.load(std::memory_order_acquire) < threadCount) {
                std::this_thread::yield();
            }
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

        std::promise<void> asyncInitCompletedPromise;
        auto asyncInitCompletedFuture = asyncInitCompletedPromise.get_future();
        serviceManager_->startAsyncInit(&asyncInitCompletedPromise, nullptr);

        const auto initTimeoutSeconds = std::max(5, options_.initTimeoutSeconds);
        auto snap = serviceManager_->waitForServiceManagerTerminalState(initTimeoutSeconds);
        if (snap.state == ServiceManagerState::Ready) {
            if (asyncInitCompletedFuture.valid() &&
                asyncInitCompletedFuture.wait_for(std::chrono::seconds(initTimeoutSeconds)) ==
                    std::future_status::timeout) {
                lifecycleFsm_.dispatch(
                    FailureEvent{"Embedded ServiceManager async initialization timed out"});
                return Error{ErrorCode::Timeout,
                             "Embedded ServiceManager async initialization timed out"};
            }
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

        const auto lifecycleBeforeShutdown = lifecycleFsm_.snapshot().state;
        if (lifecycleBeforeShutdown != LifecycleState::Stopping &&
            lifecycleBeforeShutdown != LifecycleState::Stopped &&
            lifecycleBeforeShutdown != LifecycleState::Failed) {
            lifecycleFsm_.dispatch(ShutdownRequestedEvent{});
        }

        if (workGuard_) {
            workGuard_->reset();
            workGuard_.reset();
        }
        if (ioContext_) {
            ioContext_->stop();
        }

        emitTestingShutdownSnapshot(EmbeddedServiceHost::TestingShutdownPhase::BeforeThreadJoin);

        for (auto& t : ioThreads_) {
            if (t.joinable()) {
                YAMS_ASSERT(t.get_id() != std::this_thread::get_id(),
                            "EmbeddedServiceHost shutdown must not join the current IO thread");
                t.join();
            }
        }

        emitTestingShutdownSnapshot(EmbeddedServiceHost::TestingShutdownPhase::AfterThreadJoin);

        const bool hostThreadsJoined =
            std::none_of(ioThreads_.begin(), ioThreads_.end(),
                         [](const std::thread& thread) { return thread.joinable(); });
        YAMS_ASSERT(hostThreadsJoined,
                    "EmbeddedServiceHost must drain host IO threads before releasing request "
                    "dispatcher state");

        ioThreads_.clear();
        ioContext_.reset();

        if (dispatcher_) {
            dispatcher_.reset();
        }
        if (serviceManager_) {
            serviceManager_->shutdown();
            serviceManager_.reset();
        }
        lifecycle_.reset();
        lifecycleFsm_.dispatch(StoppedEvent{});

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
    void setTestingShutdownHook(EmbeddedServiceHost::TestingShutdownHook hook) {
        testingShutdownHook_ = std::move(hook);
    }
    [[nodiscard]] std::optional<EmbeddedServiceHost::TestingShutdownSnapshot>
    getTestingShutdownSnapshot(EmbeddedServiceHost::TestingShutdownPhase phase) const {
        switch (phase) {
            case EmbeddedServiceHost::TestingShutdownPhase::BeforeThreadJoin:
                return beforeThreadJoinSnapshot_;
            case EmbeddedServiceHost::TestingShutdownPhase::AfterThreadJoin:
                return afterThreadJoinSnapshot_;
        }
        return std::nullopt;
    }

private:
    void emitTestingShutdownSnapshot(EmbeddedServiceHost::TestingShutdownPhase phase) {
        EmbeddedServiceHost::TestingShutdownSnapshot snapshot{
            .phase = phase,
            .dispatcherAlive = static_cast<bool>(dispatcher_),
            .serviceManagerAlive = static_cast<bool>(serviceManager_),
            .lifecycleAlive = static_cast<bool>(lifecycle_),
            .ioContextAlive = static_cast<bool>(ioContext_),
            .ioThreadCount = ioThreads_.size(),
        };
        switch (phase) {
            case EmbeddedServiceHost::TestingShutdownPhase::BeforeThreadJoin:
                beforeThreadJoinSnapshot_ = snapshot;
                break;
            case EmbeddedServiceHost::TestingShutdownPhase::AfterThreadJoin:
                afterThreadJoinSnapshot_ = snapshot;
                break;
        }
        if (testingShutdownHook_) {
            testingShutdownHook_(snapshot);
        }
    }

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
    EmbeddedServiceHost::TestingShutdownHook testingShutdownHook_;
    std::optional<EmbeddedServiceHost::TestingShutdownSnapshot> beforeThreadJoinSnapshot_;
    std::optional<EmbeddedServiceHost::TestingShutdownSnapshot> afterThreadJoinSnapshot_;
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

    // Register an atexit handler to drain the static map before CRT begins
    // uncontrolled static destruction. On Windows in particular, static
    // weak_ptr destruction during CRT exit can race with IOCP-backed
    // coroutine frames that still hold shared_ptr references.
    static const int sAtexitRegistered = []() {
        std::atexit([]() {
            // Collect live hosts under the lock, then shut them down outside
            // it: shutdown() joins threads and runs arbitrary teardown, and
            // holding sMutex across that would deadlock any thread that
            // reaches getOrCreate() during exit.
            std::vector<std::shared_ptr<EmbeddedServiceHost>> live;
            {
                std::lock_guard<std::mutex> lk(sMutex);
                live.reserve(sHosts.size());
                for (auto& [key, weak] : sHosts) {
                    if (auto host = weak.lock()) {
                        live.push_back(std::move(host));
                    }
                }
                sHosts.clear();
            }
            for (auto& host : live) {
                (void)host->shutdown();
            }
        });
        return 0;
    }();
    (void)sAtexitRegistered;

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

void EmbeddedServiceHost::testing_setShutdownHook(TestingShutdownHook hook) {
    impl_->setTestingShutdownHook(std::move(hook));
}

std::optional<EmbeddedServiceHost::TestingShutdownSnapshot>
EmbeddedServiceHost::testing_getShutdownSnapshot(TestingShutdownPhase phase) const {
    return impl_->getTestingShutdownSnapshot(phase);
}

} // namespace yams::daemon
