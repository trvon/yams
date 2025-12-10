#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

namespace yams::daemon {

class ServiceManager;
class WorkCoordinator;
struct StateComponent;

// Centralized tuning controller owned by ServiceManager.
// Periodically reads metrics and TuneAdvisor policies and coordinates
// resource allocation across daemon subsystems (IPC CPU/IO pools, writer budgets, etc.).
class TuningManager {
public:
    TuningManager(ServiceManager* sm, StateComponent* state, WorkCoordinator* coordinator);
    ~TuningManager();

    void start();
    void stop();

    // Optional hook: invoked to adjust RepairCoordinator tokens/batch
    void setRepairControlHook(std::function<void(uint32_t tokens, uint32_t batch)> cb) {
        setRepair_ = std::move(cb);
    }

    void setWriterBudgetHook(std::function<void(std::size_t)> cb) {
        setWriterBudget_ = std::move(cb);
    }

private:
    boost::asio::awaitable<void> tuningLoop();
    void tick_once();
    void apply_post_ingest_control(std::size_t queued, std::size_t inflight, std::size_t capacity,
                                   std::uint64_t activeConns);

    ServiceManager* sm_;
    StateComponent* state_;
    WorkCoordinator* coordinator_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::atomic<bool> running_{false};
    std::future<void> tuningFuture_{};

    // Repair tuning helpers (hysteresis + rate limiting)
    std::function<void(uint32_t, uint32_t)> setRepair_{};
    std::chrono::steady_clock::time_point repairBusySince_{};
    std::chrono::steady_clock::time_point repairReadySince_{};
    std::chrono::steady_clock::time_point repairRateWindowStart_{};
    uint64_t repairBatchesAtWindowStart_{0};

    std::function<void(std::size_t)> setWriterBudget_{};

    // PI controller state for post-ingest threads
    std::chrono::steady_clock::time_point lastPiAdjust_{};
    double integratorQueueErr_{0.0};
};

} // namespace yams::daemon
