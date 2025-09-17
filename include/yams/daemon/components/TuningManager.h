#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <yams/compat/thread_stop_compat.h>

namespace yams::daemon {

class ServiceManager;
struct StateComponent;

// Centralized tuning controller owned by ServiceManager.
// Periodically reads metrics and TuneAdvisor policies and coordinates
// resource allocation across daemon subsystems (IPC CPU/IO pools, writer budgets, etc.).
class TuningManager {
public:
    TuningManager(ServiceManager* sm, StateComponent* state);
    ~TuningManager();

    void start();
    void stop();

    // Optional hook: invoked to adjust RepairCoordinator tokens/batch
    void setRepairControlHook(std::function<void(uint32_t tokens, uint32_t batch)> cb) {
        setRepair_ = std::move(cb);
    }

private:
    void tick_once();

    ServiceManager* sm_;
    StateComponent* state_;
    yams::compat::jthread thread_;
    std::atomic<bool> running_{false};

    // Repair tuning helpers (hysteresis + rate limiting)
    std::function<void(uint32_t, uint32_t)> setRepair_{};
    std::chrono::steady_clock::time_point repairBusySince_{};
    std::chrono::steady_clock::time_point repairReadySince_{};
    std::chrono::steady_clock::time_point repairRateWindowStart_{};
    uint64_t repairBatchesAtWindowStart_{0};
};

} // namespace yams::daemon
