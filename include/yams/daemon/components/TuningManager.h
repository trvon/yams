#pragma once

#include <atomic>
#include <cstdint>
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

private:
    void tick_once();

    ServiceManager* sm_;
    StateComponent* state_;
    yams::compat::jthread thread_;
    std::atomic<bool> running_{false};
};

} // namespace yams::daemon
