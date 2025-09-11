#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/ipc/resource_tuner.h>

namespace yams::daemon {

ResourceTuner& ResourceTuner::instance() {
    static ResourceTuner inst;
    return inst;
}

void ResourceTuner::onBackpressureFlip(const std::string& component, bool on) {
    if (on) {
        // Backpressure ON: suggest increasing workers for component
        PoolManager::Delta d{component, +scale_step_, "backpressure_on", cooldown_ms_};
        PoolManager::instance().apply_delta(d);
    } else {
        // Backpressure OFF: suggest decreasing workers
        PoolManager::Delta d{component, -scale_step_, "backpressure_off", cooldown_ms_};
        PoolManager::instance().apply_delta(d);
    }
}

void ResourceTuner::onTimeout(const std::string& component) {
    // Operation timeout likely indicates need for more capacity
    PoolManager::Delta d{component, +scale_step_, "timeout", cooldown_ms_};
    PoolManager::instance().apply_delta(d);
}

void ResourceTuner::updateLoadHints(double cpuPercent, std::uint64_t muxQueuedBytes,
                                    std::uint64_t workerQueued, std::uint64_t workerThreads,
                                    std::uint64_t activeConnections) {
    // Heuristics:
    // - Idle shrink: no active conns, no queued work, low CPU -> scale down toward min.
    // - Pressure grow: high CPU or significant queues -> scale up.
    auto& pm = PoolManager::instance();

    // Resolve thresholds using TuneAdvisor where possible
    const std::uint64_t maxMux = TuneAdvisor::maxMuxBytes();
    std::uint64_t maxWorkerQ = 0;
    try {
        maxWorkerQ = TuneAdvisor::maxWorkerQueue(static_cast<size_t>(workerThreads));
    } catch (...) {
    }

    const bool idleCpu = (cpuPercent >= 0.0) && (cpuPercent < 5.0);
    const bool noConns = (activeConnections == 0);
    const bool noQueues =
        (workerQueued == 0) && (muxQueuedBytes < (1ull * 1024ull * 1024ull)); // <1 MiB

    if (noConns && noQueues && idleCpu) {
        // Nudge pools down
        pm.apply_delta({"ipc", -scale_step_, "idle_shrink", cooldown_ms_});
        pm.apply_delta({"ipc_io", -scale_step_, "idle_shrink", cooldown_ms_});
        return;
    }

    // Pressure grow
    bool cpuHigh = (cpuPercent > 70.0);
    bool workerQHigh =
        (maxWorkerQ > 0) ? (workerQueued > maxWorkerQ) : (workerQueued > (workerThreads * 2));
    bool muxHigh =
        (maxMux > 0) ? (muxQueuedBytes > maxMux) : (muxQueuedBytes > (64ull * 1024ull * 1024ull));

    if (cpuHigh || workerQHigh || muxHigh || activeConnections > 0) {
        // Scale IPC CPU pool when CPU or worker queue high
        if (cpuHigh || workerQHigh) {
            pm.apply_delta(
                {"ipc", +scale_step_, cpuHigh ? "cpu_high" : "worker_queue_high", cooldown_ms_});
        }
        // Scale IO pool when mux backlog is significant or connections are active
        if (muxHigh || activeConnections > 0) {
            pm.apply_delta(
                {"ipc_io", +scale_step_, muxHigh ? "mux_backlog" : "conns_active", cooldown_ms_});
        }
    }
}

} // namespace yams::daemon
