#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
#include <stop_token>
#include <thread>

namespace yams {
namespace api {
class IContentStore;
}
namespace metadata {
class MetadataRepository;
}
namespace vector {
class EmbeddingGenerator;
}
} // namespace yams

namespace yams::daemon {

class ServiceManager;
struct StateComponent;
class YamsDaemon;

// Lightweight, feature-flagged background coordinator for repair tasks.
// Non-invasive: derives coarse scheduling hints from server stats (idle vs active).
class RepairCoordinator {
public:
    struct Config {
        bool enable{false};
        std::filesystem::path dataDir{}; // used to locate vectors.db
        std::uint32_t maxBatch{16};      // max docs per tick when idle
        std::uint32_t tickMs{2000};      // coordinator loop interval
        // Stage gating: allow heavier stages only when maintenance tokens are available
        std::uint32_t maintenanceTokens{1}; // number of concurrent heavy-stage tokens
    };

    // activeConnFn returns current active connection count
    RepairCoordinator(ServiceManager* services, StateComponent* state,
                      std::function<size_t()> activeConnFn, Config cfg);
    ~RepairCoordinator();

    void start();
    void stop();

private:
    void run(std::stop_token st);
    bool maintenance_allowed() const; // idle window based on server stats
    // Token gating helpers (inline to avoid ODR/decl mismatches)
    bool try_acquire_token() {
        if (cfg_.maintenanceTokens == 0)
            return true; // tokens disabled => always allowed
        auto cur = tokens_.load();
        while (cur > 0) {
            if (tokens_.compare_exchange_weak(cur, cur - 1)) {
                return true;
            }
        }
        return false;
    }
    void release_token() {
        if (cfg_.maintenanceTokens == 0)
            return; // tokens disabled
        auto cur = tokens_.load();
        while (cur < cfg_.maintenanceTokens) {
            if (tokens_.compare_exchange_weak(cur, cur + 1)) {
                return;
            }
        }
    }
    // Execute a callable only if a maintenance token is available; ensures release.
    // Returns true if the callable was executed.
    bool with_token(const std::function<void()>& fn) {
        if (cfg_.maintenanceTokens == 0) {
            if (fn)
                fn();
            return true;
        }
        if (!try_acquire_token())
            return false;
        try {
            if (fn)
                fn();
        } catch (...) {
            release_token();
            throw;
        }
        release_token();
        return true;
    }

    ServiceManager* services_;
    StateComponent* state_;
    std::function<size_t()> activeConnFn_{};
    Config cfg_{};
    std::atomic<std::uint32_t> tokens_{0};

    std::jthread thread_{};
    std::atomic<bool> running_{false};
};

} // namespace yams::daemon
