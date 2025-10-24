#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/thread_pool.hpp>
#include <yams/compat/thread_stop_compat.h>

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
        std::filesystem::path dataDir{};          // used to locate vectors.db
        std::uint32_t maxBatch{16};               // max docs per batch
        std::uint32_t maintenanceTokens{1};       // number of concurrent heavy-stage tokens
        bool allowDegraded{true};                 // allow limited work when not fully ready
        std::uint32_t maxActiveDuringDegraded{1}; // run with up to N active connections
    };

    // Event types for document operations
    struct DocumentAddedEvent {
        std::string hash;
        std::string path;
    };

    struct DocumentRemovedEvent {
        std::string hash;
    };

    // activeConnFn returns current active connection count
    RepairCoordinator(ServiceManager* services, StateComponent* state,
                      std::function<size_t()> activeConnFn, Config cfg);
    ~RepairCoordinator();

    void start();
    void stop();

    // Event-driven interface - called when documents are added/removed
    void onDocumentAdded(const DocumentAddedEvent& event);
    void onDocumentRemoved(const DocumentRemovedEvent& event);

    // Live tuning hooks (thread-safe best-effort); allow daemon to adapt scheduling
    void setMaintenanceTokens(std::uint32_t tokens) {
        tokens_.store(tokens, std::memory_order_relaxed);
        cfg_.maintenanceTokens = tokens;
    }
    void setMaxBatch(std::uint32_t maxBatch) { cfg_.maxBatch = maxBatch; }

private:
    // Coroutine-based main loop; scheduled on the daemon IO executor
    boost::asio::awaitable<void> runAsync();
    bool maintenance_allowed() const; // idle window based on server stats and degraded policy
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

    // Event queue for document operations
    std::queue<std::string> pendingDocuments_;
    mutable std::mutex queueMutex_;
    std::condition_variable queueCv_;
    std::atomic<bool> running_{false};

    // Coarse progress tracking for indexing/embedding repair so status can reflect background
    // progress. Tracks only the initial backlog; subsequent live additions are not included in
    // the percentage but will still be processed.
    std::atomic<std::uint64_t> totalBacklog_{0};
    std::atomic<std::uint64_t> processed_{0};
    void update_progress_pct();

    // Graceful shutdown coordination for detached coroutine
    struct ShutdownState {
        std::atomic<bool> finished{false};
        std::atomic<bool> running{true}; // Coroutine-owned running flag
        std::mutex mutex;
        std::condition_variable cv;
    };
    std::shared_ptr<ShutdownState> shutdownState_;
};

} // namespace yams::daemon
  // Live tuning hooks (thread-safe best-effort); allow daemon to adapt scheduling
