#pragma once

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/cli/asio_client_pool.hpp>
#include <yams/daemon/ipc/response_of.hpp>

namespace yams::cli {

// ============================================================================
// DaemonClientPool: pool of DaemonClient instances with instrumentation
// ============================================================================

class DaemonClientPool {
public:
    struct Config {
        // Client instance bounds
        size_t min_clients = 1;
        size_t max_clients = 8;

        // Pool timing
        std::chrono::milliseconds acquire_timeout{2000}; // wait for free client
        std::chrono::seconds idle_timeout{60};           // pruning threshold

        // Default DaemonClient config applied to created clients
        yams::daemon::ClientConfig client_config{};

        // Logging
        bool verbose = false;
    };

    // Stable pool entry object
    struct Entry {
        std::unique_ptr<yams::daemon::DaemonClient> client;
        std::chrono::steady_clock::time_point last_used{};
        bool in_use = false;
        size_t id = 0;

        Entry() = default;
        Entry(Entry&& other) noexcept
            : client(std::move(other.client)), last_used(other.last_used), in_use(other.in_use),
              id(other.id) {}

        Entry& operator=(Entry&& other) noexcept {
            if (this != &other) {
                client = std::move(other.client);
                last_used = other.last_used;
                in_use = other.in_use;
                id = other.id;
            }
            return *this;
        }

        Entry(const Entry&) = delete;
        Entry& operator=(const Entry&) = delete;
    };

    // RAII lease for a pooled client
    class Lease {
    public:
        Lease() = default;
        Lease(DaemonClientPool* pool, std::shared_ptr<struct Entry> entry,
              yams::daemon::DaemonClient* client, std::chrono::milliseconds waited,
              size_t client_id)
            : pool_(pool), entry_(std::move(entry)), client_(client), waited_(waited),
              client_id_cached_(client_id) {}

        ~Lease() { release(); }

        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;

        Lease(Lease&& other) noexcept : pool_(nullptr), client_(nullptr), waited_(0) {
            move_from(std::move(other));
        }
        Lease& operator=(Lease&& other) noexcept {
            if (this != &other) {
                release();
                move_from(std::move(other));
            }
            return *this;
        }

        bool valid() const { return client_ != nullptr; }
        explicit operator bool() const { return valid(); }

        // Access to pooled client
        yams::daemon::DaemonClient* operator->() { return client_; }
        yams::daemon::DaemonClient& operator*() { return *client_; }
        const yams::daemon::DaemonClient* operator->() const { return client_; }
        const yams::daemon::DaemonClient& operator*() const { return *client_; }

        size_t client_id() const { return client_id_cached_; }
        std::chrono::milliseconds waited() const { return waited_; }

    private:
        void release() {
            if (!pool_ || client_ == nullptr) {
                return;
            }
            // Mark entry free under lock
            {
                std::lock_guard<std::mutex> lk(pool_->mtx_);
                if (entry_) {
                    entry_->in_use = false;
                    entry_->last_used = std::chrono::steady_clock::now();
                }
                pool_->total_releases_++;
            }
            pool_->cv_.notify_one();

            pool_ = nullptr;
            client_ = nullptr;
            entry_.reset();
        }

        void move_from(Lease&& other) noexcept {
            pool_ = other.pool_;
            entry_ = std::move(other.entry_);
            client_ = other.client_;
            waited_ = other.waited_;
            client_id_cached_ = other.client_id_cached_;
            other.pool_ = nullptr;
            other.client_ = nullptr;
            other.waited_ = std::chrono::milliseconds{0};
            other.client_id_cached_ = static_cast<size_t>(-1);
        }

        DaemonClientPool* pool_ = nullptr;
        std::shared_ptr<struct Entry> entry_;
        yams::daemon::DaemonClient* client_ = nullptr;
        std::chrono::milliseconds waited_{0};
        size_t client_id_cached_ = static_cast<size_t>(-1);
    };

    explicit DaemonClientPool(Config cfg) : cfg_(std::move(cfg)), next_id_(1), stop_(false) {
        // Pre-create min_clients
        for (size_t i = 0; i < cfg_.min_clients && i < cfg_.max_clients; ++i) {
            (void)create_entry_unlocked(); // best effort
        }

        // Start pruning thread
        pruning_thread_ = std::thread([this] { pruning_loop(); });
    }

    ~DaemonClientPool() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            stop_ = true;
        }
        cv_.notify_all();
        if (pruning_thread_.joinable()) {
            pruning_thread_.join();
        }
        // Explicitly cleanup
        std::lock_guard<std::mutex> lk(mtx_);
        entries_.clear();
    }

    // Try to acquire a client within pool acquire timeout
    Result<Lease> acquire() { return try_acquire_for(cfg_.acquire_timeout); }

    // Try to acquire a client within provided timeout
    Result<Lease> try_acquire_for(std::chrono::milliseconds timeout) {
        auto start_wait = std::chrono::steady_clock::now();

        std::unique_lock<std::mutex> lk(mtx_);

        // Fast path: find a free entry
        size_t idx = find_free_unlocked_index_unlocked();
        if (idx != npos) {
            auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_wait);
            return make_lease_unlocked(lk, idx, waited);
        }

        // Create a new one if we have capacity
        if (entries_.size() < cfg_.max_clients) {
            size_t idx_new = create_entry_unlocked();
            if (idx_new != npos) {
                auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start_wait);
                return make_lease_unlocked(lk, idx_new, waited);
            }
        }

        // Wait for an entry to become free
        bool woke = cv_.wait_for(
            lk, timeout, [this] { return stop_ || find_free_unlocked_index_unlocked() != npos; });

        if (!woke || stop_) {
            acquire_timeouts_++;
            return Error{ErrorCode::Timeout, "Timed out waiting for daemon client from pool"};
        }

        size_t idx2 = find_free_unlocked_index_unlocked();
        if (idx2 == npos) {
            return Error{ErrorCode::InternalError,
                         "Pool signaled free client but none available (race)"};
        }

        auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_wait);
        return make_lease_unlocked(lk, idx2, waited);
    }

    // Best-effort warm up more clients
    Result<void> warm_up(size_t count) {
        std::lock_guard<std::mutex> lk(mtx_);
        size_t target = std::min(cfg_.max_clients, entries_.size() + count);
        while (entries_.size() < target) {
            if (create_entry_unlocked() == npos) {
                break;
            }
        }
        return Result<void>();
    }

    struct Stats {
        size_t total_clients = 0;
        size_t busy_clients = 0;
        size_t idle_clients = 0;
        size_t total_acquires = 0;
        size_t total_releases = 0;
        size_t acquire_timeouts = 0;
        size_t connect_failures = 0;
    };

    Stats stats() const {
        std::lock_guard<std::mutex> lk(mtx_);
        Stats s;
        s.total_clients = entries_.size();
        for (auto const& e : entries_) {
            if (e->in_use) {
                s.busy_clients++;
            } else {
                s.idle_clients++;
            }
        }
        s.total_acquires = total_acquires_;
        s.total_releases = total_releases_;
        s.acquire_timeouts = acquire_timeouts_;
        s.connect_failures = connect_failures_;
        return s;
    }

    const Config& config() const { return cfg_; }

    // Mark a specific client as bad so that it reconnects on next use.
    void mark_bad(size_t client_id) {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto& e : entries_) {
            if (e->id == client_id) {
                if (e->client) {
                    e->client->disconnect();
                }
                e->last_used = std::chrono::steady_clock::now();
                break;
            }
        }
    }

private:
    static constexpr size_t npos = static_cast<size_t>(-1);

    size_t find_free_unlocked_index_unlocked() const {
        for (size_t i = 0; i < entries_.size(); ++i) {
            if (!entries_[i]->in_use) {
                return i;
            }
        }
        return npos;
    }

    size_t create_entry_unlocked() {
        if (entries_.size() >= cfg_.max_clients) {
            return npos;
        }

        auto client_cfg = cfg_.client_config;
        // Nudge timeouts up slightly to handle bursty conditions
        if (client_cfg.headerTimeout < std::chrono::milliseconds(90000)) {
            client_cfg.headerTimeout = std::chrono::milliseconds(90000);
        }
        if (client_cfg.bodyTimeout < std::chrono::milliseconds(300000)) {
            client_cfg.bodyTimeout = std::chrono::milliseconds(300000);
        }
        auto client = std::make_unique<yams::daemon::DaemonClient>(client_cfg);

        auto e = std::make_shared<Entry>();
        e->client = std::move(client);
        e->last_used = std::chrono::steady_clock::now();
        e->in_use = false;
        e->id = next_id_++;

        entries_.emplace_back(e);
        if (cfg_.verbose) {
            spdlog::debug("[DaemonClientPool] Created client {}", entries_.back()->id);
        }
        return entries_.size() - 1;
    }

    Result<Lease> make_lease_unlocked(std::unique_lock<std::mutex>& lk, size_t idx,
                                      std::chrono::milliseconds waited) {
        // Mark the entry as in use and capture required state while locked.
        auto entry = entries_[idx];
        entry->in_use = true;
        entry->last_used = std::chrono::steady_clock::now();
        total_acquires_++;
        const auto captured_id = entry->id;
        // Capture client pointer while still holding the lock
        yams::daemon::DaemonClient* client_ptr = entry->client.get();

        // We drop the lock during connect; another thread won't see this entry because in_use=true
        lk.unlock();

        // Ensure connection is ready (outside lock)
        if (!client_ptr->isConnected()) {
            auto rc = client_ptr->connect();
            if (!rc) {
                // Failed to connect, mark free and return error
                lk.lock();
                // Use stable shared entry pointer under lock
                entry->in_use = false;
                entry->last_used = std::chrono::steady_clock::now();
                connect_failures_++;
                cv_.notify_one();
                lk.unlock();
                return rc.error();
            }
        }

        if (cfg_.verbose) {
            spdlog::debug("[DaemonClientPool] Acquire client {} (waited {} ms, timeout={} ms, "
                          "request_timeout={} ms)",
                          captured_id, waited.count(), cfg_.acquire_timeout.count(),
                          cfg_.client_config.requestTimeout.count());
        }

        return Lease(this, entry, client_ptr, waited, captured_id);
    }

    void pruning_loop() {
        using namespace std::chrono_literals;
        while (true) {
            {
                std::unique_lock<std::mutex> lk(mtx_);
                if (stop_) {
                    break;
                }

                auto now = std::chrono::steady_clock::now();
                // Keep at least min_clients
                if (entries_.size() > cfg_.min_clients) {
                    // Remove idle entries older than idle_timeout and not in use
                    size_t removed = 0;
                    for (size_t i = 0; i < entries_.size();) {
                        auto& e = entries_[i];
                        if (!e->in_use && (now - e->last_used) > cfg_.idle_timeout &&
                            entries_.size() - removed > cfg_.min_clients) {
                            if (cfg_.verbose) {
                                spdlog::debug("[DaemonClientPool] Pruning idle client {}", e->id);
                            }
                            // erase by swap-pop
                            using std::swap;
                            swap(entries_[i], entries_.back());
                            entries_.pop_back();
                            removed++;
                            continue;
                        }
                        ++i;
                    }
                    (void)removed;
                }
            }
            std::this_thread::sleep_for(1s);
        }
    }

    Config cfg_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<std::shared_ptr<Entry>> entries_;
    std::atomic<size_t> next_id_;
    std::thread pruning_thread_;
    bool stop_;

    // Stats
    size_t total_acquires_ = 0;
    size_t total_releases_ = 0;
    size_t acquire_timeouts_ = 0;
    size_t connect_failures_ = 0;

    friend class Lease;
};

// ============================================================================
// PooledRequestManager<TRequest, TResponse>
// Pools DaemonClient instances and funnels requests through DaemonClient::call
// ============================================================================

// Prefer streaming-aware client calls where available to reduce large single-frame responses
// Simple heuristics decide when to stream to reduce latency and memory for large results.
inline bool should_stream(const yams::daemon::SearchRequest& r) {
    // Env override for search limit threshold
    size_t limit_threshold = 50;
    if (const char* s = std::getenv("YAMS_STREAM_SEARCH_LIMIT")) {
        long v = std::strtol(s, nullptr, 10);
        if (v > 0)
            limit_threshold = static_cast<size_t>(v);
    }
    // Stream when results likely large or progressive output is useful
    return (r.limit > limit_threshold) || (!r.pathsOnly && r.limit > limit_threshold / 2) ||
           r.jsonOutput; // json can be large; stream to avoid big single frame
}
inline bool should_stream(const yams::daemon::ListRequest& r) {
    size_t limit_threshold = 100;
    if (const char* s = std::getenv("YAMS_STREAM_LIST_LIMIT")) {
        long v = std::strtol(s, nullptr, 10);
        if (v > 0)
            limit_threshold = static_cast<size_t>(v);
    }
    return (r.limit > limit_threshold) || r.showSnippets || r.pathsOnly;
}
inline bool should_stream(const yams::daemon::GrepRequest& r) {
    // Grep generally benefits from streaming when scanning many files or printing paths
    size_t file_hint_threshold = 10;
    if (const char* s = std::getenv("YAMS_STREAM_GREP_FILE_HINT")) {
        long v = std::strtol(s, nullptr, 10);
        if (v > 0)
            file_hint_threshold = static_cast<size_t>(v);
    }
    return r.pathsOnly || r.filesOnly || r.countOnly || r.maxMatches > 0 || (!r.path.empty()) ||
           (!r.paths.empty() && r.paths.size() >= file_hint_threshold) || r.recursive;
}
inline Result<yams::daemon::SearchResponse> call_pref(yams::daemon::DaemonClient* /*c*/,
                                                      const yams::daemon::SearchRequest& r) {
    // Synchronous roundtrip via Asio client pool (no Task::get())
    yams::cli::AsioClientPool pool{};
    return pool.call<yams::daemon::SearchRequest, yams::daemon::SearchResponse>(r);
}
inline Result<yams::daemon::ListResponse> call_pref(yams::daemon::DaemonClient* /*c*/,
                                                    const yams::daemon::ListRequest& r) {
    yams::cli::AsioClientPool pool{};
    return pool.call<yams::daemon::ListRequest, yams::daemon::ListResponse>(r);
}
inline Result<yams::daemon::GrepResponse> call_pref(yams::daemon::DaemonClient* /*c*/,
                                                    const yams::daemon::GrepRequest& r) {
    yams::cli::AsioClientPool pool{};
    return pool.call<yams::daemon::GrepRequest, yams::daemon::GrepResponse>(r);
}
template <class Req>
inline Result<yams::daemon::ResponseOfT<Req>> call_pref(yams::daemon::DaemonClient* /*c*/,
                                                        const Req& r) {
    yams::cli::AsioClientPool pool{};
    return pool.call<Req, yams::daemon::ResponseOfT<Req>>(r);
}

template <typename TRequest, typename TResponse = yams::daemon::ResponseOfT<TRequest>>
class PooledRequestManager {
public:
    using RenderFunc = std::function<Result<void>(const TResponse&)>;

    explicit PooledRequestManager(DaemonClientPool::Config pool_cfg = DaemonClientPool::Config{})
        : pool_(std::make_unique<DaemonClientPool>(std::move(pool_cfg))) {}

    // Executes request via a pooled DaemonClient.
    // If pool acquisition fails, fallback() is invoked (caller may perform local handling).
    Result<void> execute(const TRequest& req, std::function<Result<void>()> fallback,
                         RenderFunc render) {
        auto acquire_start = std::chrono::steady_clock::now();
        auto lease_res = pool_->acquire();
        auto acquire_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - acquire_start);

        if (!lease_res) {
            spdlog::warn("[PooledRequestManager] Pool acquire failed after {} ms: {}",
                         acquire_ms.count(), lease_res.error().message);
            try {
                return fallback ? fallback() : Error{ErrorCode::ResourceBusy, "Pool unavailable"};
            } catch (...) {
                return Error{ErrorCode::InternalError, "Fallback threw an exception"};
            }
        }

        auto lease = std::move(lease_res).value();
        const auto client_id = lease.client_id();

        // Transient-aware execute with small internal retries to avoid false fallbacks
        // on brief startup races (metadata repo/content store not yet ready, reconnects, etc.).
    auto is_transient = [](const Error& e) {
            if (e.code == ErrorCode::NetworkError)
        return true; // treat ECONNRESET/EPIPE as transient
            if (e.code == ErrorCode::InvalidState || e.code == ErrorCode::NotInitialized) {
                // Heuristic match on common readiness messages
                const std::string& m = e.message;
                return m.find("not ready") != std::string::npos ||
                       m.find("not available") != std::string::npos ||
                       m.find("initializ") != std::string::npos; // initializing/initialization
            }
            return false;
        };

        constexpr int kMaxAttempts = 3;
        std::chrono::milliseconds backoff{75};
        Error last_err{ErrorCode::Unknown, "unknown"};
        for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
            auto t0 = std::chrono::steady_clock::now();
            auto resp = call_pref(lease.operator->(), req);
            auto t1 = std::chrono::steady_clock::now();
            auto call_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

            if (pool_->config().verbose) {
                spdlog::debug("[PooledRequestManager] client={} waited={}ms roundtrip={}ms "
                              "deadline={}ms attempt={}",
                              client_id, lease.waited().count(), call_ms.count(),
                              pool_->config().client_config.requestTimeout.count(), attempt);
            }

            if (resp) {
                return render(resp.value());
            }

            last_err = resp.error();
            const bool network = (last_err.code == ErrorCode::NetworkError);
            // Downgrade noisy peer-closure errors to debug, keep others at warn
            const bool is_reset = (last_err.message.find("Connection reset") != std::string::npos ||
                                   last_err.message.find("Broken pipe") != std::string::npos ||
                                   last_err.message.find("EPIPE") != std::string::npos ||
                                   last_err.message.find("ECONNRESET") != std::string::npos);
            if (is_reset) {
                spdlog::debug("[PooledRequestManager] client {} attempt {}: {}", client_id,
                              attempt, last_err.message);
            } else {
                spdlog::warn(
                    "[PooledRequestManager] Daemon call error on client {} (attempt {}): {}",
                    client_id, attempt, last_err.message);
            }
            if (network) {
                // Force reconnect on next attempt
                pool_->mark_bad(client_id);
            }

            if (attempt < kMaxAttempts && is_transient(last_err)) {
                // Add tiny jitter to avoid thundering herd
                auto jitter = std::chrono::milliseconds{(std::rand() % 21)}; // 0-20ms
                std::this_thread::sleep_for(backoff + jitter);
                // Exponential backoff up to ~300ms
                backoff = std::min(backoff * 2, std::chrono::milliseconds{300});
                continue; // retry
            }

            // Non-transient or exhausted retries -> go to fallback path
            break;
        }

        // If daemon call failed, log and attempt fallback (e.g., local handling)
        daemon_failures_.fetch_add(1, std::memory_order_relaxed);
        const bool is_network = (last_err.code == ErrorCode::NetworkError);
        spdlog::error("[PooledRequestManager] Daemon call failed on client {}: {}{}.", client_id,
                      last_err.message, is_network ? " (network)" : "");

        if (is_network) {
            pool_->mark_bad(client_id);
        }

        try {
            if (!fallback) {
                spdlog::error(
                    "[PooledRequestManager] No fallback provided; surfacing daemon error");
                return last_err;
            }
            auto fb = fallback();
            if (fb) {
                fallbacks_succeeded_.fetch_add(1, std::memory_order_relaxed);
                spdlog::info("[PooledRequestManager] Recovered from daemon failure via fallback");
                return fb; // success via recovery
            }
            fallbacks_failed_.fetch_add(1, std::memory_order_relaxed);
            spdlog::error("[PooledRequestManager] Fallback path failed after daemon error: {}",
                          fb.error().message);
            return fb; // propagate fallback error
        } catch (...) {
            fallbacks_failed_.fetch_add(1, std::memory_order_relaxed);
            return Error{ErrorCode::InternalError, "Fallback threw an exception"};
        }
    }

    DaemonClientPool& pool() { return *pool_; }
    const DaemonClientPool& pool() const { return *pool_; }

    struct RecoveryStats {
        size_t daemon_failures = 0;
        size_t fallbacks_succeeded = 0;
        size_t fallbacks_failed = 0;
    };

    RecoveryStats recovery_stats() const {
        return RecoveryStats{daemon_failures_.load(std::memory_order_relaxed),
                             fallbacks_succeeded_.load(std::memory_order_relaxed),
                             fallbacks_failed_.load(std::memory_order_relaxed)};
    }

private:
    std::unique_ptr<DaemonClientPool> pool_;
    std::atomic<size_t> daemon_failures_{0};
    std::atomic<size_t> fallbacks_succeeded_{0};
    std::atomic<size_t> fallbacks_failed_{0};
};

// ============================================================================
// Convenience shims
// ============================================================================

// Execute with a provided manager (no fallback)
template <typename TRequest, typename TResponse = yams::daemon::ResponseOfT<TRequest>,
          typename Fallback, typename Render>
inline Result<void> daemon_first_pooled(PooledRequestManager<TRequest, TResponse>& manager,
                                        const TRequest& req, Fallback&& /*unused*/,
                                        Render&& render) {
    return manager.execute(req, [] { return Result<void>(); }, std::forward<Render>(render));
}

// Legacy-like shim that keeps a static pool per TRequest/TResponse
template <typename TRequest, typename TResponse = yams::daemon::ResponseOfT<TRequest>,
          typename Fallback, typename Render>
inline Result<void> daemon_first(const TRequest& req, Fallback&& fallback, Render&& render) {
    // Static pool with basic defaults. Tunable via env if desired.
    static DaemonClientPool::Config cfg = [] {
        DaemonClientPool::Config c;
        if (const char* s = std::getenv("YAMS_POOL_MAX")) {
            long v = std::strtol(s, nullptr, 10);
            if (v > 0)
                c.max_clients = static_cast<size_t>(v);
        }
        if (const char* s = std::getenv("YAMS_POOL_MIN")) {
            long v = std::strtol(s, nullptr, 10);
            if (v > 0)
                c.min_clients = static_cast<size_t>(v);
        }
        if (const char* s = std::getenv("YAMS_POOL_VERBOSE")) {
            c.verbose = (std::string(s) == "1" || std::string(s) == "true");
        }
        return c;
    }();

    static PooledRequestManager<TRequest, TResponse> manager(cfg);
    return manager.execute(req, std::forward<Fallback>(fallback), std::forward<Render>(render));
}

} // namespace yams::cli