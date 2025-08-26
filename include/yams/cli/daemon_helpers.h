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
#include <yams/daemon/ipc/connection_pool.h>

#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
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

    // RAII lease for a pooled client
    class Lease {
    public:
        Lease() = default;
        Lease(DaemonClientPool* pool, size_t index, yams::daemon::DaemonClient* client,
              std::chrono::milliseconds waited)
            : pool_(pool), index_(index), client_(client), waited_(waited) {
            if (client_) {
                // Mark in-flight usage
                pool_->entries_[index_].inflight.fetch_add(1, std::memory_order_acq_rel);
                // Light sanity: if >1, we're double-using the same client concurrently
                auto inflight = pool_->entries_[index_].inflight.load(std::memory_order_acquire);
                if (inflight > 1) {
                    spdlog::warn("[DaemonClientPool] Client {} reused concurrently (inflight={}) - "
                                 "this indicates misuse",
                                 pool_->entries_[index_].id, inflight);
                }
            }
        }

        ~Lease() { release(); }

        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;

        Lease(Lease&& other) noexcept
            : pool_(nullptr), index_(static_cast<size_t>(-1)), client_(nullptr), waited_(0) {
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

        size_t client_id() const {
            return pool_ ? pool_->entries_[index_].id : static_cast<size_t>(-1);
        }
        std::chrono::milliseconds waited() const { return waited_; }

    private:
        void release() {
            if (!pool_ || client_ == nullptr) {
                return;
            }
            // Clear in-flight and mark entry free
            auto& e = pool_->entries_[index_];
            e.inflight.store(0, std::memory_order_release);
            {
                std::lock_guard<std::mutex> lk(pool_->mtx_);
                e.in_use = false;
                e.last_used = std::chrono::steady_clock::now();
                pool_->total_releases_++;
            }
            pool_->cv_.notify_one();

            pool_ = nullptr;
            client_ = nullptr;
            index_ = static_cast<size_t>(-1);
        }

        void move_from(Lease&& other) noexcept {
            pool_ = other.pool_;
            index_ = other.index_;
            client_ = other.client_;
            waited_ = other.waited_;
            other.pool_ = nullptr;
            other.index_ = static_cast<size_t>(-1);
            other.client_ = nullptr;
            other.waited_ = std::chrono::milliseconds{0};
        }

        DaemonClientPool* pool_ = nullptr;
        size_t index_ = static_cast<size_t>(-1);
        yams::daemon::DaemonClient* client_ = nullptr;
        std::chrono::milliseconds waited_{0};
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
    };

    Stats stats() const {
        std::lock_guard<std::mutex> lk(mtx_);
        Stats s;
        s.total_clients = entries_.size();
        for (auto const& e : entries_) {
            if (e.in_use) {
                s.busy_clients++;
            } else {
                s.idle_clients++;
            }
        }
        s.total_acquires = total_acquires_;
        s.total_releases = total_releases_;
        return s;
    }

    const Config& config() const { return cfg_; }

private:
    struct Entry {
        std::unique_ptr<yams::daemon::DaemonClient> client;
        std::chrono::steady_clock::time_point last_used{};
        std::atomic<uint32_t> inflight{0};
        bool in_use = false;
        size_t id = 0;

        Entry() = default;
        Entry(Entry&& other) noexcept
            : client(std::move(other.client)), last_used(other.last_used),
              inflight(other.inflight.load(std::memory_order_relaxed)), in_use(other.in_use),
              id(other.id) {}

        Entry& operator=(Entry&& other) noexcept {
            if (this != &other) {
                client = std::move(other.client);
                last_used = other.last_used;
                inflight.store(other.inflight.load(std::memory_order_relaxed),
                               std::memory_order_relaxed);
                in_use = other.in_use;
                id = other.id;
            }
            return *this;
        }

        Entry(const Entry&) = delete;
        Entry& operator=(const Entry&) = delete;
    };

    static constexpr size_t npos = static_cast<size_t>(-1);

    size_t find_free_unlocked_index_unlocked() const {
        for (size_t i = 0; i < entries_.size(); ++i) {
            if (!entries_[i].in_use) {
                return i;
            }
        }
        return npos;
    }

    size_t create_entry_unlocked() {
        if (entries_.size() >= cfg_.max_clients) {
            return npos;
        }

        auto client = std::make_unique<yams::daemon::DaemonClient>(cfg_.client_config);

        Entry e;
        e.client = std::move(client);
        e.last_used = std::chrono::steady_clock::now();
        e.inflight.store(0, std::memory_order_relaxed);
        e.in_use = false;
        e.id = next_id_++;

        entries_.emplace_back(std::move(e));
        if (cfg_.verbose) {
            spdlog::debug("[DaemonClientPool] Created client {}", entries_.back().id);
        }
        return entries_.size() - 1;
    }

    Result<Lease> make_lease_unlocked(std::unique_lock<std::mutex>& lk, size_t idx,
                                      std::chrono::milliseconds waited) {
        auto& e = entries_[idx];
        e.in_use = true;
        e.last_used = std::chrono::steady_clock::now();
        total_acquires_++;
        auto id = e.id;

        // We drop the lock during connect; another thread won't see this entry because in_use=true
        lk.unlock();

        // Ensure connection is ready
        if (!e.client->isConnected()) {
            auto rc = e.client->connect();
            if (!rc) {
                // Failed to connect, mark free and return error
                lk.lock();
                e.in_use = false;
                cv_.notify_one();
                lk.unlock();
                return rc.error();
            }
        }

        if (cfg_.verbose) {
            spdlog::debug("[DaemonClientPool] Acquire client {} (waited {} ms, timeout={} ms, "
                          "request_timeout={} ms)",
                          id, waited.count(), cfg_.acquire_timeout.count(),
                          cfg_.client_config.requestTimeout.count());
        }

        return Lease(this, idx, e.client.get(), waited);
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
                        if (!e.in_use && (now - e.last_used) > cfg_.idle_timeout &&
                            entries_.size() - removed > cfg_.min_clients) {
                            if (cfg_.verbose) {
                                spdlog::debug("[DaemonClientPool] Pruning idle client {}", e.id);
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
    std::vector<Entry> entries_;
    std::atomic<size_t> next_id_;
    std::thread pruning_thread_;
    bool stop_;

    // Stats
    size_t total_acquires_ = 0;
    size_t total_releases_ = 0;

    friend class Lease;
};

// ============================================================================
// PooledRequestManager<TRequest, TResponse>
// Pools DaemonClient instances and funnels requests through DaemonClient::call
// ============================================================================

template <typename TRequest, typename TResponse = yams::daemon::ResponseOfT<TRequest>>
class PooledRequestManager {
public:
    using RenderFunc = std::function<Result<void>(const TResponse&)>;

    explicit PooledRequestManager(DaemonClientPool::Config pool_cfg = DaemonClientPool::Config{})
        : pool_(std::make_unique<DaemonClientPool>(std::move(pool_cfg))) {}
    // Legacy compatibility: construct from ConnectionPool::Config by mapping fields
    explicit PooledRequestManager(yams::daemon::ConnectionPool::Config pool_cfg_legacy) {
        DaemonClientPool::Config mapped;
        mapped.min_clients = pool_cfg_legacy.min_connections;
        mapped.max_clients = pool_cfg_legacy.max_connections;
        mapped.idle_timeout = pool_cfg_legacy.idle_timeout;
        mapped.acquire_timeout = std::chrono::duration_cast<std::chrono::milliseconds>(
            pool_cfg_legacy.connection_timeout);
        // Map a conservative request timeout from connection_timeout as a baseline
        mapped.client_config.requestTimeout = std::chrono::duration_cast<std::chrono::milliseconds>(
            pool_cfg_legacy.connection_timeout);
        pool_ = std::make_unique<DaemonClientPool>(std::move(mapped));
    }

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

        // Instrument request timing
        auto t0 = std::chrono::steady_clock::now();
        auto resp = lease->template call<TRequest>(req);
        auto t1 = std::chrono::steady_clock::now();

        auto call_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);
        if (pool_->config().verbose) {
            spdlog::debug("[PooledRequestManager] client={} waited={}ms roundtrip={}ms "
                          "deadline={}ms",
                          client_id, lease.waited().count(), call_ms.count(),
                          pool_->config().client_config.requestTimeout.count());
        }

        if (!resp) {
            return resp.error();
        }

        return render(resp.value());
    }

    DaemonClientPool& pool() { return *pool_; }
    const DaemonClientPool& pool() const { return *pool_; }

private:
    std::unique_ptr<DaemonClientPool> pool_;
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
inline Result<void> daemon_first(const TRequest& req, Fallback&& /*fallback_unused*/,
                                 Render&& render) {
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
    return manager.execute(req, [] { return Result<void>(); }, std::forward<Render>(render));
}

} // namespace yams::cli