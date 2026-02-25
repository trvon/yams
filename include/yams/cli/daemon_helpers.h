#pragma once

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/app/services/retrieval_service.h>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/client/ipc_failure.h>
#include <yams/daemon/client/sandbox_detection.h>
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

        // Do not eagerly connect here; AsioTransportAdapter manages connections per-request.

        auto e = std::make_shared<Entry>();
        e->client = std::move(client);
        e->last_used = std::chrono::steady_clock::now();
        e->in_use = false;
        e->id = next_id_++;

        entries_.emplace_back(e);
        if (cfg_.verbose) {
            spdlog::debug("[DaemonClientPool] Created and connected client {}",
                          entries_.back()->id);
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

        // Connection check is not needed here. The double connection issue has been
        // fixed in DaemonClient::sendRequest() and DaemonClient::sendRequestStreaming()
        // by skipping the legacy connect() call when using AsioTransportAdapter.
        //
        // AsioTransportAdapter creates and manages its own boost::asio connections,
        // so calling the POSIX-based connect() here would create an unused connection
        // that immediately EOFs, causing spurious entries in the daemon logs.

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
            std::unique_lock<std::mutex> lk(mtx_);
            if (cv_.wait_for(lk, 1s, [this] { return stop_; })) {
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

template <typename TRequest, typename TResponse = yams::daemon::ResponseOfT<TRequest>>
class PooledRequestManager {
public:
    using RenderFunc = std::function<Result<void>(const TResponse&)>;

    explicit PooledRequestManager(DaemonClientPool::Config pool_cfg = DaemonClientPool::Config{})
        : pool_(std::make_unique<DaemonClientPool>(std::move(pool_cfg))) {}

    // Synchronous execute method - DEPRECATED and only for test compatibility
    // This will return NotImplemented in production code
    Result<void> execute(const TRequest& /*req*/, std::function<Result<void>()> fallback,
                         RenderFunc /*render*/) {
        // Synchronous bridge has been removed; prefer execute_async.
        // Use fallback if provided; otherwise signal not implemented.
        if (fallback)
            return fallback();
        return Error{ErrorCode::NotImplemented, "Synchronous execute() is not available"};
    }

    // Async (coroutine) execution path using pooled DaemonClient::call<TRequest>()
    // Mirrors the sync logic but avoids blocking during the daemon roundtrip.
    [[nodiscard]] boost::asio::awaitable<Result<void>>
    execute_async(const TRequest& req, std::function<Result<void>()> fallback, RenderFunc render) {
        auto acquire_start = std::chrono::steady_clock::now();
        auto lease_res = pool_->acquire();
        auto acquire_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - acquire_start);

        if (!lease_res) {
            spdlog::warn("[PooledRequestManager] Pool acquire failed after {} ms: {}",
                         acquire_ms.count(), lease_res.error().message);
            try {
                co_return fallback ? fallback()
                                   : Error{ErrorCode::ResourceBusy, "Pool unavailable"};
            } catch (...) {
                co_return Error{ErrorCode::InternalError, "Fallback threw an exception"};
            }
        }

        auto lease = std::move(lease_res).value();
        const auto client_id = lease.client_id();

        auto is_transient = [](const Error& e) {
            // Prefer stable classification when available
            if (auto kindOpt = yams::daemon::parseIpcFailureKind(e.message))
                return yams::daemon::isTransient(*kindOpt);
            // Back-compat: treat transport/network as transient
            if (e.code == ErrorCode::NetworkError)
                return true;
            // Treat startup/initialization as transient
            if (e.code == ErrorCode::InvalidState || e.code == ErrorCode::NotInitialized) {
                const std::string& m = e.message;
                return m.find("not ready") != std::string::npos ||
                       m.find("not available") != std::string::npos ||
                       m.find("initializ") != std::string::npos; // initializing/initialization
            }
            // Backpressure/overload from daemon should be retried with backoff
            if (e.code == ErrorCode::RateLimited || e.code == ErrorCode::ResourceExhausted)
                return true;
            return false;
        };

        constexpr int kMaxAttempts = 3;
        std::chrono::milliseconds backoff{75};
        Error last_err{ErrorCode::Unknown, "unknown"};
        auto exec = co_await boost::asio::this_coro::executor;
        for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
            auto t0 = std::chrono::steady_clock::now();
            auto resp = co_await lease->call<TRequest>(req);
            auto t1 = std::chrono::steady_clock::now();
            auto call_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

            if (pool_->config().verbose) {
                spdlog::debug("[PooledRequestManager/async] client={} waited={}ms roundtrip={}ms "
                              "deadline={}ms attempt={}",
                              client_id, lease.waited().count(), call_ms.count(),
                              pool_->config().client_config.requestTimeout.count(), attempt);
            }

            if (resp) {
                co_return render(resp.value());
            }

            last_err = resp.error();
            const bool network = (last_err.code == ErrorCode::NetworkError);
            const bool is_reset = (last_err.message.find("Connection reset") != std::string::npos ||
                                   last_err.message.find("Broken pipe") != std::string::npos ||
                                   last_err.message.find("EPIPE") != std::string::npos ||
                                   last_err.message.find("ECONNRESET") != std::string::npos);
            if (is_reset) {
                spdlog::debug("[PooledRequestManager/async] client {} attempt {}: {}", client_id,
                              attempt, last_err.message);
            } else {
                spdlog::warn(
                    "[PooledRequestManager/async] Daemon call error on client {} (attempt {}): {}",
                    client_id, attempt, last_err.message);
            }
            if (network) {
                pool_->mark_bad(client_id);
            }

            if (attempt < kMaxAttempts && is_transient(last_err)) {
                // Determine backoff using daemon's retryAfterMs when available
                std::chrono::milliseconds retry_delay = backoff;
                // Env override has highest priority
                if (const char* env_ms = std::getenv("YAMS_RETRY_AFTER_MS")) {
                    long v = std::strtol(env_ms, nullptr, 10);
                    if (v > 0)
                        retry_delay = std::chrono::milliseconds{v};
                } else {
                    // Probe daemon status for retryAfterMs hint
                    yams::daemon::StatusRequest sreq{};
                    sreq.detailed = false;
                    if (auto sresp = co_await lease->call<yams::daemon::StatusRequest>(sreq)) {
                        const auto ms = sresp.value().retryAfterMs;
                        if (ms > 0)
                            retry_delay = std::chrono::milliseconds{ms};
                    }
                }
                // Jitter and clamp
                auto jitter = std::chrono::milliseconds{(std::rand() % 41)}; // 0-40ms
                auto delay = std::min<std::chrono::milliseconds>(retry_delay + jitter,
                                                                 std::chrono::milliseconds{2000});
                boost::asio::steady_timer timer(exec, delay);
                co_await timer.async_wait(boost::asio::use_awaitable);
                // Exponential backoff floor, but cap growth
                backoff = std::min(backoff * 2, std::chrono::milliseconds{500});
                continue;
            }

            break; // non-transient or exhausted
        }

        daemon_failures_.fetch_add(1, std::memory_order_relaxed);
        const bool is_network = (last_err.code == ErrorCode::NetworkError);
        spdlog::error("[PooledRequestManager/async] Daemon call failed on client {}: {}{}.",
                      client_id, last_err.message, is_network ? " (network)" : "");
        if (is_network) {
            pool_->mark_bad(client_id);
        }

        try {
            if (!fallback) {
                spdlog::error(
                    "[PooledRequestManager/async] No fallback provided; surfacing daemon error");
                co_return last_err;
            }
            auto fb = fallback();
            if (fb) {
                fallbacks_succeeded_.fetch_add(1, std::memory_order_relaxed);
                spdlog::info(
                    "[PooledRequestManager/async] Recovered from daemon failure via fallback");
                co_return fb;
            }
            fallbacks_failed_.fetch_add(1, std::memory_order_relaxed);
            spdlog::error(
                "[PooledRequestManager/async] Fallback path failed after daemon error: {}",
                fb.error().message);
            co_return fb;
        } catch (...) {
            fallbacks_failed_.fetch_add(1, std::memory_order_relaxed);
            co_return Error{ErrorCode::InternalError, "Fallback threw an exception"};
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

inline bool operator==(const yams::daemon::ClientConfig& lhs,
                       const yams::daemon::ClientConfig& rhs) {
    return lhs.socketPath == rhs.socketPath && lhs.dataDir == rhs.dataDir &&
           lhs.connectTimeout == rhs.connectTimeout && lhs.headerTimeout == rhs.headerTimeout &&
           lhs.bodyTimeout == rhs.bodyTimeout && lhs.requestTimeout == rhs.requestTimeout &&
           lhs.maxRetries == rhs.maxRetries && lhs.autoStart == rhs.autoStart &&
           lhs.enableCircuitBreaker == rhs.enableCircuitBreaker &&
           lhs.enableChunkedResponses == rhs.enableChunkedResponses &&
           lhs.maxChunkSize == rhs.maxChunkSize && lhs.maxInflight == rhs.maxInflight &&
           lhs.progressiveOutput == rhs.progressiveOutput &&
           lhs.singleUseConnections == rhs.singleUseConnections &&
           lhs.disableStreamingForLargeQueries == rhs.disableStreamingForLargeQueries &&
           lhs.acceptCompressed == rhs.acceptCompressed && lhs.transportMode == rhs.transportMode;
}

inline bool operator!=(const yams::daemon::ClientConfig& lhs,
                       const yams::daemon::ClientConfig& rhs) {
    return !(lhs == rhs);
}

inline std::mutex& cli_pool_mutex() {
    static std::mutex gMutex;
    return gMutex;
}

inline std::shared_ptr<DaemonClientPool>& cli_pool_instance() {
    static std::shared_ptr<DaemonClientPool> gPool;
    return gPool;
}

inline yams::daemon::ClientConfig& cli_pool_config() {
    static yams::daemon::ClientConfig gConfig;
    return gConfig;
}

inline bool& cli_pool_initialized() {
    static bool initialized = false;
    return initialized;
}

enum class CliDaemonAccessPolicy {
    AllowInProcessFallback,
    RequireSocket,
};

struct CliDaemonClientPlan {
    yams::daemon::ClientConfig config{};
    yams::daemon::ClientTransportMode resolvedMode{yams::daemon::ClientTransportMode::Auto};
    bool requireSocket{false};
    bool allowInProcessFallback{true};
    bool allowLocalServiceFallback{false};
    bool allowSocketAutoStart{false};
    bool usedInProcessFallback{false};
    std::string fallbackReason;
};

struct CliDaemonClientLease {
    std::shared_ptr<DaemonClientPool::Lease> lease;
    CliDaemonClientPlan plan;
};

namespace detail {

inline std::string to_lower_copy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

inline std::string trim_copy(std::string_view sv) {
    std::size_t start = 0;
    std::size_t end = sv.size();
    while (start < end && std::isspace(static_cast<unsigned char>(sv[start])) != 0) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(sv[end - 1])) != 0) {
        --end;
    }
    return std::string(sv.substr(start, end - start));
}

inline bool is_socket_mode_forced_by_env() {
    const char* raw = std::getenv("YAMS_EMBEDDED");
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    auto mode = to_lower_copy(trim_copy(raw));
    return mode == "0" || mode == "false" || mode == "off" || mode == "no" || mode == "socket" ||
           mode == "daemon";
}

inline Result<void> ensure_socket_daemon_ready(
    const yams::daemon::ClientConfig& cfg,
    std::chrono::milliseconds readyTimeout = std::chrono::milliseconds{10000}) {
    auto effectiveSocket = cfg.socketPath.empty()
                               ? yams::daemon::DaemonClient::resolveSocketPathConfigFirst()
                               : cfg.socketPath;

    if (!yams::daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
        if (!cfg.autoStart) {
            return Error{ErrorCode::NotInitialized, "Daemon is not running"};
        }

        yams::daemon::ClientConfig startCfg;
        startCfg.socketPath = effectiveSocket;
        startCfg.dataDir = cfg.dataDir;
        if (auto r = yams::daemon::DaemonClient::startDaemon(startCfg); !r) {
            return Error{ErrorCode::InternalError,
                         std::string("Failed to start daemon: ") + r.error().message};
        }
    }

    if (readyTimeout.count() <= 0) {
        return Result<void>();
    }

    const auto deadline = std::chrono::steady_clock::now() + readyTimeout;
    auto sleepFor = std::chrono::milliseconds(50);
    while (std::chrono::steady_clock::now() < deadline) {
        if (yams::daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
            return Result<void>();
        }
        auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            break;
        }
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        std::this_thread::sleep_for(std::min(sleepFor, remaining));
        sleepFor = std::min(sleepFor * 2, std::chrono::milliseconds(500));
    }

    return Error{ErrorCode::Timeout, "Daemon did not become ready in time"};
}

} // namespace detail

inline Result<CliDaemonClientPlan> prepare_cli_daemon_client_plan(
    const yams::daemon::ClientConfig& requestedCfg,
    CliDaemonAccessPolicy policy = CliDaemonAccessPolicy::AllowInProcessFallback,
    std::chrono::milliseconds readyTimeout = std::chrono::milliseconds{-1}) {
    CliDaemonClientPlan plan;
    plan.config = requestedCfg;

    const bool socketForcedByEnv = detail::is_socket_mode_forced_by_env();
    const bool requireSocket =
        socketForcedByEnv || (policy == CliDaemonAccessPolicy::RequireSocket);
    plan.requireSocket = requireSocket;
    plan.allowInProcessFallback = !requireSocket;
    plan.allowLocalServiceFallback = false;
    plan.allowSocketAutoStart = requireSocket;

    if (!requireSocket) {
        // For normal CLI flows, avoid daemon auto-start storms and prefer in-process fallback.
        plan.config.autoStart = false;
    }

    auto effectiveReadyTimeout = readyTimeout;
    if (effectiveReadyTimeout.count() < 0) {
        effectiveReadyTimeout =
            requireSocket ? std::chrono::milliseconds(10000) : std::chrono::milliseconds(1200);
    }

    const auto resolvedMode = yams::daemon::resolve_transport_mode(plan.config);
    plan.resolvedMode = resolvedMode;
    plan.config.transportMode = resolvedMode;

    if (resolvedMode == yams::daemon::ClientTransportMode::InProcess && requireSocket) {
        return Error{ErrorCode::InvalidState,
                     "Socket transport required by configuration (YAMS_EMBEDDED)"};
    }

    if (resolvedMode == yams::daemon::ClientTransportMode::Socket) {
        auto ready = detail::ensure_socket_daemon_ready(plan.config, effectiveReadyTimeout);
        if (!ready) {
            if (requireSocket) {
                return ready.error();
            }
            plan.usedInProcessFallback = true;
            plan.fallbackReason = ready.error().message;
            plan.resolvedMode = yams::daemon::ClientTransportMode::InProcess;
            plan.config.transportMode = yams::daemon::ClientTransportMode::InProcess;
            plan.config.autoStart = false;
        }
    }

    return plan;
}

inline void
apply_cli_daemon_plan_to_retrieval_options(const CliDaemonClientPlan& plan,
                                           yams::app::services::RetrievalOptions& opts) {
    opts.transportMode = plan.resolvedMode;
    opts.autoStart = plan.config.autoStart;
    if (!plan.config.socketPath.empty()) {
        opts.socketPath = plan.config.socketPath;
    }
    if (!plan.config.dataDir.empty()) {
        opts.explicitDataDir = plan.config.dataDir;
    }
}

inline bool is_transport_failure(const yams::Error& err) {
    if (err.code == ErrorCode::Timeout) {
        return true;
    }
    return yams::daemon::parseIpcFailureKind(err.message).has_value();
}

#if defined(YAMS_TESTING)
inline void cli_pool_reset_for_test() {
    std::lock_guard<std::mutex> lk(cli_pool_mutex());
    cli_pool_instance().reset();
    cli_pool_initialized() = false;
}
#endif

inline Result<DaemonClientPool::Lease>
acquire_cli_daemon_client(const yams::daemon::ClientConfig& cfg, size_t min_clients = 1,
                          size_t max_clients = 12) {
    auto& gMutex = cli_pool_mutex();
    auto& gPool = cli_pool_instance();
    auto& gConfig = cli_pool_config();
    auto& initialized = cli_pool_initialized();

    std::shared_ptr<DaemonClientPool> poolStrong;
    {
        std::lock_guard<std::mutex> lk(gMutex);
        if (!initialized || cfg != gConfig || !gPool) {
            DaemonClientPool::Config pcfg;
            // Allow environment overrides to modulate pool pressure under heavy ops
            size_t env_min = 0, env_max = 0;
            if (const char* s = std::getenv("YAMS_DAEMON_POOL_MIN")) {
                long v = std::strtol(s, nullptr, 10);
                if (v > 0)
                    env_min = static_cast<size_t>(v);
            }
            if (const char* s = std::getenv("YAMS_DAEMON_POOL_MAX")) {
                long v = std::strtol(s, nullptr, 10);
                if (v > 0)
                    env_max = static_cast<size_t>(v);
            }
            pcfg.min_clients = std::max<size_t>(1, env_min ? env_min : min_clients);
            pcfg.max_clients = std::max(pcfg.min_clients, env_max ? env_max : max_clients);
            pcfg.client_config = cfg;
            gPool = std::make_shared<DaemonClientPool>(pcfg);
            gConfig = cfg;
            initialized = true;
        }
        poolStrong = gPool;
    }

    if (!poolStrong) {
        return Error{ErrorCode::InternalError, "Daemon client pool unavailable"};
    }

    return poolStrong->acquire();
}

inline Result<std::shared_ptr<DaemonClientPool::Lease>>
acquire_cli_daemon_client_shared(const yams::daemon::ClientConfig& cfg, size_t min_clients = 1,
                                 size_t max_clients = 12) {
    auto lease = acquire_cli_daemon_client(cfg, min_clients, max_clients);
    if (!lease)
        return lease.error();

    std::shared_ptr<DaemonClientPool> poolStrong;
    {
        std::lock_guard<std::mutex> lk(cli_pool_mutex());
        poolStrong = cli_pool_instance();
    }

    if (!poolStrong) {
        return Error{ErrorCode::InternalError, "Daemon client pool unavailable"};
    }

    auto sharedLease = std::shared_ptr<DaemonClientPool::Lease>(
        new DaemonClientPool::Lease(std::move(lease.value())),
        [poolStrong = std::move(poolStrong)](DaemonClientPool::Lease* leasePtr) {
            delete leasePtr;
            (void)poolStrong;
        });
    return sharedLease;
}

inline Result<CliDaemonClientLease> acquire_cli_daemon_client_shared_with_policy(
    const yams::daemon::ClientConfig& requestedCfg,
    CliDaemonAccessPolicy policy = CliDaemonAccessPolicy::AllowInProcessFallback,
    size_t min_clients = 1, size_t max_clients = 12,
    std::chrono::milliseconds readyTimeout = std::chrono::milliseconds{-1}) {
    auto planRes = prepare_cli_daemon_client_plan(requestedCfg, policy, readyTimeout);
    if (!planRes) {
        return planRes.error();
    }

    auto plan = std::move(planRes.value());
    auto leaseRes = acquire_cli_daemon_client_shared(plan.config, min_clients, max_clients);
    if (!leaseRes) {
        return leaseRes.error();
    }

    CliDaemonClientLease out;
    out.plan = std::move(plan);
    out.lease = std::move(leaseRes.value());
    return out;
}

inline Result<std::shared_ptr<DaemonClientPool::Lease>>
acquire_cli_daemon_client_shared_with_fallback(
    const yams::daemon::ClientConfig& requestedCfg,
    CliDaemonAccessPolicy policy = CliDaemonAccessPolicy::AllowInProcessFallback,
    size_t min_clients = 1, size_t max_clients = 12,
    std::chrono::milliseconds readyTimeout = std::chrono::milliseconds{-1},
    CliDaemonClientPlan* outPlan = nullptr) {
    auto leaseRes = acquire_cli_daemon_client_shared_with_policy(requestedCfg, policy, min_clients,
                                                                 max_clients, readyTimeout);
    if (!leaseRes) {
        return leaseRes.error();
    }
    auto lease = std::move(leaseRes.value());
    if (outPlan != nullptr) {
        *outPlan = lease.plan;
    }
    return std::move(lease.lease);
}

// Generic runner for any boost::asio::awaitable<Result<T>>. Optional timeout (0 => no timeout).
template <typename T>
inline Result<T> run_result(boost::asio::awaitable<Result<T>> aw,
                            std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
    auto& io = yams::daemon::GlobalIOContext::instance().get_io_context();
    // Use shared_ptr to ensure the promise outlives the coroutine even if we timeout and return
    // early. The coroutine captures a copy of the shared_ptr, preventing use-after-free.
    auto prom = std::make_shared<std::promise<Result<T>>>();
    auto fut = prom->get_future();
    boost::asio::co_spawn(
        io,
        [a = std::move(aw), prom]() mutable -> boost::asio::awaitable<void> {
            try {
                auto r = co_await std::move(a);
                prom->set_value(std::move(r));
            } catch (const std::exception& e) {
                prom->set_value(
                    Error{ErrorCode::InternalError, std::string("Awaitable threw: ") + e.what()});
            } catch (...) {
                prom->set_value(
                    Error{ErrorCode::InternalError, "Awaitable threw unknown exception"});
            }
            co_return;
        },
        boost::asio::detached);

    if (timeout.count() > 0) {
        if (fut.wait_for(timeout) != std::future_status::ready)
            return Error{ErrorCode::Timeout, "Awaitable timed out"};
    } else {
        fut.wait();
    }
    return fut.get();
}

} // namespace yams::cli
