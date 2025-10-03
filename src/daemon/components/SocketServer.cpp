#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/request_handler.h>
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif

#ifdef __linux__
#include <sys/prctl.h>
#endif

namespace {
void set_current_thread_name(const std::string& name) {
#ifdef __linux__
    prctl(PR_SET_NAME, name.c_str(), 0, 0, 0);
#elif __APPLE__
    pthread_setname_np(name.c_str());
#endif
}

bool stream_trace_enabled() {
    static int enabled = [] {
        if (const char* raw = std::getenv("YAMS_STREAM_TRACE")) {
            std::string v(raw);
            for (auto& ch : v)
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (v == "1" || v == "true" || v == "on")
                return 1;
        }
        return 0;
    }();
    return enabled != 0;
}

bool socket_run_diag_enabled() {
    static int enabled = [] {
        if (const char* raw = std::getenv("YAMS_SOCKET_RUN_DIAG")) {
            std::string v(raw);
            for (auto& ch : v)
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (v == "1" || v == "true" || v == "on")
                return 1;
        }
        return 0;
    }();
    return enabled != 0;
}
} // namespace

#include <spdlog/spdlog.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>

#include <atomic>
#include <cctype>
#include <cstdlib>
#include <filesystem>

#ifndef _WIN32
#include <sys/un.h>
#endif
#include <memory>

namespace yams::daemon {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using local = boost::asio::local::stream_protocol;

SocketServer::SocketServer(const Config& config, RequestDispatcher* dispatcher,
                           StateComponent* state)
    : config_(config), dispatcher_(dispatcher), state_(state) {}

SocketServer::~SocketServer() {
    stop();
}

Result<void> SocketServer::start() {
    if (running_.exchange(true)) {
        return Error{ErrorCode::InvalidState, "Socket server already running"};
    }

    stopping_.store(false, std::memory_order_relaxed);

    try {
        spdlog::info("Starting socket server on {}", config_.socketPath.string());

        if (config_.workerThreads == 0) {
            config_.workerThreads = 1;
            spdlog::warn("SocketServer: workerThreads was 0; coercing to 1");
        }

        // Normalize to an absolute path to avoid surprises after daemon chdir("/")
        std::filesystem::path sockPath = config_.socketPath;
        if (!sockPath.is_absolute()) {
            try {
                sockPath = std::filesystem::absolute(sockPath);
            } catch (...) {
                // fallback: keep original
            }
        }

        std::error_code ec;
        std::filesystem::remove(sockPath, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            spdlog::warn("Failed to remove existing socket: {}", ec.message());
        }

        auto parent = sockPath.parent_path();
        if (!parent.empty() && !std::filesystem::exists(parent)) {
            std::filesystem::create_directories(parent);
        }

#ifndef _WIN32
        {
            std::string sp = sockPath.string();
            if (sp.size() >= sizeof(sockaddr_un::sun_path)) {
                running_ = false;
                return Error{
                    ErrorCode::InvalidArgument,
                    std::string("Socket path too long for AF_UNIX (") + std::to_string(sp.size()) +
                        "/" + std::to_string(sizeof(sockaddr_un::sun_path)) + ") : '" + sp + "'"};
            }
        }
#endif
        io_context_.restart();
        work_guard_.emplace(io_context_.get_executor());

        acceptor_ = std::make_unique<local::acceptor>(io_context_);
        local::endpoint endpoint(sockPath.string());
        acceptor_->open(endpoint.protocol());
        acceptor_->bind(endpoint);
        acceptor_->listen(boost::asio::socket_base::max_listen_connections);

        std::filesystem::permissions(sockPath, std::filesystem::perms::owner_all |
                                                   std::filesystem::perms::group_read |
                                                   std::filesystem::perms::group_write);

        actualSocketPath_ = sockPath;

        // Seed writer budget prior to first connection
        std::size_t initialBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
        if (initialBudget == 0)
            initialBudget = TuneAdvisor::writerBudgetBytesPerTurn();
        if (initialBudget == 0)
            initialBudget = 256 * 1024;
        setWriterBudget(initialBudget);

        co_spawn(
            io_context_,
            [this]() -> awaitable<void> {
                co_await accept_loop();
                co_return;
            },
            detached);
        spdlog::info("SocketServer: accept_loop scheduled");

        try {
            auto rec = TuneAdvisor::recommendedThreads(0.5 /*backgroundFactor*/);
            if (rec > 0) {
                config_.workerThreads = std::max<std::size_t>(
                    1, std::min<std::size_t>(config_.workerThreads, static_cast<std::size_t>(rec)));
            }
        } catch (...) {
        }

        workerGuards_.reserve(config_.workerThreads);
        for (size_t i = 0; i < config_.workerThreads; ++i) {
            auto flag = std::make_shared<std::atomic<bool>>(false);
            IoWorker w{};
            w.exit = flag;
            workerGuards_.push_back(
                std::make_shared<
                    boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>(
                    io_context_.get_executor()));
            w.th = std::thread([this, i, flag, guard = workerGuards_.back()] {
                set_current_thread_name("yams-ipc-worker-" + std::to_string(i));
                const bool trace = stream_trace_enabled();
                if (trace) {
                    spdlog::info("stream-trace: worker {} entering io_context.run loop", i);
                }
                try {
                    while (!stopping_.load(std::memory_order_relaxed) &&
                           !flag->load(std::memory_order_relaxed)) {
                        auto processed = io_context_.run_one();
                        if (processed == 0) {
                            using namespace std::chrono_literals;
                            std::this_thread::sleep_for(1ms);
                            if (trace) {
                                static thread_local uint32_t idle_ticks = 0;
                                if (++idle_ticks % 500 == 0) {
                                    spdlog::info("stream-trace: worker {} idle", i);
                                }
                            }
                        } else if (trace) {
                            spdlog::info("stream-trace: worker {} handled {} events", i, processed);
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::error("Worker thread exception: {}", e.what());
                }
            });
            {
                std::lock_guard<std::mutex> lk(workersMutex_);
                workers_.emplace_back(std::move(w));
            }
        }

        if (socket_run_diag_enabled()) {
            diagThread_ = std::thread([this] {
                set_current_thread_name("yams-ipc-diag");
                try {
                    spdlog::info("SocketServer diag thread: io_context.run starting");
                    auto handlers = io_context_.run();
                    spdlog::info(
                        "SocketServer diag thread: io_context.run exited handlers={} stopped={}",
                        handlers, io_context_.stopped());
                } catch (const std::exception& e) {
                    spdlog::error("SocketServer diag thread exception: {}", e.what());
                }
            });
        }

        start_io_reconciler();

        if (state_) {
            state_->readiness.ipcServerReady.store(true);
            try {
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - state_->stats.startTime)
                              .count();
                state_->initDurationsMs.emplace("ipc_server", static_cast<uint64_t>(ms));
            } catch (...) {
            }
        }

        spdlog::info("Socket server listening on {}", sockPath.string());
        return {};

    } catch (const std::exception& e) {
        running_ = false;
        return Error{ErrorCode::IOError,
                     fmt::format("Failed to start socket server: {}", e.what())};
    }
}

Result<void> SocketServer::stop() {
    // Wrap entire shutdown sequence to ensure no exception escapes and triggers std::terminate.
    try {
        if (!running_.exchange(false)) {
            return Error{ErrorCode::InvalidState, "Socket server not running"};
        }

        spdlog::info("Stopping socket server");
        stopping_ = true;

        try {
            if (acceptor_ && acceptor_->is_open()) {
                boost::system::error_code ec;
                acceptor_->close(ec);
                if (ec) {
                    spdlog::warn("Error closing acceptor: {}", ec.message());
                }
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception while closing acceptor: {}", e.what());
        } catch (...) {
            spdlog::warn("Unknown exception while closing acceptor");
        }

        // Stop and join the IO reconciler thread BEFORE touching workers_ to avoid
        // concurrent modifications while we iterate/join worker threads.
        try {
            stop_io_reconciler();
        } catch (...) {
        }
        try {
            if (ioReconThread_.joinable()) {
                // Request cooperative stop (no-op on fallback) then join.
                try {
                    ioReconThread_.request_stop();
                } catch (...) {
                }
                try {
                    ioReconThread_.join();
                } catch (const std::exception& e) {
                    spdlog::warn("ioReconThread join exception: {}", e.what());
                } catch (...) {
                    spdlog::warn("ioReconThread join unknown exception");
                }
            }
        } catch (...) {
        }

        // Signal all workers to exit under lock, then stop the io_context.
        try {
            std::lock_guard<std::mutex> lk(workersMutex_);
            for (auto& w : workers_) {
                if (w.exit)
                    w.exit->store(true, std::memory_order_relaxed);
            }
        } catch (...) {
        }
        try {
            io_context_.stop();
        } catch (...) {
        }

        // Move workers to a local list under lock to avoid iterator invalidation
        // if any other component attempts to resize during shutdown (defensive).
        std::vector<IoWorker> localWorkers;
        try {
            std::lock_guard<std::mutex> lk(workersMutex_);
            localWorkers = std::move(workers_);
            workers_.clear();
        } catch (...) {
            // If the move failed, fall back to joining what's present without lock.
        }

        // Join/detach worker threads safely.
        const auto self_id = std::this_thread::get_id();
        auto join_worker = [&](IoWorker& w) {
            if (!w.th.joinable())
                return;
            if (w.th.get_id() == self_id) {
                try {
                    w.th.detach();
                } catch (...) {
                }
            } else {
                try {
                    w.th.join();
                } catch (const std::exception& e) {
                    spdlog::warn("Worker join exception: {}", e.what());
                } catch (...) {
                    spdlog::warn("Worker join unknown exception");
                }
            }
        };

        if (!localWorkers.empty()) {
            for (auto& w : localWorkers) {
                join_worker(w);
            }
        } else {
            // Fallback path if move failed: join workers_ directly (may be empty already)
            for (auto& w : workers_) {
                join_worker(w);
            }
            workers_.clear();
        }
        work_guard_.reset();
        workerGuards_.clear();

        if (diagThread_.joinable()) {
            try {
                diagThread_.join();
            } catch (const std::exception& e) {
                spdlog::warn("Diag thread join exception: {}", e.what());
            } catch (...) {
                spdlog::warn("Diag thread join unknown exception");
            }
        }

        if (state_) {
            state_->readiness.ipcServerReady.store(false);
        }

        if (!actualSocketPath_.empty()) {
            std::error_code ec;
            std::filesystem::remove(actualSocketPath_, ec);
            if (ec && ec != std::errc::no_such_file_or_directory) {
                spdlog::warn("Failed to remove socket file: {}", ec.message());
            }
            actualSocketPath_.clear();
        }

        spdlog::info("Socket server stopped (total_conn={} active_conn={})",
                     totalConnections_.load(std::memory_order_relaxed),
                     activeConnections_.load(std::memory_order_relaxed));
    } catch (const std::exception& e) {
        spdlog::error("SocketServer::stop unhandled exception: {}", e.what());
    } catch (...) {
        spdlog::error("SocketServer::stop unknown exception");
    }
    stopping_.store(false, std::memory_order_relaxed);
    return {};
}

void SocketServer::setWriterBudget(std::size_t bytes) {
    if (bytes == 0)
        bytes = TuneAdvisor::writerBudgetBytesPerTurn();
    if (bytes == 0)
        bytes = 256 * 1024;
    if (!writerBudget_)
        writerBudget_ = std::make_shared<std::atomic<std::size_t>>(bytes);
    else
        writerBudget_->store(bytes, std::memory_order_relaxed);
    MuxMetricsRegistry::instance().setWriterBudget(bytes);
}

void SocketServer::start_io_reconciler() {
    try {
        ioReconThread_ = yams::compat::jthread([this](yams::compat::stop_token st) {
            using namespace std::chrono_literals;
            while (!st.stop_requested() && running_.load(std::memory_order_relaxed)) {
                try {
                    std::uint32_t desired = 0;
                    try {
                        desired =
                            yams::daemon::PoolManager::instance().stats("ipc_io").current_size;
                    } catch (...) {
                        desired = 0;
                    }
                    if (desired == 0) {
                        desired = static_cast<std::uint32_t>(
                            std::max<std::size_t>(1, config_.workerThreads));
                    }
                    std::size_t current = 0;
                    {
                        std::lock_guard<std::mutex> lk(workersMutex_);
                        current = workers_.size();
                    }
                    if (desired > current) {
                        const std::size_t add = desired - current;
                        for (std::size_t i = 0; i < add; ++i) {
                            auto flag = std::make_shared<std::atomic<bool>>(false);
                            IoWorker w{};
                            w.exit = flag;
                            const std::size_t idx = current + i;
                            w.th = std::thread([this, idx, flag] {
                                set_current_thread_name("yams-ipc-worker-" + std::to_string(idx));
                                const bool trace = stream_trace_enabled();
                                try {
                                    while (!stopping_.load(std::memory_order_relaxed) &&
                                           !flag->load(std::memory_order_relaxed)) {
                                        uint32_t poll_ms = 100;
                                        try {
                                            if (auto snap =
                                                    TuningSnapshotRegistry::instance().get())
                                                poll_ms = snap->workerPollMs;
                                            else
                                                poll_ms = TuneAdvisor::workerPollMs();
                                        } catch (...) {
                                        }
                                        auto handled =
                                            io_context_.run_for(std::chrono::milliseconds(poll_ms));
                                        if (trace && handled > 0) {
                                            spdlog::info("stream-trace: worker {} processed {} "
                                                         "handlers (io_stopped={})",
                                                         idx, handled, io_context_.stopped());
                                        }
                                        if (trace && handled == 0) {
                                            static thread_local uint32_t idle_ticks = 0;
                                            if (++idle_ticks % 50 == 0) {
                                                spdlog::info(
                                                    "stream-trace: worker {} idle (io_stopped={})",
                                                    idx, io_context_.stopped());
                                            }
                                        }
                                        if (io_context_.stopped()) {
                                            io_context_.restart();
                                            if (trace) {
                                                spdlog::info(
                                                    "stream-trace: worker {} restarting io_context",
                                                    idx);
                                            }
                                        }
                                    }
                                } catch (const std::exception& e) {
                                    spdlog::error("Worker thread exception: {}", e.what());
                                }
                            });
                            std::lock_guard<std::mutex> lk(workersMutex_);
                            workers_.emplace_back(std::move(w));
                        }
                    } else if (desired < current) {
                        const std::size_t remove = current - desired;
                        for (std::size_t i = 0; i < remove; ++i) {
                            IoWorker victim;
                            {
                                std::lock_guard<std::mutex> lk(workersMutex_);
                                if (workers_.empty())
                                    break;
                                victim = std::move(workers_.back());
                                workers_.pop_back();
                            }
                            if (victim.exit)
                                victim.exit->store(true, std::memory_order_relaxed);
                            boost::asio::post(io_context_, [] {});
                            if (victim.th.joinable())
                                victim.th.join();
                        }
                    }
                } catch (...) {
                }
                std::this_thread::sleep_for(500ms);
            }
        });
    } catch (...) {
    }
}

void SocketServer::stop_io_reconciler() {
    try {
        if (ioReconThread_.joinable()) {
            ioReconThread_.request_stop();
        }
    } catch (...) {
    }
}

awaitable<void> SocketServer::accept_loop() {
    static const bool trace = stream_trace_enabled();
    static bool trace_env_logged = false;
    static bool logged_entry = false;
    spdlog::debug("Accept loop started");
    if (!logged_entry) {
        logged_entry = true;
        spdlog::info("SocketServer: accept_loop coroutine entered");
    }
    if (!trace && !trace_env_logged) {
        trace_env_logged = true;
        if (const char* raw = std::getenv("YAMS_STREAM_TRACE")) {
            spdlog::info("stream-trace: accept_loop env present but disabled ('{}')", raw);
        } else {
            spdlog::info("stream-trace: accept_loop env not set");
        }
    }
    if (trace) {
        spdlog::info("stream-trace: accept_loop starting (max_conn={} worker_threads={} socket={})",
                     config_.maxConnections, config_.workerThreads,
                     actualSocketPath_.empty() ? config_.socketPath.string()
                                               : actualSocketPath_.string());
    }

    while (running_ && !stopping_) {
#if defined(TRACY_ENABLE)
        ZoneScopedN("SocketServer::accept_loop");
#endif
        bool need_delay = false;
        auto backoff_ms = config_.acceptBackoffMs;

        try {
            try {
                uint32_t delay_ms = 0;
                uint64_t maxWorkerQueue = 0;
                uint64_t maxMuxBytes = TuneAdvisor::maxMuxBytes();
                uint64_t maxActiveConn = TuneAdvisor::maxActiveConn();

                if (maxWorkerQueue == 0 && dispatcher_) {
                    try {
                        if (auto sm = dispatcher_->getServiceManager()) {
                            maxWorkerQueue = TuneAdvisor::maxWorkerQueue(sm->getWorkerThreads());
                        }
                    } catch (...) {
                    }
                }
                if (maxMuxBytes == 0)
                    maxMuxBytes = TuneAdvisor::maxMuxBytes();

                uint64_t queued = 0;
                try {
                    if (dispatcher_ && dispatcher_->getServiceManager())
                        queued = dispatcher_->getServiceManager()->getWorkerQueueDepth();
                } catch (...) {
                }
                int64_t muxQueued = 0;
                try {
                    muxQueued = yams::daemon::MuxMetricsRegistry::instance().snapshot().queuedBytes;
                } catch (...) {
                }
                uint64_t activeConn = state_ ? state_->stats.activeConnections.load() : 0;

                bool bp_worker = (maxWorkerQueue > 0 && queued > maxWorkerQueue);
                bool bp_mux = (maxMuxBytes > 0 && muxQueued > static_cast<int64_t>(maxMuxBytes));
                bool bp_conn = (maxActiveConn > 0 && activeConn > maxActiveConn);

                if (bp_worker || bp_mux || bp_conn) {
                    uint32_t base = 5;
                    uint32_t extra = 0;
                    if (bp_worker)
                        extra += 5;
                    if (bp_mux)
                        extra += 5;
                    if (bp_conn)
                        extra += 5;
                    delay_ms = std::min<uint32_t>(base + extra, 20);
                    if (state_) {
                        state_->stats.acceptBackpressureDelays.fetch_add(1,
                                                                         std::memory_order_relaxed);
                    }
                }

                if (delay_ms > 0) {
                    boost::asio::steady_timer timer(io_context_);
                    timer.expires_after(std::chrono::milliseconds(delay_ms));
                    co_await timer.async_wait(use_awaitable);
                    continue;
                }
            } catch (...) {
            }
            if (activeConnections_.load() >= config_.maxConnections) {
                if (trace) {
                    spdlog::info("stream-trace: accept throttled (active={} max={})",
                                 activeConnections_.load(std::memory_order_relaxed),
                                 config_.maxConnections);
                }
                if (state_) {
                    state_->stats.acceptCapacityDelays.fetch_add(1, std::memory_order_relaxed);
                }
                boost::asio::steady_timer timer(io_context_);
                timer.expires_after(std::chrono::milliseconds(20));
                co_await timer.async_wait(use_awaitable);
                continue;
            }

            if (trace) {
                spdlog::info("stream-trace: waiting for accept (active={} total={})",
                             activeConnections_.load(std::memory_order_relaxed),
                             totalConnections_.load(std::memory_order_relaxed));
            }
            auto socket = co_await acceptor_->async_accept(use_awaitable);

            if (trace) {
                spdlog::info("stream-trace: accept completed (active={} total={})",
                             activeConnections_.load(std::memory_order_relaxed),
                             totalConnections_.load(std::memory_order_relaxed));
            }

            auto current = activeConnections_.fetch_add(1) + 1;
            totalConnections_.fetch_add(1);

            spdlog::info("SocketServer: accepted connection, active={} total={}", current,
                         totalConnections_.load());

            if (state_) {
                state_->stats.activeConnections.store(current);
            }

            co_spawn(acceptor_->get_executor(), handle_connection(std::move(socket)), detached);

        } catch (const boost::system::system_error& e) {
            if (!running_ || stopping_)
                break;

            if (e.code() == boost::asio::error::operation_aborted) {
                break;
            }

            static int einval_streak = 0;
#if defined(__APPLE__)
            if (e.code().value() == EINVAL) {
                ++einval_streak;
                spdlog::debug("Accept error (EINVAL): {} (streak={})", e.what(), einval_streak);
                if (einval_streak >= 3) {
                    try {
                        boost::system::error_code ec;
                        if (acceptor_)
                            acceptor_->close(ec);
                        auto rebuildPath = actualSocketPath_;
                        if (rebuildPath.empty()) {
                            rebuildPath = config_.socketPath;
                        }
                        std::filesystem::remove(rebuildPath, ec);
                        acceptor_ = std::make_unique<local::acceptor>(io_context_);
                        local::endpoint endpoint(rebuildPath.string());
                        acceptor_->open(endpoint.protocol());
                        acceptor_->bind(endpoint);
                        acceptor_->listen(boost::asio::socket_base::max_listen_connections);
                        actualSocketPath_ = rebuildPath;
                        static std::atomic<bool> s_warned_once{false};
                        static const bool s_quiet = []() {
                            if (const char* v = std::getenv("YAMS_QUIET_EINVAL_REBUILD")) {
                                return *v != '\0' && std::string(v) != "0" &&
                                       strcasecmp(v, "false") != 0;
                            }
                            return true;
                        }();
                        if (!s_quiet && !s_warned_once.exchange(true)) {
                            spdlog::warn("Rebuilt IPC acceptor after repeated EINVAL on {}",
                                         config_.socketPath.string());
                        } else {
                            spdlog::debug("Rebuilt IPC acceptor after repeated EINVAL on {}",
                                          config_.socketPath.string());
                        }
                        if (state_) {
                            state_->stats.ipcEinvalRebuilds.fetch_add(1, std::memory_order_relaxed);
                        }
                        einval_streak = 0;
                    } catch (const std::exception& re) {
                        spdlog::error("Failed to rebuild IPC acceptor: {}", re.what());
                    }
                }
                backoff_ms = std::chrono::milliseconds(100);
                need_delay = true;
            } else
#endif
            {
                einval_streak = 0;
                spdlog::warn("Accept error: {} ({})", e.what(), e.code().message());
                need_delay = true;
            }
        } catch (const std::exception& e) {
            if (!running_ || stopping_)
                break;
            spdlog::error("Unexpected accept error: {}", e.what());
            break;
        }

        if (need_delay) {
            boost::asio::steady_timer timer(io_context_);
            timer.expires_after(backoff_ms);
            try {
                co_await timer.async_wait(use_awaitable);
            } catch (const boost::system::system_error&) {
            }

            if (!running_ || stopping_)
                break;
        }
    }

    spdlog::debug("Accept loop ended");
}

awaitable<void> SocketServer::handle_connection(local::socket socket) {
#if defined(TRACY_ENABLE)
    ZoneScopedN("SocketServer::handle_connection");
#endif
    static const bool trace = stream_trace_enabled();
    struct CleanupGuard {
        SocketServer* server;
        bool trace;
        ~CleanupGuard() {
            auto current = server->activeConnections_.fetch_sub(1) - 1;
            if (server->state_) {
                server->state_->stats.activeConnections.store(current);
            }
            if (trace) {
                spdlog::info("stream-trace: handle_connection cleanup active={} total={}", current,
                             server->totalConnections_.load());
            } else {
                spdlog::debug("Connection closed, active: {}", current);
            }
        }
    } guard{this, trace};

    if (trace) {
        spdlog::info("stream-trace: handle_connection begin active={} total={} socket_valid={}",
                     activeConnections_.load(std::memory_order_relaxed),
                     totalConnections_.load(std::memory_order_relaxed), socket.is_open());
    }

    try {
        if (!writerBudget_) {
            std::size_t initialBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
            if (initialBudget == 0)
                initialBudget = TuneAdvisor::writerBudgetBytesPerTurn();
            if (initialBudget == 0)
                initialBudget = 256 * 1024;
            writerBudget_ = std::make_shared<std::atomic<std::size_t>>(initialBudget);
        }

        RequestHandler::Config handlerConfig;
        handlerConfig.worker_executor = io_context_.get_executor();
        handlerConfig.writer_budget_ref = writerBudget_;
        handlerConfig.writer_budget_bytes_per_turn = writerBudget_->load(std::memory_order_relaxed);
        handlerConfig.enable_streaming = true;
        handlerConfig.enable_multiplexing = true;
        if (dispatcher_) {
            try {
                handlerConfig.worker_executor = dispatcher_->getWorkerExecutor();
                handlerConfig.worker_job_signal = dispatcher_->getWorkerJobSignal();
            } catch (...) {
            }
        }
        handlerConfig.chunk_size = TuneAdvisor::chunkSize();
        handlerConfig.close_after_response = false;
        handlerConfig.graceful_half_close = true;
        auto connectionTimeout = config_.connectionTimeout;
        if (connectionTimeout.count() == 0) {
            connectionTimeout = std::chrono::milliseconds(30000);
        }
        auto timeoutSeconds = std::chrono::duration_cast<std::chrono::seconds>(connectionTimeout);
        if (timeoutSeconds.count() == 0 && connectionTimeout.count() > 0) {
            timeoutSeconds = std::chrono::seconds(1);
        }
        handlerConfig.read_timeout = timeoutSeconds;
        handlerConfig.write_timeout = timeoutSeconds;
        handlerConfig.max_inflight_per_connection = TuneAdvisor::serverMaxInflightPerConn();
        if (handlerConfig.writer_budget_bytes_per_turn == 0) {
            handlerConfig.writer_budget_bytes_per_turn =
                TuneAdvisor::serverWriterBudgetBytesPerTurn();
            if (handlerConfig.writer_budget_bytes_per_turn == 0)
                handlerConfig.writer_budget_bytes_per_turn =
                    TuneAdvisor::writerBudgetBytesPerTurn();
            if (handlerConfig.writer_budget_bytes_per_turn == 0)
                handlerConfig.writer_budget_bytes_per_turn = 256 * 1024;
            if (writerBudget_)
                writerBudget_->store(handlerConfig.writer_budget_bytes_per_turn,
                                     std::memory_order_relaxed);
        }
        MuxMetricsRegistry::instance().setWriterBudget(handlerConfig.writer_budget_bytes_per_turn);
        RequestDispatcher* disp = nullptr;
        {
            std::lock_guard<std::mutex> lk(dispatcherMutex_);
            disp = dispatcher_;
        }
        try {
            if (disp) {
                handlerConfig.worker_executor = disp->getWorkerExecutor();
                handlerConfig.worker_job_signal = disp->getWorkerJobSignal();
            }
        } catch (...) {
        }
        RequestHandler handler(disp, handlerConfig);

        yams::compat::stop_token token{};

        co_await handler.handle_connection(std::move(socket), token);
    } catch (const std::exception& e) {
        spdlog::error("SocketServer::handle_connection error: {}", e.what());
    }
}

} // namespace yams::daemon
