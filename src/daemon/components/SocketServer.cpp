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
} // namespace

#include <spdlog/spdlog.h>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>

#include <atomic>
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

    try {
        spdlog::info("Starting socket server on {}", config_.socketPath.string());

        // Ensure we have at least one IO worker thread; a zero value would prevent
        // the io_context from ever running the accept loop and handlers, leading to
        // client connections that time out on reads.
        if (config_.workerThreads == 0) {
            config_.workerThreads = 1;
            spdlog::warn("SocketServer: workerThreads was 0; coercing to 1");
        }

        // Remove existing socket file
        std::error_code ec;
        std::filesystem::remove(config_.socketPath, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            spdlog::warn("Failed to remove existing socket: {}", ec.message());
        }

        // Ensure parent directory exists
        auto parent = config_.socketPath.parent_path();
        if (!parent.empty() && !std::filesystem::exists(parent)) {
            std::filesystem::create_directories(parent);
        }

        // Create acceptor and bind
#ifndef _WIN32
        // Guard against AF_UNIX sun_path length limit (~108 bytes on Linux)
        {
            std::string sp = config_.socketPath.string();
            if (sp.size() >= sizeof(sockaddr_un::sun_path)) {
                running_ = false;
                return Error{
                    ErrorCode::InvalidArgument,
                    std::string("Socket path too long for AF_UNIX (") + std::to_string(sp.size()) +
                        "/" + std::to_string(sizeof(sockaddr_un::sun_path)) + ") : '" + sp + "'"};
            }
        }
#endif
        // Ensure io_context is fresh and kept alive while we spin up threads
        io_context_.restart();
        work_guard_.emplace(io_context_.get_executor());

        acceptor_ = std::make_unique<local::acceptor>(io_context_);
        local::endpoint endpoint(config_.socketPath.string());
        acceptor_->open(endpoint.protocol());
        acceptor_->bind(endpoint);
        acceptor_->listen(boost::asio::socket_base::max_listen_connections);

        // Set socket permissions
        std::filesystem::permissions(config_.socketPath, std::filesystem::perms::owner_all |
                                                             std::filesystem::perms::group_read |
                                                             std::filesystem::perms::group_write);

        actualSocketPath_ = config_.socketPath;

        // Start accept loop before spawning worker threads to ensure pending work exists
        co_spawn(io_context_, accept_loop(), detached);

        // Clamp worker thread count to centralized recommendation to avoid oversubscription
        try {
            auto rec = TuneAdvisor::recommendedThreads(0.5 /*backgroundFactor*/);
            if (rec > 0) {
                config_.workerThreads = std::max<std::size_t>(
                    1, std::min<std::size_t>(config_.workerThreads, static_cast<std::size_t>(rec)));
            }
        } catch (...) {
        }

        // Start worker threads (exit-aware)
        for (size_t i = 0; i < config_.workerThreads; ++i) {
            auto flag = std::make_shared<std::atomic<bool>>(false);
            IoWorker w{};
            w.exit = flag;
            w.th = std::thread([this, i, flag] {
                set_current_thread_name("yams-ipc-worker-" + std::to_string(i));
                try {
                    while (!stopping_.load(std::memory_order_relaxed) &&
                           !flag->load(std::memory_order_relaxed)) {
                        // Use run_for to efficiently process events for a short duration
                        // before re-checking the stop flag. This avoids busy-spinning.
                        io_context_.run_for(std::chrono::milliseconds(100));
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

        // Start IO reconciler thread (PoolManager-driven autoscaling for IPC IO)
        start_io_reconciler();

        // Update state if available
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

        spdlog::info("Socket server listening on {}", config_.socketPath.string());
        return {};

    } catch (const std::exception& e) {
        running_ = false;
        return Error{ErrorCode::IOError,
                     fmt::format("Failed to start socket server: {}", e.what())};
    }
}

Result<void> SocketServer::stop() {
    if (!running_.exchange(false)) {
        return Error{ErrorCode::InvalidState, "Socket server not running"};
    }

    spdlog::info("Stopping socket server");
    stopping_ = true;

    // Close acceptor first to stop new connections
    if (acceptor_ && acceptor_->is_open()) {
        boost::system::error_code ec;
        acceptor_->close(ec);
        if (ec) {
            spdlog::warn("Error closing acceptor: {}", ec.message());
        }
    }

    // Stop IO reconciler and signal workers to exit
    stop_io_reconciler();
    {
        std::lock_guard<std::mutex> lk(workersMutex_);
        for (auto& w : workers_) {
            if (w.exit)
                w.exit->store(true, std::memory_order_relaxed);
        }
    }
    // Stop io_context and release work guard so threads can exit on next loop turn
    io_context_.stop();

    // Join all worker threads
    for (auto& w : workers_) {
        if (w.th.joinable()) {
            w.th.join();
        }
    }
    workers_.clear();
    work_guard_.reset();

    // Update state
    if (state_) {
        state_->readiness.ipcServerReady.store(false);
    }

    // Remove socket file
    if (!actualSocketPath_.empty()) {
        std::error_code ec;
        std::filesystem::remove(actualSocketPath_, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            spdlog::warn("Failed to remove socket file: {}", ec.message());
        }
        actualSocketPath_.clear();
    }

    spdlog::info("Socket server stopped");
    return {};
}

void SocketServer::start_io_reconciler() {
    try {
        ioReconThread_ = yams::compat::jthread([this](yams::compat::stop_token st) {
            using namespace std::chrono_literals;
            // Best-effort autoscaling loop
            while (!st.stop_requested() && running_.load(std::memory_order_relaxed)) {
                try {
                    // Desired size driven by PoolManager("ipc_io") if available; fallback to
                    // current.
                    std::uint32_t desired = 0;
                    try {
                        desired =
                            yams::daemon::PoolManager::instance().stats("ipc_io").current_size;
                    } catch (...) {
                        desired = 0;
                    }
                    if (desired == 0) {
                        // Initialize at least one IO worker if unconfigured
                        desired = static_cast<std::uint32_t>(
                            std::max<std::size_t>(1, config_.workerThreads));
                    }
                    std::size_t current = 0;
                    {
                        std::lock_guard<std::mutex> lk(workersMutex_);
                        current = workers_.size();
                    }
                    if (desired > current) {
                        // Scale up: spawn additional workers
                        const std::size_t add = desired - current;
                        for (std::size_t i = 0; i < add; ++i) {
                            auto flag = std::make_shared<std::atomic<bool>>(false);
                            IoWorker w{};
                            w.exit = flag;
                            const std::size_t idx = current + i;
                            w.th = std::thread([this, idx, flag] {
                                set_current_thread_name("yams-ipc-worker-" + std::to_string(idx));
                                try {
                                    while (!stopping_.load(std::memory_order_relaxed) &&
                                           !flag->load(std::memory_order_relaxed)) {
                                        boost::asio::steady_timer timer(io_context_);
                                        uint32_t poll_ms = 100;
                                        try {
                                            if (auto snap =
                                                    TuningSnapshotRegistry::instance().get())
                                                poll_ms = snap->workerPollMs;
                                            else
                                                poll_ms = TuneAdvisor::workerPollMs();
                                        } catch (...) {
                                        }
                                        timer.expires_after(std::chrono::milliseconds(poll_ms));
                                        timer.async_wait([](const boost::system::error_code&) {});
                                        io_context_.run_one();
                                        boost::system::error_code ec;
                                        (void)ec;
                                        timer.cancel(ec);
                                    }
                                } catch (const std::exception& e) {
                                    spdlog::error("Worker thread exception: {}", e.what());
                                }
                            });
                            std::lock_guard<std::mutex> lk(workersMutex_);
                            workers_.emplace_back(std::move(w));
                        }
                    } else if (desired < current) {
                        // Scale down: signal some workers to retire and join them
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
                            // Poke the context to wake the thread
                            io_context_.post([] {});
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
        // best-effort only
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
    spdlog::debug("Accept loop started");

    while (running_ && !stopping_) {
#if defined(TRACY_ENABLE)
        ZoneScopedN("SocketServer::accept_loop");
#endif
        bool need_delay = false;
        auto backoff_ms = config_.acceptBackoffMs;

        try {
            // Backpressure: if worker queue or mux bytes exceed thresholds, delay accepts
            try {
                uint32_t delay_ms = 0;
                uint64_t maxWorkerQueue = 0;
                uint64_t maxMuxBytes = TuneAdvisor::maxMuxBytes();
                uint64_t maxActiveConn = TuneAdvisor::maxActiveConn();

                // Derive auto defaults
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
                    uint32_t base = 50;
                    uint32_t extra = 0;
                    if (bp_worker)
                        extra += static_cast<uint32_t>(
                            std::min<uint64_t>(queued - maxWorkerQueue, 1000));
                    if (bp_mux) {
                        uint64_t over = static_cast<uint64_t>(muxQueued) > maxMuxBytes
                                            ? static_cast<uint64_t>(muxQueued) - maxMuxBytes
                                            : 0;
                        extra +=
                            static_cast<uint32_t>(std::min<uint64_t>(over / (256 * 1024), 4000));
                    }
                    if (bp_conn)
                        extra += 200;
                    delay_ms = base + extra;
                }

                if (delay_ms > 0) {
                    boost::asio::steady_timer timer(io_context_);
                    timer.expires_after(std::chrono::milliseconds(delay_ms));
                    co_await timer.async_wait(use_awaitable);
                    continue;
                }
            } catch (...) {
                // ignore backpressure calculation errors
            }
            // Check connection limit
            if (activeConnections_.load() >= config_.maxConnections) {
                // Brief delay when at capacity
                boost::asio::steady_timer timer(io_context_);
                timer.expires_after(std::chrono::milliseconds(100));
                co_await timer.async_wait(use_awaitable);
                continue;
            }

            auto socket = co_await acceptor_->async_accept(use_awaitable);

            auto current = activeConnections_.fetch_add(1) + 1;
            totalConnections_.fetch_add(1);

            spdlog::debug("New connection accepted, active: {}", current);

            // Update metrics
            if (state_) {
                state_->stats.activeConnections.store(current);
            }

            // Handle connection in parallel
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
                // On macOS, AF_UNIX accept can enter a bad state returning EINVAL repeatedly
                // (e.g., after a crash/restart or kqueue race). Rebuild the acceptor after a few
                // hits.
                ++einval_streak;
                spdlog::debug("Accept error (EINVAL): {} (streak={})", e.what(), einval_streak);
                if (einval_streak >= 3) {
                    try {
                        // Close and rebuild acceptor
                        boost::system::error_code ec;
                        if (acceptor_)
                            acceptor_->close(ec);
                        // Remove any stale socket path
                        std::filesystem::remove(config_.socketPath, ec);
                        // Recreate and listen again
                        acceptor_ = std::make_unique<local::acceptor>(io_context_);
                        local::endpoint endpoint(config_.socketPath.string());
                        acceptor_->open(endpoint.protocol());
                        acceptor_->bind(endpoint);
                        acceptor_->listen(boost::asio::socket_base::max_listen_connections);
                        // Mute noisy warnings after the first rebuild; allow opt-in via env
                        static std::atomic<bool> s_warned_once{false};
                        static const bool s_quiet = []() {
                            if (const char* v = std::getenv("YAMS_QUIET_EINVAL_REBUILD")) {
                                return *v != '\0' && std::string(v) != "0" &&
                                       strcasecmp(v, "false") != 0;
                            }
                            return true; // default to quiet (debug) after first occurrence
                        }();
                        if (!s_quiet && !s_warned_once.exchange(true)) {
                            spdlog::warn("Rebuilt IPC acceptor after repeated EINVAL on {}",
                                         config_.socketPath.string());
                        } else {
                            spdlog::debug("Rebuilt IPC acceptor after repeated EINVAL on {}",
                                          config_.socketPath.string());
                        }
                        // Bump recovery metric
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

        // Delay outside of catch block if needed
        if (need_delay) {
            boost::asio::steady_timer timer(io_context_);
            timer.expires_after(backoff_ms);
            try {
                co_await timer.async_wait(use_awaitable);
            } catch (const boost::system::system_error&) {
                // Timer cancelled, continue
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
    // Ensure cleanup on exit
    struct CleanupGuard {
        SocketServer* server;
        ~CleanupGuard() {
            auto current = server->activeConnections_.fetch_sub(1) - 1;
            if (server->state_) {
                server->state_->stats.activeConnections.store(current);
            }
            spdlog::debug("Connection closed, active: {}", current);
        }
    } guard{this};

    try {
        // Create request handler for this connection with streaming support
        RequestHandler::Config handlerConfig;
        handlerConfig.enable_streaming = true;
        handlerConfig.enable_multiplexing = true; // Default: multiplex per connection
        // Wire worker executor from dispatcher so StreamingRequestProcessor can offload
        // heavy compute (search/list/grep and generic requests) off the IO threads.
        if (dispatcher_) {
            try {
                handlerConfig.worker_executor = dispatcher_->getWorkerExecutor();
                handlerConfig.worker_job_signal = dispatcher_->getWorkerJobSignal();
            } catch (...) {
                // Leave defaults if dispatcher cannot provide an executor
            }
        }
        // Chunk size: honor TuneAdvisor; larger chunks improve throughput for uploads/add
        // Default TuneAdvisor::chunkSize is 512 KiB; do not cap to 32 KiB here.
        handlerConfig.chunk_size = TuneAdvisor::chunkSize();
        // Persistent connections; client may issue multiple requests sequentially
        handlerConfig.close_after_response = false;
        handlerConfig.graceful_half_close = true;
        // Increase idle read timeout to 5 minutes to avoid dropping persistent CLI connections
        handlerConfig.read_timeout = std::chrono::seconds(300);
        // Keep in sync with centralized serverMaxInflightPerConn
        handlerConfig.max_inflight_per_connection = TuneAdvisor::serverMaxInflightPerConn();
        handlerConfig.writer_budget_bytes_per_turn = TuneAdvisor::writerBudgetBytesPerTurn();
        MuxMetricsRegistry::instance().setWriterBudget(handlerConfig.writer_budget_bytes_per_turn);
        RequestDispatcher* disp = nullptr;
        {
            std::lock_guard<std::mutex> lk(dispatcherMutex_);
            disp = dispatcher_;
        }
        // If a worker pool is available through the dispatcher, pass its executor BEFORE handler
        // construction
        try {
            if (disp) {
                handlerConfig.worker_executor = disp->getWorkerExecutor();
                handlerConfig.worker_job_signal = disp->getWorkerJobSignal();
            }
        } catch (...) {
        }
        RequestHandler handler(disp, handlerConfig);

        // Create a stop_source for this connection
        yams::compat::stop_token token{};

        // Delegate connection handling to RequestHandler which supports streaming
        co_await handler.handle_connection(std::move(socket), token);
    } catch (const std::exception& e) {
        spdlog::debug("Connection handler error: {}", e.what());
    }

    // Socket will be closed when it goes out of scope
}

} // namespace yams::daemon
