#include <yams/common/fs_utils.h>
#include <yams/daemon/components/admission_control.h>
#include <yams/daemon/components/AdmissionPolicy.h>
#include <yams/daemon/components/IOCoordinator.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/profiling.h>

namespace {
bool stream_trace_enabled() {
    return false;
}

// Diagnostic thread removed - simplified architecture with fixed worker pool
} // namespace

#include <spdlog/spdlog.h>
#include <atomic>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/write.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>

#ifndef _WIN32
#include <strings.h>
#include <sys/un.h>
#endif
#include <memory>

namespace yams::daemon {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using local = boost::asio::local::stream_protocol;

namespace {
RequestHandler::Config::AdmissionDecision makeSocketBusyDecision(RequestDispatcher* dispatcher,
                                                                 StateComponent* state,
                                                                 std::size_t active,
                                                                 std::size_t limit) {
    const auto overload = admission::captureSnapshot(
        dispatcher ? dispatcher->getServiceManager() : nullptr, state,
        static_cast<std::uint64_t>(active), static_cast<std::uint64_t>(limit));
    const auto decisionCore = admission::makeBusyDecision("Server busy", overload);
    RequestHandler::Config::AdmissionDecision decision;
    decision.code = decisionCore.code;
    decision.message = decisionCore.message;
    decision.retry = ErrorResponse::RetryInfo{decisionCore.retryAfterMs, "overload"};
    return decision;
}
} // namespace

size_t SocketServer::mainActiveConnectionCount() const {
    return mainActiveConnections_.load(std::memory_order_relaxed);
}

size_t SocketServer::connectionSlotsFreeForLimit(size_t limit) const {
    const size_t mainActive = mainActiveConnectionCount();
    return (mainActive < limit) ? (limit - mainActive) : 0;
}

bool SocketServer::mainSocketEmergencyGuardRejects() const {
    const size_t active = mainActiveConnectionCount();
    const size_t softLimit = slotLimit_.load(std::memory_order_relaxed);
    const size_t hardLimit = AdmissionPolicy::emergencySessionLimit(softLimit);
    return active >= hardLimit;
}

void SocketServer::publishConnectionStats(size_t currentActiveConnections,
                                          std::optional<size_t> slotLimitOverride) const {
    if (!state_) {
        return;
    }

    const auto slotLimit =
        slotLimitOverride.value_or(state_->stats.maxConnections.load(std::memory_order_relaxed));
    const auto effectiveLimit = (slotLimit == 0) ? size_t{1024} : slotLimit;
    state_->stats.activeConnections.store(currentActiveConnections, std::memory_order_relaxed);
    state_->stats.connectionSlotsFree.store(connectionSlotsFreeForLimit(effectiveLimit),
                                            std::memory_order_relaxed);
}

SocketAdmissionVerdict SocketServer::evaluateRequestAdmission(const Request& request,
                                                              bool isProxy) const {
    const auto active = isProxy ? proxyActiveConnections_.load(std::memory_order_relaxed)
                                : mainActiveConnectionCount();
    const auto limit = slotLimit_.load(std::memory_order_relaxed);
    return AdmissionPolicy::evaluateSocketAdmission(active, limit, request, isProxy);
}

void SocketServer::testing_setConnectionCounts(size_t mainActive, size_t proxyActive) {
    mainActiveConnections_.store(mainActive, std::memory_order_relaxed);
    proxyActiveConnections_.store(proxyActive, std::memory_order_relaxed);
    activeConnections_.store(mainActive + proxyActive, std::memory_order_relaxed);
}

bool SocketServer::testing_mainSocketEmergencyGuardRejects() const {
    return mainSocketEmergencyGuardRejects();
}

SocketAdmissionVerdict
SocketServer::testing_mainSocketAdmissionVerdict(const Request& request) const {
    return evaluateRequestAdmission(request, false);
}

SocketServer::SocketServer(const Config& config, IOCoordinator* ioCoordinator,
                           WorkCoordinator* coordinator, RequestDispatcher* dispatcher,
                           StateComponent* state)
    : config_(config), ioCoordinator_(ioCoordinator), coordinator_(coordinator),
      dispatcher_(dispatcher), state_(state) {}

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
            yams::common::ensureDirectories(parent);
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
        if (!ioCoordinator_) {
            running_ = false;
            return Error{ErrorCode::InvalidState,
                         "IOCoordinator must be available before SocketServer"};
        }
        if (!coordinator_ || !coordinator_->isRunning()) {
            running_ = false;
            return Error{ErrorCode::InvalidState,
                         "WorkCoordinator must be started before SocketServer"};
        }

        auto io_context = ioCoordinator_->getIOContext();

        acceptor_ = std::make_unique<local::acceptor>(*io_context);
        local::endpoint endpoint(sockPath.string());
        acceptor_->open(endpoint.protocol());
        acceptor_->bind(endpoint);
        acceptor_->listen(boost::asio::socket_base::max_listen_connections);

#ifndef _WIN32
        // Set socket permissions on Unix (Windows Unix sockets don't support filesystem
        // permissions)
        std::filesystem::permissions(sockPath, std::filesystem::perms::owner_all |
                                                   std::filesystem::perms::group_read |
                                                   std::filesystem::perms::group_write);
#endif

        {
            std::lock_guard<std::mutex> lk(socketPathsMutex_);
            actualSocketPath_ = sockPath;
        }

        // Determine initial connection slots: use config if specified, otherwise compute from
        // hardware/profile
        size_t initialSlots = config_.maxConnections;
        if (initialSlots == 0) {
            initialSlots = TuneAdvisor::connectionSlotsTarget();
            spdlog::info("SocketServer: using computed connection slots: {} (from TuneAdvisor)",
                         initialSlots);
        }

        connectionSlots_ = std::make_unique<std::counting_semaphore<>>(initialSlots);
        slotLimit_.store(initialSlots, std::memory_order_relaxed);
        spdlog::info("SocketServer: bounded concurrency enabled (max {} slots)", initialSlots);

        // Proxy gets its own hard cap so it cannot starve main/control connections.
        if (!config_.proxySocketPath.empty()) {
            size_t proxySlots = config_.proxyMaxConnections;
            if (proxySlots == 0) {
                proxySlots = 1;
            }
            proxyConnectionSlots_ = std::make_unique<std::counting_semaphore<>>(proxySlots);
            spdlog::info("SocketServer: proxy bounded concurrency enabled (max {} slots)",
                         proxySlots);
        }

        if (state_) {
            state_->stats.maxConnections.store(initialSlots, std::memory_order_relaxed);
            state_->stats.connectionSlotsFree.store(initialSlots, std::memory_order_relaxed);
            state_->stats.oldestConnectionAge.store(0, std::memory_order_relaxed);
            state_->stats.forcedCloseCount.store(0, std::memory_order_relaxed);
        }

        // Seed writer budget prior to first connection
        std::size_t initialBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
        if (initialBudget == 0)
            initialBudget = TuneAdvisor::writerBudgetBytesPerTurn();
        if (initialBudget == 0)
            initialBudget = 256ULL * 1024;
        setWriterBudget(initialBudget);

        acceptLoopFuture_ = co_spawn(
            ioCoordinator_->getExecutor(),
            [this]() -> awaitable<void> {
                co_await accept_loop(false);
                co_return;
            },
            boost::asio::use_future);
        spdlog::info("SocketServer: accept_loop scheduled on IOCoordinator");

        // Start proxy acceptor if configured
        if (!config_.proxySocketPath.empty()) {
            auto proxySockPath = config_.proxySocketPath;
            if (!proxySockPath.is_absolute()) {
                try {
                    proxySockPath = std::filesystem::absolute(proxySockPath);
                } catch (...) {
                }
            }

            std::error_code proxy_ec;
            std::filesystem::remove(proxySockPath, proxy_ec);
            auto proxyParent = proxySockPath.parent_path();
            if (!proxyParent.empty() && !std::filesystem::exists(proxyParent)) {
                yams::common::ensureDirectories(proxyParent);
            }

#ifndef _WIN32
            {
                std::string psp = proxySockPath.string();
                if (psp.size() >= sizeof(sockaddr_un::sun_path)) {
                    spdlog::warn("Proxy socket path too long for AF_UNIX, skipping proxy: {}", psp);
                    goto skip_proxy;
                }
            }
#endif

            {
                try {
                    proxyAcceptor_ = std::make_unique<local::acceptor>(*io_context);
                    local::endpoint ep(proxySockPath.string());
                    proxyAcceptor_->open(ep.protocol());
                    proxyAcceptor_->bind(ep);
                    proxyAcceptor_->listen(boost::asio::socket_base::max_listen_connections);
                } catch (const std::exception& e) {
                    spdlog::warn("Failed to start proxy acceptor: {}", e.what());
                    goto skip_proxy;
                }

#ifndef _WIN32
                std::filesystem::permissions(proxySockPath,
                                             std::filesystem::perms::owner_all |
                                                 std::filesystem::perms::group_read |
                                                 std::filesystem::perms::group_write);
#endif

                {
                    std::lock_guard<std::mutex> lk(socketPathsMutex_);
                    proxySocketPath_ = proxySockPath;
                }

                proxyAcceptLoopFuture_ = co_spawn(
                    ioCoordinator_->getExecutor(),
                    [this]() -> awaitable<void> {
                        co_await accept_loop(true);
                        co_return;
                    },
                    boost::asio::use_future);
                spdlog::info("SocketServer: proxy accept_loop scheduled on {}",
                             proxySockPath.string());
            }
        skip_proxy:;
        }

        // WorkCoordinator handles all threading - no need for separate worker pool
        spdlog::info("SocketServer: using IOCoordinator ({} threads) for IPC I/O; WorkCoordinator "
                     "({} threads) for CPU",
                     ioCoordinator_->getThreadCount(), coordinator_->getWorkerCount());

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
        spdlog::error("SocketServer::start exception: {}", e.what());
        return Error{ErrorCode::IOError,
                     fmt::format("Failed to start socket server: {}", e.what())};
    }
}

Result<void> SocketServer::stop() {
    try {
        // Always attempt cleanup even when not marked running. This keeps partial
        // startup failures from leaking acceptors bound to an io_context that may
        // be destroyed during daemon teardown.
        const bool wasRunning = running_.exchange(false);

        spdlog::info("Stopping socket server{}", wasRunning ? "" : " (cleanup-only)");
        stopping_.store(true, std::memory_order_relaxed);

        // Request stop on all active connections via stop_source
        try {
            stop_source_.request_stop();
        } catch (...) {
        }

        // Close all active sockets IMMEDIATELY for deterministic shutdown
        try {
            std::vector<std::shared_ptr<TrackedSocket>> sockets;
            {
                std::lock_guard<std::mutex> lk(activeSocketsMutex_);
                for (auto& weak_sock : activeSockets_) {
                    if (auto tracked = weak_sock.lock()) {
                        sockets.push_back(std::move(tracked));
                    }
                }
                activeSockets_.clear();
            }

            const auto closed = close_sockets_on_executor(std::move(sockets));
            spdlog::info("Closed {} active connections", closed);

            auto shutdownDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            {
                std::unique_lock<std::mutex> lock(shutdownMutex_);
                shutdownCv_.wait_until(lock, shutdownDeadline, [this]() {
                    return activeConnections_.load(std::memory_order_relaxed) == 0;
                });
            }
            const auto remaining = activeConnections_.load(std::memory_order_relaxed);
            if (remaining > 0) {
                spdlog::warn("SocketServer: {} active connection(s) still draining during shutdown",
                             remaining);
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception while closing active sockets: {}", e.what());
        } catch (...) {
            spdlog::warn("Unknown exception while closing active sockets");
        }

        // Close proxy acceptor first
        if (proxyAcceptor_) {
            try {
                execute_on_io_context([this]() {
                    if (!proxyAcceptor_)
                        return;
                    boost::system::error_code ec;
                    proxyAcceptor_->cancel(ec);
                    if (proxyAcceptor_->is_open())
                        proxyAcceptor_->close(ec);
                });
            } catch (const std::exception& e) {
                spdlog::warn("SocketServer: failed to close proxy acceptor: {}", e.what());
            }
        }
        if (proxyAcceptLoopFuture_.valid()) {
            try {
                auto status = proxyAcceptLoopFuture_.wait_for(std::chrono::seconds(2));
                if (status == std::future_status::ready) {
                    proxyAcceptLoopFuture_.get();
                }
            } catch (const std::exception& e) {
                spdlog::warn("Proxy accept_loop terminated: {}", e.what());
            }
            proxyAcceptLoopFuture_ = {};
        }
        proxyAcceptor_.reset();

        // Close acceptor on executor thread and wait for accept loop completion
        close_acceptor_on_executor();

        if (acceptLoopFuture_.valid()) {
            try {
                // Wait longer for accept loop to complete gracefully
                auto status = acceptLoopFuture_.wait_for(std::chrono::seconds(3));
                if (status == std::future_status::ready) {
                    acceptLoopFuture_.get();
                    spdlog::info("SocketServer: accept_loop completed");
                } else {
                    spdlog::warn("SocketServer: accept_loop timed out during shutdown after 3s");
                }
            } catch (const std::exception& e) {
                spdlog::warn("SocketServer: accept_loop terminated with exception: {}", e.what());
            } catch (...) {
                spdlog::warn("SocketServer: accept_loop terminated with unknown exception");
            }
            acceptLoopFuture_ = {};
        }

        // Destroy acceptor while WorkCoordinator is still alive to avoid dangling
        // references during later shutdown (e.g., when destructor runs after
        // WorkCoordinator teardown).
        acceptor_.reset();
        connectionSlots_.reset();
        proxyConnectionSlots_.reset();

        // WorkCoordinator manages thread lifecycle - just signal completion
        spdlog::info("SocketServer: accept loop stopped, WorkCoordinator continues running");

        if (state_) {
            state_->readiness.ipcServerReady.store(false);
        }

        // Cleanup socket files (paths are read by other threads; copy+clear under lock)
        std::filesystem::path actualPath;
        std::filesystem::path proxyPath;
        {
            std::lock_guard<std::mutex> lk(socketPathsMutex_);
            actualPath = actualSocketPath_;
            proxyPath = proxySocketPath_;
            actualSocketPath_.clear();
            proxySocketPath_.clear();
        }
        if (!actualPath.empty()) {
            std::error_code ec;
            std::filesystem::remove(actualPath, ec);
        }
        if (!proxyPath.empty()) {
            std::error_code ec;
            std::filesystem::remove(proxyPath, ec);
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
        bytes = 256ULL * 1024;
    if (!writerBudget_)
        writerBudget_ = std::make_shared<std::atomic<std::size_t>>(bytes);
    else
        writerBudget_->store(bytes, std::memory_order_relaxed);
    MuxMetricsRegistry::instance().setWriterBudget(bytes);
}

awaitable<void> SocketServer::accept_loop(bool isProxy) {
    static const bool trace = stream_trace_enabled();
    static std::atomic<bool> logged_entry{false};

    const char* loopLabel = isProxy ? "proxy" : "main";
    auto* activeAcceptor = isProxy ? proxyAcceptor_.get() : acceptor_.get();

    spdlog::debug("{} accept loop started", loopLabel);
    if (!logged_entry.exchange(true, std::memory_order_relaxed)) {
        spdlog::info("SocketServer: accept_loop coroutine entered");
    }
    if (trace) {
        std::string sockStr;
        if (isProxy) {
            sockStr = proxySocketPath().string();
        } else {
            std::filesystem::path p;
            {
                std::lock_guard<std::mutex> lk(socketPathsMutex_);
                p = actualSocketPath_;
            }
            sockStr = p.empty() ? config_.socketPath.string() : p.string();
        }
        spdlog::info("stream-trace: accept_loop starting ({}, max_conn={} socket={})", loopLabel,
                     isProxy ? config_.proxyMaxConnections : config_.maxConnections, sockStr);
    }

    if (!activeAcceptor) {
        spdlog::warn("SocketServer: {} acceptor is null, exiting accept_loop", loopLabel);
        co_return;
    }

    uint32_t noSlotRejectStreak = 0;

    while (running_ && !stopping_) {
        YAMS_ZONE_SCOPED_N("SocketServer::accept_loop");

        // IMPORTANT: Accept first, then try to acquire a slot.
        // If we acquire slots before accept(), we can starve the acceptor/backlog under load.
        std::counting_semaphore<>* slots =
            isProxy ? proxyConnectionSlots_.get() : connectionSlots_.get();
        try {
            // Use as_tuple to avoid exception overhead during shutdown
            auto [ec, socket] =
                co_await activeAcceptor->async_accept(boost::asio::as_tuple(use_awaitable));

            // Handle errors without exception
            if (ec) {
                if (!running_ || stopping_) {
                    break; // Clean shutdown
                }

                if (ec == boost::asio::error::operation_aborted) {
                    break; // Acceptor closed
                }

                // Handle platform-specific errors
                static int einval_streak = 0;
                bool need_delay = false;
                auto backoff_ms = config_.acceptBackoffMs;

#if defined(__APPLE__)
                if (ec.value() == EINVAL) {
                    ++einval_streak;
                    spdlog::debug("Accept error (EINVAL): {} (streak={})", ec.message(),
                                  einval_streak);
                    if (einval_streak >= 3) {
                        try {
                            boost::system::error_code rebuild_ec;
                            if (activeAcceptor)
                                activeAcceptor->close(rebuild_ec);
                            std::filesystem::path rebuildPath;
                            if (isProxy) {
                                rebuildPath = proxySocketPath();
                            } else {
                                {
                                    std::lock_guard<std::mutex> lk(socketPathsMutex_);
                                    rebuildPath = actualSocketPath_;
                                }
                                if (rebuildPath.empty()) {
                                    rebuildPath = config_.socketPath;
                                }
                            }
                            std::filesystem::remove(rebuildPath, rebuild_ec);
                            auto rebuilt =
                                std::make_unique<local::acceptor>(*ioCoordinator_->getIOContext());
                            local::endpoint endpoint(rebuildPath.string());
                            rebuilt->open(endpoint.protocol());
                            rebuilt->bind(endpoint);
                            rebuilt->listen(boost::asio::socket_base::max_listen_connections);
                            if (isProxy) {
                                proxyAcceptor_ = std::move(rebuilt);
                                activeAcceptor = proxyAcceptor_.get();
                                {
                                    std::lock_guard<std::mutex> lk(socketPathsMutex_);
                                    proxySocketPath_ = rebuildPath;
                                }
                            } else {
                                acceptor_ = std::move(rebuilt);
                                activeAcceptor = acceptor_.get();
                                {
                                    std::lock_guard<std::mutex> lk(socketPathsMutex_);
                                    actualSocketPath_ = rebuildPath;
                                }
                            }
                            static std::atomic<bool> s_warned_once{false};
                            if (!s_warned_once.exchange(true)) {
                                spdlog::debug("Rebuilt IPC acceptor after repeated EINVAL on {}",
                                              config_.socketPath.string());
                            }
                            if (state_) {
                                state_->stats.ipcEinvalRebuilds.fetch_add(
                                    1, std::memory_order_relaxed);
                            }
                            einval_streak = 0;
                        } catch (const std::exception& re) {
                            spdlog::error("Failed to rebuild IPC acceptor: {}", re.what());
                        }
                    }
                    need_delay = true;
                    backoff_ms = std::chrono::milliseconds(100);
                } else
#endif
                {
                    einval_streak = 0;
                    spdlog::warn("Accept error: {} ({})", ec.message(), ec.value());
                    need_delay = true;
                }

                if (need_delay) {
                    boost::asio::steady_timer timer(*ioCoordinator_->getIOContext());
                    timer.expires_after(backoff_ms);
                    try {
                        co_await timer.async_wait(use_awaitable);
                    } catch (const boost::system::system_error&) {
                    }
                    if (!running_ || stopping_)
                        break;
                }
                continue; // Retry accept
            }

            // If shutdown begins after accept completes, do not spawn a handler.
            // Close the just-accepted socket.
            if (!running_ || stopping_) {
                boost::system::error_code close_ec;
                socket.close(close_ec);
                break;
            }

            bool slotAcquired = true;
            if (isProxy && slots) {
                slotAcquired = slots->try_acquire();
            }

            if (isProxy && !slotAcquired) {
                noSlotRejectStreak = std::min<uint32_t>(noSlotRejectStreak + 1, 7);
                boost::system::error_code shutdown_ec;
                socket.shutdown(boost::asio::socket_base::shutdown_both, shutdown_ec);
                boost::system::error_code close_ec;
                socket.close(close_ec);
                if (state_) {
                    state_->stats.connectionSlotsFree.store(0, std::memory_order_relaxed);
                }

                if (trace) {
                    spdlog::debug("stream-trace: accept rejected (no slots)");
                } else {
                    static std::atomic<uint64_t> s_noSlotRejects{0};
                    uint64_t n = s_noSlotRejects.fetch_add(1, std::memory_order_relaxed) + 1;
                    if ((n & 0x3FFu) == 0) { // log every 1024th reject
                        spdlog::debug("SocketServer: {} accept rejected (no slots), rejects={}",
                                      loopLabel, n);
                    }
                }

                // Back off adaptively to avoid an accept/close spin loop under sustained
                // overload. This reduces connection churn for short UX requests while still
                // letting the accept loop recover quickly once slots free up.
                const auto maxBackoff = std::max<std::chrono::milliseconds>(
                    config_.acceptBackoffMs, std::chrono::milliseconds(4));
                const auto adaptiveBackoff = std::min<std::chrono::milliseconds>(
                    maxBackoff, std::chrono::milliseconds(1u << noSlotRejectStreak));
                boost::asio::steady_timer slot_timer(*ioCoordinator_->getIOContext());
                slot_timer.expires_after(adaptiveBackoff);
                co_await slot_timer.async_wait(use_awaitable);

                continue;
            }

            noSlotRejectStreak = 0;

            if (!isProxy) {
                if (mainSocketEmergencyGuardRejects()) {
                    const size_t active = mainActiveConnectionCount();
                    const size_t softLimit = slotLimit_.load(std::memory_order_relaxed);
                    const size_t hardLimit = AdmissionPolicy::emergencySessionLimit(softLimit);
                    boost::system::error_code shutdown_ec;
                    socket.shutdown(boost::asio::local::stream_protocol::socket::shutdown_both,
                                    shutdown_ec);
                    boost::system::error_code close_ec;
                    socket.close(close_ec);
                    if (state_) {
                        state_->stats.connectionSlotsFree.store(0, std::memory_order_relaxed);
                        state_->stats.forcedCloseCount.fetch_add(1, std::memory_order_relaxed);
                    }

                    spdlog::debug("SocketServer: {} accept rejected by emergency session guard "
                                  "(active={} hard_limit={} soft_limit={})",
                                  loopLabel, active, hardLimit, softLimit);

                    boost::asio::steady_timer slot_timer(*ioCoordinator_->getIOContext());
                    slot_timer.expires_after(std::max<std::chrono::milliseconds>(
                        config_.acceptBackoffMs, std::chrono::milliseconds(4)));
                    co_await slot_timer.async_wait(use_awaitable);
                    continue;
                }
            }

            // If shutdown begins after slot acquisition, do not spawn a handler.
            if (!running_ || stopping_) {
                boost::system::error_code close_ec;
                socket.close(close_ec);
                if (isProxy && slots) {
                    slots->release();
                }
                break;
            }

            // Generate monotonic connection token for end-to-end tracing
            const uint64_t conn_token = connectionToken_.fetch_add(1, std::memory_order_relaxed);

            if (trace) {
                spdlog::info("stream-trace: [conn={}] slot_acquired", conn_token);
                spdlog::info("stream-trace: [conn={}] accept completed (active={} total={})",
                             conn_token, activeConnections_.load(std::memory_order_relaxed),
                             totalConnections_.load(std::memory_order_relaxed));
            }

            auto current = activeConnections_.fetch_add(1) + 1;
            totalConnections_.fetch_add(1);
            if (isProxy) {
                proxyActiveConnections_.fetch_add(1, std::memory_order_relaxed);
            } else {
                mainActiveConnections_.fetch_add(1, std::memory_order_relaxed);
            }

            spdlog::debug("SocketServer: [conn={}] accepted {} connection, active={} total={}",
                          conn_token, loopLabel, current, totalConnections_.load());

            publishConnectionStats(current);
            TuningManager::notifyWakeup();

            auto connectionExecutor = boost::asio::make_strand(ioCoordinator_->getExecutor());
            auto sock = std::make_shared<local::socket>(std::move(socket));
            auto tracked = std::make_shared<TrackedSocket>();
            tracked->socket = sock;
            tracked->executor = connectionExecutor;
            tracked->set_created_at(
                std::chrono::steady_clock::now()); // Track connection creation time
            register_socket(tracked);

            if (trace) {
                spdlog::info("stream-trace: [conn={}] handler_spawned", conn_token);
            }

            // Create a capturing lambda that releases the semaphore on completion
            auto wrapped_handler = [this, conn_token, tracked, isProxy,
                                    slots]() mutable -> awaitable<void> {
                // RAII guard for semaphore - handles shrink debt for graceful downsizing
                struct SemaphoreGuard {
                    SocketServer* server;
                    std::counting_semaphore<>* sem;
                    bool proxyConn;
                    ~SemaphoreGuard() {
                        if (!sem || !server)
                            return;
                        if (proxyConn) {
                            sem->release();
                            return;
                        }
                    }
                } guard{this, slots, isProxy};

                // Handle the connection with tracing token
                co_await handle_connection(tracked, conn_token, isProxy);
            };

            // Spawn detached; shutdown closes sockets/acceptors and waits for accept loops before
            // executor teardown.
            co_spawn(connectionExecutor, wrapped_handler(), boost::asio::detached);

        } catch (const std::exception& e) {
            if (!running_ || stopping_)
                break;

            spdlog::error("Unexpected error in accept loop: {}", e.what());
            break;
        }
    }

    spdlog::debug("Accept loop ended");
}

awaitable<void> SocketServer::handle_connection(std::shared_ptr<TrackedSocket> tracked_socket,
                                                uint64_t conn_token, bool isProxy) {
    YAMS_ZONE_SCOPED_N("SocketServer::handle_connection");
    static const bool trace = stream_trace_enabled();
    const auto handler_start_time = std::chrono::steady_clock::now();

    if (!tracked_socket || !tracked_socket->socket) {
        co_return;
    }

    auto sock = tracked_socket->socket;

    // Track IPC task lifecycle: pending (spawned) -> active (executing) -> done
    struct IpcTaskTracker {
        StateComponent* state;
        bool active{false};
        IpcTaskTracker(StateComponent* s) : state(s) {
            if (state) {
                state->stats.ipcTasksPending.fetch_add(1, std::memory_order_relaxed);
            }
        }
        void markActive() {
            if (state && !active) {
                state->stats.ipcTasksPending.fetch_sub(1, std::memory_order_relaxed);
                state->stats.ipcTasksActive.fetch_add(1, std::memory_order_relaxed);
                active = true;
            }
        }
        ~IpcTaskTracker() {
            if (state) {
                if (active) {
                    state->stats.ipcTasksActive.fetch_sub(1, std::memory_order_relaxed);
                } else {
                    state->stats.ipcTasksPending.fetch_sub(1, std::memory_order_relaxed);
                }
            }
        }
    } ipcTracker{state_};

    struct CleanupGuard {
        SocketServer* server;
        bool trace;
        uint64_t conn_token;
        std::chrono::steady_clock::time_point start_time;
        bool proxyConn;
        ~CleanupGuard() {
            auto current = server->activeConnections_.fetch_sub(1) - 1;
            if (proxyConn) {
                server->proxyActiveConnections_.fetch_sub(1, std::memory_order_relaxed);
            } else {
                server->mainActiveConnections_.fetch_sub(1, std::memory_order_relaxed);
            }
            if (current == 0) {
                std::lock_guard<std::mutex> lock(server->shutdownMutex_);
                server->shutdownCv_.notify_all();
            }
            server->publishConnectionStats(current);
            auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - start_time)
                                   .count();
            if (trace) {
                spdlog::info("stream-trace: [conn={}] handle_connection cleanup active={} total={} "
                             "duration_ms={}",
                             conn_token, current, server->totalConnections_.load(), duration_ms);
            } else {
                spdlog::debug("[conn={}] Connection closed, active: {}, duration_ms: {}",
                              conn_token, current, duration_ms);
            }
        }
    } guard{this, trace, conn_token, handler_start_time, isProxy};

    if (trace) {
        spdlog::info("stream-trace: [conn={}] handler_ready active={} total={} socket_valid={}",
                     conn_token, activeConnections_.load(std::memory_order_relaxed),
                     totalConnections_.load(std::memory_order_relaxed), sock && sock->is_open());
    }

    try {
        if (!writerBudget_) {
            std::size_t initialBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
            if (initialBudget == 0)
                initialBudget = TuneAdvisor::writerBudgetBytesPerTurn();
            if (initialBudget == 0)
                initialBudget = 256ULL * 1024;
            writerBudget_ = std::make_shared<std::atomic<std::size_t>>(initialBudget);
        }

        RequestHandler::Config handlerConfig;
        handlerConfig.writer_budget_ref = writerBudget_;
        handlerConfig.writer_budget_bytes_per_turn = writerBudget_->load(std::memory_order_relaxed);
        handlerConfig.enable_streaming = true;
        handlerConfig.enable_multiplexing = true;
        // Request coroutines run on the dedicated RequestExecutor (cli_executor).
        if (dispatcher_) {
            try {
                handlerConfig.worker_executor = dispatcher_->getWorkerExecutor();
                handlerConfig.cli_executor = dispatcher_->getCliExecutor();
                handlerConfig.worker_job_signal = dispatcher_->getWorkerJobSignal();
            } catch (...) {
            }
        }
        handlerConfig.chunk_size = TuneAdvisor::chunkSize();
        handlerConfig.close_after_response = false;
        handlerConfig.graceful_half_close = true;
        auto connectionTimeout = config_.connectionTimeout;
        if (connectionTimeout.count() == 0) {
            connectionTimeout = std::chrono::milliseconds(TuneAdvisor::ipcTimeoutMs());
        }
        auto timeoutSeconds = std::chrono::duration_cast<std::chrono::seconds>(connectionTimeout);
        if (timeoutSeconds.count() == 0 && connectionTimeout.count() > 0) {
            timeoutSeconds = std::chrono::seconds(1);
        }
        handlerConfig.read_timeout = timeoutSeconds;
        handlerConfig.write_timeout = timeoutSeconds;
        auto streamChunkTimeoutMs = TuneAdvisor::streamChunkTimeoutMs();
        if (streamChunkTimeoutMs == 0 && connectionTimeout.count() > 0) {
            streamChunkTimeoutMs = static_cast<uint32_t>(connectionTimeout.count());
        }
        handlerConfig.stream_chunk_timeout = std::chrono::milliseconds(streamChunkTimeoutMs);
        handlerConfig.max_inflight_per_connection = TuneAdvisor::serverMaxInflightPerConn();
        handlerConfig.admission_control = [this, isProxy](const Request& request)
            -> std::optional<RequestHandler::Config::AdmissionDecision> {
            const auto verdict = evaluateRequestAdmission(request, isProxy);
            if (verdict != SocketAdmissionVerdict::reject) {
                return std::nullopt;
            }

            const size_t active = isProxy ? proxyActiveConnections_.load(std::memory_order_relaxed)
                                          : mainActiveConnectionCount();
            const size_t limit = slotLimit_.load(std::memory_order_relaxed);

            if (state_) {
                state_->stats.acceptCapacityDelays.fetch_add(1, std::memory_order_relaxed);
                state_->stats.connectionSlotsFree.store(0, std::memory_order_relaxed);
            }
            TuningManager::notifyWakeup();
            return makeSocketBusyDecision(dispatcher_, state_, active, limit);
        };
        // Wire health-check counter so RequestHandler can tag ping/status connections
        if (state_) {
            handlerConfig.health_check_counter = &state_->stats.healthCheckConnections;
        }
        MuxMetricsRegistry::instance().setWriterBudget(handlerConfig.writer_budget_bytes_per_turn);
        RequestDispatcher* disp = nullptr;
        {
            std::lock_guard<std::mutex> lk(dispatcherMutex_);
            disp = dispatcher_;
        }
        // Wire worker_job_signal for cross-executor coordination.
        // worker_executor is already set above; this path only adds the signal.
        try {
            if (disp) {
                handlerConfig.worker_job_signal = disp->getWorkerJobSignal();
            }
        } catch (...) {
        }
        auto handler = std::make_shared<RequestHandler>(disp, handlerConfig);

        // Wrap socket in shared_ptr for tracking and use modern C++20 move semantics
        // Use the server's stop_source token so we can cancel connections during shutdown
        auto token = stop_source_.get_token();

        // Mark IPC task as active (transitioned from pending to executing)
        ipcTracker.markActive();

        // Enforce maximum connection lifetime to prevent zombie connections
        // Proxy connections are persistent — skip lifetime enforcement
        if (!isProxy && config_.maxConnectionLifetime.count() > 0) {
            // Use async_initiate to race connection handler against lifetime timer
            // (replaces experimental::awaitable_operators)
            auto executor = sock->get_executor();
            auto connection_strand = boost::asio::make_strand(executor);
            auto lifetime = config_.maxConnectionLifetime;
            auto created_at = tracked_socket->created_at(); // Thread-safe read

            bool timedOut =
                co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                     void(std::exception_ptr, bool)>(
                    [this, handler, sock, token, conn_token, executor, connection_strand, lifetime,
                     created_at](auto completion_handler) mutable {
                        // Shared state for race coordination
                        auto completed = std::make_shared<std::atomic<bool>>(false);
                        auto timer = std::make_shared<boost::asio::steady_timer>(connection_strand);
                        timer->expires_after(lifetime);

                        using HandlerT = std::decay_t<decltype(completion_handler)>;
                        auto handlerPtr = std::make_shared<HandlerT>(std::move(completion_handler));
                        auto completion_exec =
                            boost::asio::get_associated_executor(*handlerPtr, executor);

                        // Set up lifetime timer
                        timer->async_wait([this, completed, handlerPtr, completion_exec, conn_token,
                                           created_at, lifetime,
                                           sock](const boost::system::error_code& ec) mutable {
                            if (ec == boost::asio::error::operation_aborted)
                                return; // Cancelled by handler completion
                            if (!completed->exchange(true, std::memory_order_acq_rel)) {
                                // Timer won - lifetime exceeded
                                auto newCount =
                                    forcedCloseCount_.fetch_add(1, std::memory_order_relaxed) + 1;
                                if (state_) {
                                    state_->stats.forcedCloseCount.store(newCount,
                                                                         std::memory_order_relaxed);
                                }
                                auto age_s = std::chrono::duration_cast<std::chrono::seconds>(
                                                 std::chrono::steady_clock::now() - created_at)
                                                 .count();
                                spdlog::warn("[conn={}] Connection lifetime exceeded (age={}s, "
                                             "limit={}s) - forcing close",
                                             conn_token, age_s, lifetime.count());
                                // Cancel pending async ops so the detached coroutine's
                                // async_read completes with operation_aborted.  Do NOT
                                // close() here — closing from the timer callback while
                                // the coroutine may be mid-resume on another IO thread
                                // causes a double kqueue_reactor::deregister_descriptor
                                // → SIGSEGV.
                                //
                                // cancel() is safe to call from any completion handler
                                // on the same io_context (it uses internal locking on
                                // the descriptor state).  The coroutine will observe
                                // the read error (operation_aborted), exit its loop,
                                // and close the socket itself.
                                //
                                // shutdown(both) ensures that even if the inner idle
                                // timer wins the race (swallowing the cancel), the
                                // *next* async_read returns EOF so the coroutine exits
                                // promptly rather than lingering for idle-timeout cycles.
                                if (sock && sock->is_open()) {
                                    boost::system::error_code shutdown_ec;
                                    sock->shutdown(boost::asio::socket_base::shutdown_both,
                                                   shutdown_ec);
                                    boost::system::error_code cancel_ec;
                                    sock->cancel(cancel_ec);
                                }
                                boost::asio::post(
                                    completion_exec, [h = std::move(*handlerPtr)]() mutable {
                                        std::move(h)(std::exception_ptr{}, true); // timedOut = true
                                    });
                            }
                        });

                        // Spawn connection handler
                        boost::asio::co_spawn(
                            connection_strand,
                            [handler, sock, token, conn_token, timer, completed, handlerPtr,
                             completion_exec]() mutable -> boost::asio::awaitable<void> {
                                co_await handler->handle_connection(sock, token, conn_token);

                                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                                    // Handler completed first
                                    timer->cancel();
                                    boost::asio::post(completion_exec,
                                                      [h = std::move(*handlerPtr)]() mutable {
                                                          std::move(h)(std::exception_ptr{},
                                                                       false); // timedOut = false
                                                      });
                                }
                            },
                            boost::asio::detached);
                    },
                    boost::asio::use_awaitable);

            if (timedOut) {
                // The timer callback cancelled pending I/O via cancel().
                // The detached coroutine owns socket close: it will see the
                // operation_aborted error and close the socket in its error
                // path.  Do NOT close here — the coroutine may still be
                // mid-resume on another IO thread, and concurrent close()
                // on a Boost.Asio socket triggers a double
                // kqueue_reactor::deregister_descriptor → SIGSEGV.
                //
                // If the coroutine exits without closing (e.g. exception),
                // the shared_ptr<socket> destructor handles final cleanup.
                spdlog::debug("[conn={}] Lifetime expired; socket cleanup "
                              "delegated to coroutine",
                              conn_token);
            }
        } else {
            co_await handler->handle_connection(sock, token, conn_token);
        }
    } catch (const std::exception& e) {
        spdlog::error("SocketServer::handle_connection error: {}", e.what());
    }
}

void SocketServer::register_socket(std::shared_ptr<TrackedSocket> tracked_socket) {
    if (!tracked_socket) {
        return;
    }

    std::lock_guard<std::mutex> lk(activeSocketsMutex_);
    // Clean up expired weak_ptrs while we're here
    activeSockets_.erase(std::remove_if(activeSockets_.begin(), activeSockets_.end(),
                                        [](const auto& weak) { return weak.expired(); }),
                         activeSockets_.end());
    activeSockets_.push_back(tracked_socket);
}

void SocketServer::execute_on_io_context(std::function<void()> fn) {
    if (!fn) {
        return;
    }

    if (!ioCoordinator_ || !ioCoordinator_->isRunning()) {
        fn();
        return;
    }

    auto io_context = ioCoordinator_->getIOContext();
    auto executor = ioCoordinator_->getExecutor();
    if (io_context->stopped() || executor.running_in_this_thread()) {
        fn();
        return;
    }

    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    std::exception_ptr error;

    boost::asio::post(executor, [fn = std::move(fn), &mutex, &cv, &done, &error]() mutable {
        try {
            fn();
        } catch (...) {
            error = std::current_exception();
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            done = true;
        }
        cv.notify_one();
    });

    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&done] { return done; });
    if (error) {
        std::rethrow_exception(error);
    }
}

void SocketServer::close_acceptor_on_executor() {
    if (!acceptor_) {
        return;
    }

    try {
        execute_on_io_context([this]() {
            if (!acceptor_) {
                return;
            }
            boost::system::error_code ec;
            acceptor_->cancel(ec);
            if (acceptor_->is_open()) {
                acceptor_->close(ec);
            }
        });
    } catch (const std::exception& e) {
        spdlog::warn("SocketServer: failed to close acceptor on executor: {}", e.what());
    }
}

std::size_t SocketServer::close_sockets_on_executor(
    const std::vector<std::shared_ptr<TrackedSocket>>& tracked_sockets) {
    if (tracked_sockets.empty()) {
        return 0;
    }

    std::size_t scheduled = 0;

    for (auto& tracked : tracked_sockets) {
        if (!tracked || !tracked->socket) {
            continue;
        }

        auto sock = tracked->socket;

        ++scheduled;

        auto exec = tracked->executor;
        if (!exec) {
            exec = sock->get_executor();
        }
        try {
            boost::asio::dispatch(exec, [sock]() mutable {
                boost::system::error_code ec;
                if (sock->is_open()) {
                    boost::system::error_code shutdown_ec;
                    sock->shutdown(boost::asio::socket_base::shutdown_both, shutdown_ec);
                    sock->cancel(ec);
                }
            });
        } catch (const std::exception& e) {
            spdlog::warn("SocketServer: failed to dispatch socket close: {}", e.what());
        } catch (...) {
            spdlog::warn("SocketServer: failed to dispatch socket close (unknown error)");
        }
    }

    return scheduled;
}

uint64_t SocketServer::oldestConnectionAgeSeconds() const {
    std::lock_guard<std::mutex> lk(activeSocketsMutex_);
    if (activeSockets_.empty()) {
        return 0;
    }

    auto now = std::chrono::steady_clock::now();
    uint64_t oldestAge = 0;

    for (const auto& weakPtr : activeSockets_) {
        if (auto tracked = weakPtr.lock()) {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(now - tracked->created_at())
                           .count();
            if (static_cast<uint64_t>(age) > oldestAge) {
                oldestAge = static_cast<uint64_t>(age);
            }
        }
    }

    return oldestAge;
}

// -------- Dynamic connection slot sizing (PBI-085) --------

bool SocketServer::resizeConnectionSlots(size_t newSize) {
    if (newSize == 0) {
        return false;
    }

    size_t currentLimit = slotLimit_.load(std::memory_order_relaxed);
    if (newSize == currentLimit) {
        return true; // No change needed
    }

    // With protocol-level request admission, slot resizing is now a soft budget change.
    // Connections remain established; the new limit affects future request admission.
    if (newSize > currentLimit) {
        slotLimit_.store(newSize, std::memory_order_relaxed);

        // Update state stats
        if (state_) {
            state_->stats.maxConnections.store(newSize, std::memory_order_relaxed);
            publishConnectionStats(activeConnections_.load(std::memory_order_relaxed), newSize);
        }

        spdlog::debug("SocketServer: request admission slots grown from {} to {}", currentLimit,
                      newSize);
        return true;
    }

    slotLimit_.store(newSize, std::memory_order_relaxed);

    // Update state stats
    if (state_) {
        state_->stats.maxConnections.store(newSize, std::memory_order_relaxed);
        publishConnectionStats(activeConnections_.load(std::memory_order_relaxed), newSize);
    }

    spdlog::debug("SocketServer: request admission slots shrunk from {} to {}", currentLimit,
                  newSize);

    return true;
}

double SocketServer::getSlotUtilization() const {
    size_t active = mainActiveConnectionCount();
    size_t limit = slotLimit_.load(std::memory_order_relaxed);
    if (limit == 0) {
        return 0.0;
    }
    return static_cast<double>(active) / static_cast<double>(limit);
}

} // namespace yams::daemon
