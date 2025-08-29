#include <yams/daemon/ipc/async_socket.h>

#include <spdlog/spdlog.h>
#include <atomic>
#include <coroutine>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <map>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#if (defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)) && \
    !defined(HAVE_KQUEUE)
#define HAVE_KQUEUE 1
#endif
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0
#endif
#ifndef _WIN32
#include <sys/select.h>
#endif

#ifdef HAVE_IO_URING
#include <liburing.h>
#endif

#ifdef HAVE_KQUEUE
#include <sys/event.h>
#include <sys/time.h>
#endif

namespace yams::daemon {

// ============================================================================
// AsyncIOContext Implementation
// ============================================================================

#if defined(HAVE_KQUEUE)
// macOS/BSD implementation using kqueue
class AsyncIOContext::Impl {
public:
    Impl() : stop_requested_(false) {
        kq_ = kqueue();
        if (kq_ < 0) {
            throw std::runtime_error("Failed to create kqueue");
        }

        if (pipe(wake_pipe_) != 0) {
            ::close(kq_);
            throw std::runtime_error("Failed to create wake pipe");
        }

        int flags = fcntl(wake_pipe_[0], F_GETFL, 0);
        fcntl(wake_pipe_[0], F_SETFL, flags | O_NONBLOCK);

        struct kevent ev;
        EV_SET(&ev, wake_pipe_[0], EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
    }

    ~Impl() {
        if (wake_pipe_[0] >= 0)
            ::close(wake_pipe_[0]);
        if (wake_pipe_[1] >= 0)
            ::close(wake_pipe_[1]);
        if (kq_ >= 0)
            ::close(kq_);
    }

    Result<void> register_socket(int fd, uint64_t generation) {
        std::lock_guard lock(mutex_);
        socket_generations_[fd] = generation;
        return Result<void>();
    }

    void cancel_pending_operations(int fd) {
        bool enqueued = false;
        {
            std::lock_guard lock(mutex_);
            auto it = pending_ops_.find(fd);
            if (it != pending_ops_.end()) {
                for (auto& op : it->second) {
                    cancel_queue_.push_back(std::move(op));
                }
                pending_ops_.erase(it);
                enqueued = true;
            }
            socket_generations_.erase(fd);
        }

        if (enqueued && wake_pipe_[1] >= 0) {
            char byte = 1;
            [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
        }
    }

    Result<void> unregister_socket(int fd) {
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        int result = kevent(kq_, &ev, 1, nullptr, 0, nullptr);
        if (result == -1 && errno != ENOENT) {
            spdlog::warn("AsyncIOContext(kqueue): EV_DELETE EVFILT_READ failed for fd {}: {}", fd,
                         strerror(errno));
        }

        EV_SET(&ev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        result = kevent(kq_, &ev, 1, nullptr, 0, nullptr);
        if (result == -1 && errno != ENOENT) {
            spdlog::warn("AsyncIOContext(kqueue): EV_DELETE EVFILT_WRITE failed for fd {}: {}", fd,
                         strerror(errno));
        }

        bool enqueued = false;
        {
            std::lock_guard lock(mutex_);
            auto it = pending_ops_.find(fd);
            if (it != pending_ops_.end()) {
                for (auto& op : it->second) {
                    cancel_queue_.push_back(std::move(op));
                }
                pending_ops_.erase(it);
                enqueued = true;
            }
            socket_generations_.erase(fd);
        }

        if (enqueued && wake_pipe_[1] >= 0) {
            char byte = 1;
            [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
        }

        return Result<void>();
    }

    // Type-erased submit functions to support templated AsyncSocket awaiters
    void submit_read_erased(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                            std::shared_ptr<yams::daemon::SocketAwaiterBase> awaiter,
                            uint64_t generation) {
        if (!awaiter || !h)
            return;

        // Early drain path
        if (stop_requested_.load(std::memory_order_acquire) ||
            draining_.load(std::memory_order_acquire)) {
            enqueue_cancel_(fd, size, OpType::Read, generation, awaiter, h);
            return;
        }

        // Validate generation or cancel via IO thread
        if (!validate_or_cancel_(fd, generation, size, OpType::Read, awaiter, h))
            return;

        PendingOp op;
        op.handle = h;
        op.awaiter_shared = awaiter;
        op.fd = fd;
        op.read_buffer.resize(size);
        op.external_buffer = buffer;
        op.size = size;
        op.type = OpType::Read;
        op.generation = generation;

        spdlog::debug("AsyncIOContext(kqueue): submitting read op handle={:p} awaiter={:p} (fd={})",
                      static_cast<void*>(h.address()), static_cast<void*>(awaiter.get()), fd);

        {
            std::lock_guard lock(mutex_);
            pending_ops_[fd].push_back(std::move(op));
        }

        // Register event or revert and cancel
        if (!register_event_or_cancel_(fd, EVFILT_READ, awaiter, h, size, OpType::Read,
                                       generation)) {
            return;
        }
    }

    void submit_write_erased(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                             std::shared_ptr<yams::daemon::SocketAwaiterBase> awaiter,
                             uint64_t generation) {
        if (!awaiter || !h)
            return;

        // Early drain path
        if (stop_requested_.load(std::memory_order_acquire) ||
            draining_.load(std::memory_order_acquire)) {
            enqueue_cancel_(fd, size, OpType::Write, generation, awaiter, h);
            return;
        }

        // Validate generation or cancel via IO thread
        if (!validate_or_cancel_(fd, generation, size, OpType::Write, awaiter, h))
            return;

        PendingOp op;
        op.handle = h;
        op.awaiter_shared = awaiter;
        op.fd = fd;
        op.data.assign(static_cast<const uint8_t*>(buffer),
                       static_cast<const uint8_t*>(buffer) + size);
        op.size = size;
        op.type = OpType::Write;
        op.generation = generation;

        spdlog::debug(
            "AsyncIOContext(kqueue): submitting write op handle={:p} awaiter={:p} (fd={})",
            static_cast<void*>(h.address()), static_cast<void*>(awaiter.get()), fd);

        {
            std::lock_guard lock(mutex_);
            pending_ops_[fd].push_back(std::move(op));
        }

        // Register event or revert and cancel
        if (!register_event_or_cancel_(fd, EVFILT_WRITE, awaiter, h, size, OpType::Write,
                                       generation)) {
            return;
        }
    }

    void run() {
        struct kevent events[MAX_EVENTS];
        struct timespec timeout = {0, 100000000}; // 100ms

        for (;;) {
            // First, drain any queued cancellations to ensure we don't resume from foreign threads
            {
                std::deque<PendingOp> to_cancel;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!cancel_queue_.empty()) {
                        to_cancel.swap(cancel_queue_);
                    }
                }
                for (auto& op : to_cancel) {
                    cancel_operation(op);
                }
            }

            // If a stop is requested, enter draining phase: cancel all remaining ops and exit
            if (stop_requested_.load(std::memory_order_acquire)) {
                std::deque<PendingOp> to_cancel;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!pending_ops_.empty()) {
                        for (auto& [fd, dq] : pending_ops_) {
                            for (auto& op : dq) {
                                to_cancel.push_back(std::move(op));
                            }
                        }
                        pending_ops_.clear();
                    }
                    // Clear generations to avoid further matches
                    socket_generations_.clear();
                }
                for (auto& op : to_cancel) {
                    cancel_operation(op);
                }
                // After draining pending ops and cancel queue, exit the loop
                bool more_pending = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    more_pending = !cancel_queue_.empty() || !pending_ops_.empty();
                }
                if (!more_pending) {
                    break;
                } else {
                    // Give a tiny breather before next drain iteration
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
            }

            int nev = kevent(kq_, nullptr, 0, events, MAX_EVENTS, &timeout);
            if (stop_requested_.load(std::memory_order_acquire))
                continue; // loop will drain and exit above

            if (nev < 0) {
                if (errno == EINTR) {
                    continue;
                }
                spdlog::warn("AsyncIOContext(kqueue): kevent() error: {}", strerror(errno));
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            for (int i = 0; i < nev; ++i) {
                int fd = static_cast<int>(events[i].ident);

                spdlog::debug(
                    "AsyncIOContext(kqueue): processing event fd={}, filter={}, flags=0x{:x}", fd,
                    events[i].filter, events[i].flags);

                // Wake pipe has priority; handle it regardless of registration
                if (fd == wake_pipe_[0]) {
                    char buffer[8];
                    while (read(wake_pipe_[0], buffer, sizeof(buffer)) > 0) {
                    }
                    // After wake, also drain any new cancellations
                    std::deque<PendingOp> to_cancel;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        if (!cancel_queue_.empty()) {
                            to_cancel.swap(cancel_queue_);
                        }
                    }
                    for (auto& op : to_cancel) {
                        cancel_operation(op);
                    }
                    continue;
                }

                // Validate fd is registered before processing
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (socket_generations_.find(fd) == socket_generations_.end()) {
                        spdlog::debug(
                            "AsyncIOContext(kqueue): ignoring event for unregistered fd={}", fd);
                        continue;
                    }
                }

                // Defensive error/EOF handling with atomic cancel and unregister
                if ((events[i].flags & EV_ERROR) || (events[i].flags & EV_EOF)) {
                    if (events[i].flags & EV_ERROR) {
                        int err = static_cast<int>(events[i].data);
                        spdlog::debug("AsyncIOContext(kqueue): EV_ERROR on fd {}: {}", fd,
                                      strerror(err));
                    } else {
                        spdlog::debug("AsyncIOContext(kqueue): EV_EOF (peer closed) on fd {}", fd);
                    }

                    // Atomic cancel sequence: collect and cancel operations without redundant
                    // unregister
                    std::deque<PendingOp> ops_to_cancel;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        auto it = pending_ops_.find(fd);
                        if (it != pending_ops_.end()) {
                            ops_to_cancel = std::move(it->second);
                            pending_ops_.erase(it);
                        }
                        socket_generations_.erase(fd);
                    }

                    // Resume coroutines with error state outside of lock
                    for (auto& op : ops_to_cancel) {
                        cancel_operation(op);
                    }

                    // Issue EV_DELETE for both filters to clean up kqueue state
                    struct kevent ev;
                    EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
                    kevent(kq_, &ev, 1, nullptr, 0, nullptr);
                    EV_SET(&ev, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
                    kevent(kq_, &ev, 1, nullptr, 0, nullptr);

                    continue;
                }

                // Extract one pending op for this fd/filter
                PendingOp op{};
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto it = pending_ops_.find(fd);
                    if (it != pending_ops_.end() && !it->second.empty()) {
                        OpType want =
                            (events[i].filter == EVFILT_READ) ? OpType::Read : OpType::Write;
                        auto& dq = it->second;
                        auto op_it = std::find_if(dq.begin(), dq.end(), [&](const PendingOp& p) {
                            return p.type == want;
                        });
                        if (op_it != dq.end()) {
                            op = std::move(*op_it);
                            dq.erase(op_it);
                            spdlog::debug("AsyncIOContext(kqueue): extracted op for fd={}, "
                                          "filter={}, pending_ops_remaining={}",
                                          fd, events[i].filter, dq.size());
                            if (!op.handle && !op.awaiter_shared) {
                                spdlog::debug(
                                    "AsyncIOContext(kqueue): extracted empty op for fd={}", fd);
                                continue;
                            }
                            if (op.type == OpType::Write && op.data.empty()) {
                                spdlog::debug("AsyncIOContext(kqueue): extracted write op with "
                                              "empty data for fd={}",
                                              fd);
                                continue;
                            }
                            if (op.type == OpType::Read && op.read_buffer.empty() && op.size > 0) {
                                spdlog::debug("AsyncIOContext(kqueue): extracted read op with "
                                              "empty buffer for fd={}",
                                              fd);
                                continue;
                            }
                        } else {
                            spdlog::debug(
                                "AsyncIOContext(kqueue): no pending op for fd={}, filter={}", fd,
                                events[i].filter);
                        }
                    } else {
                        spdlog::debug("AsyncIOContext(kqueue): no pending ops for fd={}", fd);
                    }
                }

                // Safety check: ensure we have valid awaiter and handle
                if (!op.awaiter_shared || !op.handle) {
                    spdlog::debug("AsyncIOContext(kqueue): op missing awaiter or handle (fd={})",
                                  fd);
                    continue;
                }

                // Validate that the event filter matches the operation type
                if ((events[i].filter == EVFILT_READ && op.type != OpType::Read) ||
                    (events[i].filter == EVFILT_WRITE && op.type != OpType::Write)) {
                    spdlog::debug("AsyncIOContext(kqueue): filter/op type mismatch (fd={}, "
                                  "filter={}, op_type={})",
                                  fd, events[i].filter, static_cast<int>(op.type));
                    continue;
                }

                // Take ownership of the operation data before any processing
                auto handle = op.handle;
                auto awaiter_shared = op.awaiter_shared;
                uint64_t op_generation = op.generation;

                // Extra liveness guard to avoid use-after-free if coroutine was already resumed
                // elsewhere
                if (!awaiter_shared || !awaiter_shared->alive.load(std::memory_order_acquire)) {
                    spdlog::debug(
                        "AsyncIOContext(kqueue): awaiter already not alive (fd={}, gen={})", fd,
                        op_generation);
                    continue;
                }

                // Safety check: if operation is already scheduled, skip it
                bool pre_scheduled = awaiter_shared->scheduled.load(std::memory_order_acquire);
                if (pre_scheduled) {
                    spdlog::debug("AsyncIOContext(kqueue): skipping already scheduled op for fd={}",
                                  fd);
                    continue;
                }

                spdlog::debug("AsyncIOContext(kqueue): processing op (fd={}, type={})", fd,
                              static_cast<int>(op.type));

                // Final safety check: ensure socket is still valid
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (socket_generations_.find(fd) == socket_generations_.end()) {
                        spdlog::debug("AsyncIOContext(kqueue): socket no longer valid for fd={}",
                                      fd);
                        continue;
                    }
                }

                // Defensive guard: if write op has empty data or zero size, cancel safely
                if (op.type == OpType::Write && (op.data.empty() || op.size == 0)) {
                    spdlog::debug("AsyncIOContext(kqueue): write op has no data (fd={}, size={})",
                                  fd, op.size);
                    awaiter_shared->result.store(-1, std::memory_order_release);
                    awaiter_shared->error_code.store(EPIPE, std::memory_order_release);
                    awaiter_shared->cancelled.store(true, std::memory_order_release);
                    bool resume_scheduled =
                        awaiter_shared->scheduled.exchange(true, std::memory_order_acq_rel);
                    if (!resume_scheduled && !handle.done()) {
                        op.awaiter_shared.reset();
                        op.handle = nullptr;
                        handle.resume();
                    }
                    continue;
                }

                // Validate generation and check if awaiter is still alive
                bool is_valid = false;
                uint64_t current_generation = 0;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto gen_it = socket_generations_.find(fd);
                    is_valid =
                        (gen_it != socket_generations_.end() && gen_it->second == op.generation);
                    if (gen_it != socket_generations_.end()) {
                        current_generation = gen_it->second;
                    }
                }

                // Additional safety check: if the socket is no longer registered, skip processing
                if (current_generation == 0) {
                    spdlog::debug("AsyncIOContext(kqueue): skipping op for unregistered fd={}", fd);
                    continue;
                }

                // Log generation mismatch
                if (!is_valid) {
                    spdlog::debug("AsyncIOContext(kqueue): generation mismatch (fd={}, op_gen={}, "
                                  "current_gen={})",
                                  fd, op.generation, current_generation);
                }

                // Check if awaiter is still alive
                bool is_alive = op.awaiter_shared->alive.load(std::memory_order_acquire);
                if (!is_alive) {
                    spdlog::debug("AsyncIOContext(kqueue): awaiter not alive (fd={}, gen={})", fd,
                                  op.generation);
                    continue;
                }

                // Check if operation is stale/cancelled before doing I/O
                if (!is_valid) {
                    spdlog::debug("AsyncIOContext(kqueue): stale op (fd={}, gen={})", fd,
                                  op_generation);
                    cancel_operation(op);
                    continue;
                }

                if (handle.done() || awaiter_shared->cancelled.load()) {
                    spdlog::debug("AsyncIOContext(kqueue): cancelled op (fd={}, gen={})", fd,
                                  op_generation);
                    continue;
                }

                // Perform non-blocking I/O
                ssize_t io_result = -1;
                int io_errno = 0;
                if (op.type == OpType::Read) {
                    io_result = recv(fd, op.read_buffer.data(), op.size, MSG_DONTWAIT);
                    io_errno = errno;
                } else {
                    size_t to_write = std::min(op.size, op.data.size());
                    if (to_write == 0) {
                        io_result = -1;
                        io_errno = EPIPE;
                    } else {
                        io_result = send(fd, op.data.data(), to_write, MSG_DONTWAIT | MSG_NOSIGNAL);
                        io_errno = errno;
                    }
                }

                // Final validation before updating awaiter
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto gen_it = socket_generations_.find(fd);
                    is_valid =
                        (gen_it != socket_generations_.end() && gen_it->second == op.generation);
                }

                // Check if awaiter is still alive after I/O
                is_alive = op.awaiter_shared->alive.load(std::memory_order_acquire);
                if (!is_alive) {
                    spdlog::debug(
                        "AsyncIOContext(kqueue): awaiter not alive after I/O (fd={}, gen={})", fd,
                        op.generation);
                    continue;
                }

                if (!is_valid) {
                    spdlog::debug("AsyncIOContext(kqueue): stale op after I/O (fd={}, gen={})", fd,
                                  op_generation);
                    cancel_operation(op);
                    continue;
                }

                if (io_result < 0) {
                    // If not actually ready, requeue and re-register event (EV_ONESHOT)
                    if (io_errno == EAGAIN || io_errno == EWOULDBLOCK) {
                        spdlog::debug("AsyncIOContext(kqueue): EAGAIN/EWOULDBLOCK, requeue op "
                                      "(fd={}, type={})",
                                      fd, static_cast<int>(op.type));
                        {
                            std::lock_guard<std::mutex> lock(mutex_);
                            // Put it back at the front to retry soon
                            pending_ops_[fd].push_front(op);
                        }
                        // Re-register the event since EV_ONESHOT was used
                        int16_t filter = (op.type == OpType::Read) ? EVFILT_READ : EVFILT_WRITE;
                        // Use the copies captured earlier
                        if (!register_event_or_cancel_(fd, filter, awaiter_shared, handle, op.size,
                                                       op.type, op.generation)) {
                            // Registration failed; cancellation path enqueued
                        }
                        continue;
                    }
                    if (io_errno == 0) {
#ifdef EPIPE
                        io_errno = EPIPE;
#else
                        io_errno = ECONNRESET;
#endif
                    }
                    awaiter_shared->error_code.store(io_errno, std::memory_order_release);
                } else if (op.type == OpType::Read && io_result > 0) {
                    // For successful read operations, copy data to caller's buffer
                    void* local_external_buffer = op.external_buffer;
                    size_t local_buffer_size = op.size;
                    auto& read_buffer = op.read_buffer;

                    if (local_external_buffer &&
                        static_cast<size_t>(io_result) <= local_buffer_size) {
                        std::memcpy(local_external_buffer, read_buffer.data(), io_result);
                    }
                }
                awaiter_shared->result.store(io_result, std::memory_order_release);

                // Resume safely (final check) with single-resume guarantee
                size_t refs = awaiter_shared.use_count();
                if (refs <= 1) {
                    spdlog::debug(
                        "AsyncIOContext(kqueue): awaiter use_count={} before resume (fd={})", refs,
                        fd);
                }
                auto keep_alive = awaiter_shared;
                bool resume_scheduled =
                    awaiter_shared->scheduled.exchange(true, std::memory_order_acq_rel);
                if (!resume_scheduled && !awaiter_shared->cancelled.load()) {
                    awaiter_shared->alive.store(false, std::memory_order_release);
                    auto local_handle = handle;
                    // Defensive: avoid resuming a completed/destroyed coroutine frame
                    if (local_handle && !local_handle.done()) {
                        try {
                            local_handle.resume();
                        } catch (...) {
                            spdlog::debug("AsyncIOContext(kqueue): exception during resume (fd={})",
                                          fd);
                        }
                    } else {
                        spdlog::debug("AsyncIOContext(kqueue): skip resume because handle is "
                                      "null/done (fd={})",
                                      fd);
                    }
                } else {
                    spdlog::debug(
                        "AsyncIOContext(kqueue): skip resume (done/cancelled/scheduled) for fd {}",
                        fd);
                }
            } // for events
        } // while
    }

    void stop() {
        // Enter draining mode: we will cancel outstanding ops without resuming coroutine frames.
        draining_.store(true, std::memory_order_release);
        stop_requested_.store(true, std::memory_order_release);
        if (wake_pipe_[1] >= 0) {
            char byte = 1;
            write(wake_pipe_[1], &byte, 1);
        }
    }

private:
    enum class OpType { Read, Write };

    // Base awaiter type for type erasure in pending operations
    // Use the shared non-templated base from the header

    struct PendingOp {
        std::coroutine_handle<> handle{};
        std::shared_ptr<yams::daemon::SocketAwaiterBase> awaiter_shared{};
        int fd{-1};
        std::vector<uint8_t> read_buffer;
        void* external_buffer{nullptr};
        std::vector<uint8_t> data;
        size_t size{0};
        OpType type{OpType::Read};
        uint64_t generation{0};
    };

    // Shared helpers to reduce duplication in submit_* paths
    void enqueue_cancel_(int fd, size_t size, OpType type, uint64_t generation,
                         const std::shared_ptr<yams::daemon::SocketAwaiterBase>& awaiter,
                         std::coroutine_handle<> h) {
        if (awaiter) {
            awaiter->result.store(-1, std::memory_order_release);
            awaiter->error_code.store(ECONNRESET, std::memory_order_release);
            awaiter->cancelled.store(true, std::memory_order_release);
        }
        PendingOp cancel_op;
        cancel_op.handle = h;
        cancel_op.awaiter_shared = awaiter;
        cancel_op.fd = fd;
        cancel_op.size = size;
        cancel_op.type = type;
        cancel_op.generation = generation;
        {
            std::lock_guard lock(mutex_);
            cancel_queue_.push_back(std::move(cancel_op));
        }
        if (wake_pipe_[1] >= 0) {
            char byte = 1;
            [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
        }
    }

    bool validate_or_cancel_(int fd, uint64_t generation, size_t size, OpType type,
                             const std::shared_ptr<yams::daemon::SocketAwaiterBase>& awaiter,
                             std::coroutine_handle<> h) {
        bool mismatch = false;
        {
            std::lock_guard lock(mutex_);
            auto gen_it = socket_generations_.find(fd);
            mismatch = (gen_it == socket_generations_.end() || gen_it->second != generation);
            if (!mismatch) {
                return true;
            }
        }
        // Outside the lock, enqueue cancel via IO thread
        enqueue_cancel_(fd, size, type, generation, awaiter, h);
        return false;
    }

    bool register_event_or_cancel_(int fd, int16_t filter,
                                   const std::shared_ptr<yams::daemon::SocketAwaiterBase>& awaiter,
                                   std::coroutine_handle<> h, size_t size, OpType type,
                                   uint64_t generation) {
        struct kevent ev;
        EV_SET(&ev, fd, filter, EV_ADD | EV_ONESHOT, 0, 0, nullptr);
        if (kevent(kq_, &ev, 1, nullptr, 0, nullptr) == -1) {
            if (filter == EVFILT_READ) {
                spdlog::error("kevent EVFILT_READ register failed for fd {}: {}", fd,
                              strerror(errno));
            } else {
                spdlog::error("kevent EVFILT_WRITE register failed for fd {}: {}", fd,
                              strerror(errno));
            }
            // Remove the op we just queued and route cancellation through IO thread
            {
                std::lock_guard lock(mutex_);
                auto it = pending_ops_.find(fd);
                if (it != pending_ops_.end()) {
                    std::erase_if(it->second, [&](const PendingOp& p) {
                        return p.awaiter_shared == awaiter && p.handle == h && p.type == type;
                    });
                }
            }
            enqueue_cancel_(fd, size, type, generation, awaiter, h);
            return false;
        }
        return true;
    }

    void cancel_operation(PendingOp& op) {
        auto awaiter = op.awaiter_shared;
        spdlog::debug("AsyncIOContext(kqueue): cancelling op (fd={}, type={})", op.fd,
                      static_cast<int>(op.type));

        if (!awaiter || !op.handle)
            return;

        // Proceed with cancellation even if socket already unregistered
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (socket_generations_.find(op.fd) == socket_generations_.end()) {
                spdlog::debug(
                    "AsyncIOContext(kqueue): socket unregistered; continuing to cancel op (fd={})",
                    op.fd);
            }
        }

        auto local_handle = op.handle;

        // Check if awaiter is still alive
        bool is_alive = awaiter->alive.load(std::memory_order_acquire);
        if (!is_alive) {
            return;
        }

        // Use atomic exchange to ensure single-resume guarantee
        bool already_scheduled = awaiter->scheduled.exchange(true, std::memory_order_acq_rel);
        if (already_scheduled) {
            return;
        }

        awaiter->result.store(-1, std::memory_order_release);
        awaiter->error_code.store(ECONNRESET, std::memory_order_release);
        awaiter->cancelled.store(true, std::memory_order_release);
        awaiter->alive.store(false, std::memory_order_release);

        size_t refs = awaiter.use_count();
        if (refs <= 1) {
            spdlog::debug(
                "AsyncIOContext(kqueue): awaiter use_count={} during cancel resume (fd={})", refs,
                op.fd);
        }
        auto keep_alive = awaiter;

        // During shutdown/draining, avoid touching coroutine handles that may already be destroyed.
        if (draining_.load(std::memory_order_acquire)) {
            op.awaiter_shared.reset();
            op.handle = nullptr;
            return;
        }

        // Defensive: do not resume a completed/destroyed coroutine frame
        if (local_handle && !local_handle.done()) {
            try {
                op.awaiter_shared.reset();
                op.handle = nullptr;
                local_handle.resume();
            } catch (...) {
            }
        } else {
            spdlog::debug(
                "AsyncIOContext(kqueue): skip cancel resume because handle is null/done (fd={})",
                op.fd);
            op.awaiter_shared.reset();
            op.handle = nullptr;
        }
    }

    static constexpr int MAX_EVENTS = 64;
    int kq_;
    int wake_pipe_[2] = {-1, -1};
    mutable std::mutex mutex_;
    std::unordered_map<int, std::deque<PendingOp>> pending_ops_;
    std::unordered_map<int, uint64_t> socket_generations_;
    std::deque<PendingOp> cancel_queue_;
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> draining_{false};
};

#else
// Portable select() fallback (Linux/other POSIX) for AsyncIOContext::Impl
class AsyncIOContext::Impl {
public:
    Impl() : stop_requested_(false) {
        if (pipe(wake_pipe_) != 0) {
            throw std::runtime_error("Failed to create wake pipe");
        }
        int flags = fcntl(wake_pipe_[0], F_GETFL, 0);
        fcntl(wake_pipe_[0], F_SETFL, flags | O_NONBLOCK);
    }

    ~Impl() {
        if (wake_pipe_[0] >= 0)
            ::close(wake_pipe_[0]);
        if (wake_pipe_[1] >= 0)
            ::close(wake_pipe_[1]);
    }

    Result<void> register_socket(int fd, uint64_t generation) {
        std::lock_guard<std::mutex> lock(mutex_);
        socket_generations_[fd] = generation;
        return Result<void>();
    }

    void cancel_pending_operations(int fd) {
        bool enqueued = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = pending_ops_.find(fd);
            if (it != pending_ops_.end()) {
                for (auto& op : it->second)
                    cancel_queue_.push_back(std::move(op));
                pending_ops_.erase(it);
                enqueued = true;
            }
            socket_generations_.erase(fd);
        }
        if (enqueued && wake_pipe_[1] >= 0) {
            char byte = 1;
            [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
        }
    }

    Result<void> unregister_socket(int fd) {
        bool enqueued = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = pending_ops_.find(fd);
            if (it != pending_ops_.end()) {
                for (auto& op : it->second)
                    cancel_queue_.push_back(std::move(op));
                pending_ops_.erase(it);
                enqueued = true;
            }
            socket_generations_.erase(fd);
        }
        if (enqueued && wake_pipe_[1] >= 0) {
            char byte = 1;
            [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
        }
        return Result<void>();
    }

    void submit_read_erased(int fd, void* buffer, size_t size, std::coroutine_handle<> h,
                            std::shared_ptr<yams::daemon::SocketAwaiterBase> awaiter,
                            uint64_t generation) {
        if (!awaiter || !h)
            return;

        if (stop_requested_.load(std::memory_order_acquire) ||
            draining_.load(std::memory_order_acquire)) {
            awaiter->result.store(-1, std::memory_order_release);
            awaiter->error_code.store(ECONNRESET, std::memory_order_release);
            awaiter->cancelled.store(true, std::memory_order_release);
            PendingOp cancel_op;
            cancel_op.handle = h;
            cancel_op.awaiter_shared = awaiter;
            cancel_op.fd = fd;
            cancel_op.size = size;
            cancel_op.type = OpType::Read;
            cancel_op.generation = generation;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                cancel_queue_.push_back(std::move(cancel_op));
            }
            if (wake_pipe_[1] >= 0) {
                char byte = 1;
                [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
            }
            return;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto gen_it = socket_generations_.find(fd);
            if (gen_it == socket_generations_.end() || gen_it->second != generation) {
                awaiter->result.store(-1, std::memory_order_release);
                awaiter->error_code.store(ECONNRESET, std::memory_order_release);
                awaiter->cancelled.store(true, std::memory_order_release);
                PendingOp cancel_op;
                cancel_op.handle = h;
                cancel_op.awaiter_shared = awaiter;
                cancel_op.fd = fd;
                cancel_op.size = size;
                cancel_op.type = OpType::Read;
                cancel_op.generation = generation;
                cancel_queue_.push_back(std::move(cancel_op));
                if (wake_pipe_[1] >= 0) {
                    char byte = 1;
                    [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
                }
                return;
            }
        }

        PendingOp op;
        op.handle = h;
        op.awaiter_shared = awaiter;
        op.fd = fd;
        op.read_buffer.resize(size);
        op.external_buffer = buffer;
        op.size = size;
        op.type = OpType::Read;
        op.generation = generation;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_ops_[fd].push_back(std::move(op));
        }

        if (wake_pipe_[1] >= 0) {
            char byte = 1;
            [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
        }
    }

    void submit_write_erased(int fd, const void* buffer, size_t size, std::coroutine_handle<> h,
                             std::shared_ptr<yams::daemon::SocketAwaiterBase> awaiter,
                             uint64_t generation) {
        if (!awaiter || !h)
            return;

        if (stop_requested_.load(std::memory_order_acquire) ||
            draining_.load(std::memory_order_acquire)) {
            awaiter->result.store(-1, std::memory_order_release);
            awaiter->error_code.store(ECONNRESET, std::memory_order_release);
            awaiter->cancelled.store(true, std::memory_order_release);
            PendingOp cancel_op;
            cancel_op.handle = h;
            cancel_op.awaiter_shared = awaiter;
            cancel_op.fd = fd;
            cancel_op.size = size;
            cancel_op.type = OpType::Write;
            cancel_op.generation = generation;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                cancel_queue_.push_back(std::move(cancel_op));
            }
            if (wake_pipe_[1] >= 0) {
                char byte = 1;
                [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
            }
            return;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto gen_it = socket_generations_.find(fd);
            if (gen_it == socket_generations_.end() || gen_it->second != generation) {
                awaiter->result.store(-1, std::memory_order_release);
                awaiter->error_code.store(ECONNRESET, std::memory_order_release);
                awaiter->cancelled.store(true, std::memory_order_release);
                PendingOp cancel_op;
                cancel_op.handle = h;
                cancel_op.awaiter_shared = awaiter;
                cancel_op.fd = fd;
                cancel_op.size = size;
                cancel_op.type = OpType::Write;
                cancel_op.generation = generation;
                cancel_queue_.push_back(std::move(cancel_op));
                if (wake_pipe_[1] >= 0) {
                    char byte = 1;
                    [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
                }
                return;
            }
        }

        PendingOp op;
        op.handle = h;
        op.awaiter_shared = awaiter;
        op.fd = fd;
        op.data.assign(static_cast<const uint8_t*>(buffer),
                       static_cast<const uint8_t*>(buffer) + size);
        op.size = size;
        op.type = OpType::Write;
        op.generation = generation;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_ops_[fd].push_back(std::move(op));
        }

        if (wake_pipe_[1] >= 0) {
            char byte = 1;
            [[maybe_unused]] ssize_t n = write(wake_pipe_[1], &byte, 1);
        }
    }

    void run() {
        for (;;) {
            // Drain queued cancellations
            {
                std::deque<PendingOp> to_cancel;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!cancel_queue_.empty())
                        to_cancel.swap(cancel_queue_);
                }
                for (auto& op : to_cancel)
                    cancel_operation(op);
            }

            if (stop_requested_.load(std::memory_order_acquire)) {
                std::deque<PendingOp> to_cancel;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!pending_ops_.empty()) {
                        for (auto& [fd, dq] : pending_ops_) {
                            for (auto& op : dq)
                                to_cancel.push_back(std::move(op));
                        }
                        pending_ops_.clear();
                    }
                    socket_generations_.clear();
                }
                for (auto& op : to_cancel)
                    cancel_operation(op);

                bool more_pending = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    more_pending = !cancel_queue_.empty() || !pending_ops_.empty();
                }
                if (!more_pending)
                    break;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }

#ifndef _WIN32
            fd_set rfds;
            fd_set wfds;
            FD_ZERO(&rfds);
            FD_ZERO(&wfds);
            int maxfd = -1;

            // Always monitor wake pipe for notifications
            if (wake_pipe_[0] >= 0) {
                FD_SET(wake_pipe_[0], &rfds);
                maxfd = std::max(maxfd, wake_pipe_[0]);
            }

            // Snapshot fds and mark readiness interest
            std::vector<int> fds_snapshot;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                fds_snapshot.reserve(pending_ops_.size());
                for (const auto& [fd, dq] : pending_ops_) {
                    bool want_read = false, want_write = false;
                    for (const auto& op : dq) {
                        if (op.type == OpType::Read)
                            want_read = true;
                        if (op.type == OpType::Write)
                            want_write = true;
                        if (want_read && want_write)
                            break;
                    }
                    if (want_read) {
                        FD_SET(fd, &rfds);
                        maxfd = std::max(maxfd, fd);
                    }
                    if (want_write) {
                        FD_SET(fd, &wfds);
                        maxfd = std::max(maxfd, fd);
                    }
                    fds_snapshot.push_back(fd);
                }
            }

            struct timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100000; // 100ms
            int nev = select(maxfd + 1, &rfds, &wfds, nullptr, &tv);
            if (stop_requested_.load(std::memory_order_acquire))
                continue;
            if (nev < 0) {
                if (errno == EINTR)
                    continue;
                spdlog::warn("AsyncIOContext(select): select() error: {}", strerror(errno));
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            // Handle wake pipe
            if (wake_pipe_[0] >= 0 && FD_ISSET(wake_pipe_[0], &rfds)) {
                char buffer[8];
                while (read(wake_pipe_[0], buffer, sizeof(buffer)) > 0) {
                }
                std::deque<PendingOp> to_cancel;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!cancel_queue_.empty())
                        to_cancel.swap(cancel_queue_);
                }
                for (auto& op : to_cancel)
                    cancel_operation(op);
            }

            // Process fds
            for (int fd : fds_snapshot) {
                bool ready_read = FD_ISSET(fd, &rfds);
                bool ready_write = FD_ISSET(fd, &wfds);
                if (!ready_read && !ready_write)
                    continue;

                PendingOp op{};
                bool got = false;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto it = pending_ops_.find(fd);
                    if (it != pending_ops_.end() && !it->second.empty()) {
                        if (ready_read) {
                            auto& dq = it->second;
                            auto it_op = std::find_if(dq.begin(), dq.end(), [](const PendingOp& p) {
                                return p.type == OpType::Read;
                            });
                            if (it_op != dq.end()) {
                                op = std::move(*it_op);
                                dq.erase(it_op);
                                got = true;
                            }
                        }
                        if (!got && ready_write) {
                            auto& dq = it->second;
                            auto it_op = std::find_if(dq.begin(), dq.end(), [](const PendingOp& p) {
                                return p.type == OpType::Write;
                            });
                            if (it_op != dq.end()) {
                                op = std::move(*it_op);
                                dq.erase(it_op);
                                got = true;
                            }
                        }
                    }
                }
                if (!got)
                    continue;

                if (!op.awaiter_shared || !op.handle)
                    continue;

                // Validate registration
                bool is_valid = false;
                uint64_t current_generation = 0;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto gen_it = socket_generations_.find(fd);
                    is_valid =
                        (gen_it != socket_generations_.end() && gen_it->second == op.generation);
                    if (gen_it != socket_generations_.end())
                        current_generation = gen_it->second;
                }
                if (current_generation == 0)
                    continue;

                if (!is_valid) {
                    cancel_operation(op);
                    continue;
                }

                // Perform non-blocking I/O
                ssize_t io_result = -1;
                int io_errno = 0;
                if (op.type == OpType::Read) {
                    io_result = recv(fd, op.read_buffer.data(), op.size, MSG_DONTWAIT);
                    io_errno = errno;
                } else {
                    size_t to_write = std::min(op.size, op.data.size());
                    if (to_write == 0) {
                        io_result = -1;
                        io_errno = EPIPE;
                    } else {
                        io_result = send(fd, op.data.data(), to_write, MSG_DONTWAIT | MSG_NOSIGNAL);
                        io_errno = errno;
                    }
                }

                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto gen_it = socket_generations_.find(fd);
                    is_valid =
                        (gen_it != socket_generations_.end() && gen_it->second == op.generation);
                }
                if (!is_valid) {
                    cancel_operation(op);
                    continue;
                }

                auto awaiter_shared = op.awaiter_shared;
                if (!awaiter_shared || awaiter_shared->cancelled.load())
                    continue;

                if (io_result < 0) {
                    if (io_errno == EAGAIN || io_errno == EWOULDBLOCK) {
                        // Not ready; requeue op
                        std::lock_guard<std::mutex> lock(mutex_);
                        pending_ops_[fd].push_front(std::move(op));
                        continue;
                    }
                    if (io_errno == 0) {
#ifdef EPIPE
                        io_errno = EPIPE;
#else
                        io_errno = ECONNRESET;
#endif
                    }
                    awaiter_shared->error_code.store(io_errno, std::memory_order_release);
                } else if (op.type == OpType::Read && io_result > 0) {
                    if (op.external_buffer && static_cast<size_t>(io_result) <= op.size) {
                        std::memcpy(op.external_buffer, op.read_buffer.data(), io_result);
                    }
                }
                awaiter_shared->result.store(io_result, std::memory_order_release);

                // Resume safely
                bool resume_scheduled =
                    awaiter_shared->scheduled.exchange(true, std::memory_order_acq_rel);
                if (!resume_scheduled && !awaiter_shared->cancelled.load() && !op.handle.done()) {
                    awaiter_shared->alive.store(false, std::memory_order_release);
                    auto local_handle = op.handle;
                    try {
                        local_handle.resume();
                    } catch (...) {
                    }
                }
            }
#else
            // Non-POSIX fallback not implemented
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
#endif
        }
    }

    void stop() {
        draining_.store(true, std::memory_order_release);
        stop_requested_.store(true, std::memory_order_release);
        if (wake_pipe_[1] >= 0) {
            char byte = 1;
            write(wake_pipe_[1], &byte, 1);
        }
    }

private:
    enum class OpType { Read, Write };

    struct PendingOp {
        std::coroutine_handle<> handle{};
        std::shared_ptr<yams::daemon::SocketAwaiterBase> awaiter_shared{};
        int fd{-1};
        std::vector<uint8_t> read_buffer;
        void* external_buffer{nullptr};
        std::vector<uint8_t> data;
        size_t size{0};
        OpType type{OpType::Read};
        uint64_t generation{0};
    };

    void cancel_operation(PendingOp& op) {
        auto awaiter = op.awaiter_shared;
        if (!awaiter || !op.handle)
            return;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            // proceed even if socket is unregistered
        }

        auto local_handle = op.handle;

        bool is_alive = awaiter->alive.load(std::memory_order_acquire);
        if (!is_alive)
            return;

        bool already_scheduled = awaiter->scheduled.exchange(true, std::memory_order_acq_rel);
        if (already_scheduled)
            return;

        awaiter->result.store(-1, std::memory_order_release);
        awaiter->error_code.store(ECONNRESET, std::memory_order_release);
        awaiter->cancelled.store(true, std::memory_order_release);
        awaiter->alive.store(false, std::memory_order_release);

        auto keep_alive = awaiter;

        if (draining_.load(std::memory_order_acquire)) {
            op.awaiter_shared.reset();
            op.handle = nullptr;
            return;
        }

        try {
            op.awaiter_shared.reset();
            op.handle = nullptr;
            if (local_handle)
                local_handle.resume();
        } catch (...) {
        }
    }

    int wake_pipe_[2] = {-1, -1};
    mutable std::mutex mutex_;
    std::unordered_map<int, std::deque<PendingOp>> pending_ops_;
    std::unordered_map<int, uint64_t> socket_generations_;
    std::deque<PendingOp> cancel_queue_;
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> draining_{false};
};
#endif

// ============================================================================
// AsyncIOContext Public Methods
// ============================================================================

AsyncIOContext::AsyncIOContext() : pImpl(std::make_unique<Impl>()) {}
AsyncIOContext::~AsyncIOContext() {
    if (pImpl) {
        pImpl->stop();
    }
}
AsyncIOContext::AsyncIOContext(AsyncIOContext&&) noexcept = default;
AsyncIOContext& AsyncIOContext::operator=(AsyncIOContext&&) noexcept = default;

Result<void> AsyncIOContext::register_socket(int fd, uint64_t generation) {
    return pImpl->register_socket(fd, generation);
}
Result<void> AsyncIOContext::unregister_socket(int fd) {
    return pImpl->unregister_socket(fd);
}
void AsyncIOContext::cancel_pending_operations(int fd) {
    pImpl->cancel_pending_operations(fd);
}

void AsyncIOContext::run() {
    pImpl->run();
}
void AsyncIOContext::stop() {
    pImpl->stop();
}

// Type-erased submit functions forwarding to Impl
void AsyncIOContext::submit_read_erased(int fd, void* buffer, size_t size,
                                        std::coroutine_handle<> h,
                                        std::shared_ptr<SocketAwaiterBase> awaiter,
                                        uint64_t generation) {
    pImpl->submit_read_erased(fd, buffer, size, h, awaiter, generation);
}

void AsyncIOContext::submit_write_erased(int fd, const void* buffer, size_t size,
                                         std::coroutine_handle<> h,
                                         std::shared_ptr<SocketAwaiterBase> awaiter,
                                         uint64_t generation) {
    pImpl->submit_write_erased(fd, buffer, size, h, awaiter, generation);
}

// (No explicit template instantiations needed; header-only templates forward to erased API.)

} // namespace yams::daemon
