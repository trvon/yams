#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <thread>
#include <utility>

namespace yams {

// Generic Task<T> coroutine type with proper continuation chaining.
template <typename T> class Task {
public:
    struct promise_type {
        std::optional<T> value_;
        std::exception_ptr exception_;
        std::coroutine_handle<> continuation_{};

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        // Start immediately so fire-and-forget tasks run without explicit await
        std::suspend_never initial_suspend() noexcept { return {}; }

        // Final suspend resumes the awaiting continuation (if any).
        struct final_awaiter {
            bool await_ready() const noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                // Transfer resumption to the continuation to avoid reentrancy/UAF; the
                // coroutine is suspended at final_suspend when this is returned.
                return h.promise().continuation_ ? h.promise().continuation_
                                                 : std::noop_coroutine();
            }
            void await_resume() const noexcept {}
        };
        final_awaiter final_suspend() noexcept { return {}; }

        template <typename U> void return_value(U&& value) { value_ = std::forward<U>(value); }

        void unhandled_exception() { exception_ = std::current_exception(); }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type h) : handle_(h) {}

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            // Only destroy if the existing coroutine is already done
            if (handle_ && handle_.done()) {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        // Do not destroy here. In complex async systems, external schedulers/IO contexts
        // may still hold references to awaiters that will resume this frame during
        // cancellation/shutdown. Destroying the frame here risks UAF. Prefer explicit
        // destruction after get(), or allow process shutdown to reclaim.
    }

    // Run to completion and return result (or throw)
    T get() {
        // If not started, start now
        if (handle_ && !handle_.done()) {
            // Busy-wait with yields until completion (for simple sync bridging)
            handle_.resume();
            while (!handle_.done()) {
                std::this_thread::yield();
            }
        }
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        T out = std::move(handle_.promise().value_.value());
        return out;
    }

    // Awaitable interface
    bool await_ready() const noexcept { return !handle_ || handle_.done(); }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        // Chain continuation then start/resume the task.
        handle_.promise().continuation_ = h;
        handle_.resume();
    }

    T await_resume() {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        T out = std::move(handle_.promise().value_.value());
        // Do NOT destroy the frame here; destruction during resume() unwinding can UAF.
        // Let the Task's destructor clean it up once done, or get().
        return out;
    }

private:
    handle_type handle_{};
};

// Specialization for Task<void>
template <> class Task<void> {
public:
    struct promise_type {
        std::exception_ptr exception_{};
        std::coroutine_handle<> continuation_{};

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        // Start immediately so fire-and-forget tasks run without explicit await
        std::suspend_never initial_suspend() noexcept { return {}; }

        struct final_awaiter {
            bool await_ready() const noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                return h.promise().continuation_ ? h.promise().continuation_
                                                 : std::noop_coroutine();
            }
            void await_resume() const noexcept {}
        };
        final_awaiter final_suspend() noexcept { return {}; }

        void return_void() noexcept {}
        void unhandled_exception() { exception_ = std::current_exception(); }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type h) : handle_(h) {}

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_ && handle_.done()) {
                handle_.destroy();
            }
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        // See comment in Task<T>::~Task(): avoid destroying frames here to prevent UAF
        // when external components may still legally resume during cancellation.
    }

    // Run to completion (or throw)
    void get() {
        if (handle_ && !handle_.done()) {
            handle_.resume();
            while (!handle_.done()) {
                std::this_thread::yield();
            }
        }
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        // NOTE: Do not destroy handle here. The coroutine frame will be leaked,
        // but this prevents a UAF crash when the IO context is on another thread.
        // Proper fix is to eliminate sync-over-async calls to get().
    }

    // Awaitable interface
    bool await_ready() const noexcept { return !handle_ || handle_.done(); }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        handle_.promise().continuation_ = h;
        handle_.resume();
    }

    void await_resume() {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        // Do not destroy here; see comment above in Task<T>::await_resume().
    }

private:
    handle_type handle_{};
};

} // namespace yams

// Provide alias into yams::daemon so existing unqualified uses resolve
namespace yams::daemon {
template <typename T = void> using Task = ::yams::Task<T>;
}
