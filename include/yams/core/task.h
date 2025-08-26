#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <thread>
#include <utility>

namespace yams {

// Generic Task<T> coroutine type
template <typename T> class Task {
public:
    struct promise_type {
        std::optional<T> value_;
        std::exception_ptr exception_;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }

        template <typename U> void return_value(U&& value) { value_ = std::forward<U>(value); }

        void unhandled_exception() { exception_ = std::current_exception(); }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type h) : handle_(h) {}

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_)
                handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        if (handle_)
            handle_.destroy();
    }

    // Run to completion and return result (or throw)
    T get() {
        while (!handle_.done()) {
            std::this_thread::yield();
        }
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        return std::move(handle_.promise().value_.value());
    }

    // Awaitable interface
    bool await_ready() const noexcept { return handle_.done(); }

    void await_suspend(std::coroutine_handle<> h) {
        (void)h;
        handle_.resume();
    }

    T await_resume() {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
        return std::move(handle_.promise().value_.value());
    }

private:
    handle_type handle_{};
};

// Specialization for Task<void>
template <> class Task<void> {
public:
    struct promise_type {
        std::exception_ptr exception_;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }

        void return_void() noexcept {}
        void unhandled_exception() { exception_ = std::current_exception(); }
    };

    using handle_type = std::coroutine_handle<promise_type>;

    explicit Task(handle_type h) : handle_(h) {}

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle_)
                handle_.destroy();
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        if (handle_)
            handle_.destroy();
    }

    // Run to completion (or throw)
    void get() {
        while (!handle_.done()) {
            std::this_thread::yield();
        }
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
    }

    // Awaitable interface
    bool await_ready() const noexcept { return handle_.done(); }

    void await_suspend(std::coroutine_handle<> h) {
        (void)h;
        handle_.resume();
    }

    void await_resume() {
        if (handle_.promise().exception_) {
            std::rethrow_exception(handle_.promise().exception_);
        }
    }

private:
    handle_type handle_{};
};

} // namespace yams

// Provide alias into yams::daemon so existing unqualified uses resolve
namespace yams::daemon {
template <typename T = void> using Task = ::yams::Task<T>;
}