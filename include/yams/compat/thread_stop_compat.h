// Minimal portability shim for std::jthread/std::stop_token on libcs that lack them.
// Prefer native C++20 types when available; otherwise provide no-op fallbacks.
#pragma once

#include <thread>
#include <type_traits>

#if __has_include(<version>)
#include <version>
#endif

#if __has_include(<stop_token>)
#include <stop_token>
#endif

namespace yams::compat {

#if defined(__cpp_lib_jthread) && (__cpp_lib_jthread >= 201911L)
// Native support present
using jthread = std::jthread;
using stop_token = std::stop_token;
using stop_source = std::stop_source;

#else

// Fallback stop_token: always reports no stop requested
struct stop_token {
    constexpr bool stop_requested() const noexcept { return false; }
};

// Fallback stop_source: no-op implementation
struct stop_source {
    stop_token get_token() const noexcept { return stop_token{}; }
    void request_stop() noexcept { /* no-op */ }
    bool stop_requested() const noexcept { return false; }
};

// Fallback jthread wrapper over std::thread with a no-op request_stop
class jthread {
public:
    jthread() = default;

    template <class F, class... Args> explicit jthread(F&& f, Args&&... args) {
        // Invoke callable with a compat::stop_token if it accepts one; otherwise without it
        t_ = std::thread([fn = std::forward<F>(f), ... as = std::forward<Args>(args)]() mutable {
            if constexpr (std::is_invocable_v<F, stop_token, Args...>) {
                fn(stop_token{}, as...);
            } else {
                fn(as...);
            }
        });
    }

    jthread(const jthread&) = delete;
    jthread& operator=(const jthread&) = delete;

    jthread(jthread&& other) noexcept : t_(std::move(other.t_)) {}
    jthread& operator=(jthread&& other) noexcept {
        if (this != &other) {
            if (t_.joinable())
                t_.join();
            t_ = std::move(other.t_);
        }
        return *this;
    }

    ~jthread() {
        if (t_.joinable())
            t_.join();
    }

    bool joinable() const noexcept { return t_.joinable(); }
    void join() {
        if (t_.joinable())
            t_.join();
    }
    void request_stop() noexcept { /* no-op */ }

private:
    std::thread t_;
};

#endif // __cpp_lib_jthread

} // namespace yams::compat
