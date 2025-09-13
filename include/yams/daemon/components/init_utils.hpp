#pragma once

#include <spdlog/spdlog.h>
#include <chrono>
#include <functional>
#include <future>
#include <map>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>

#include <yams/core/types.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>

namespace yams::daemon::init {

// Internal helper: detect Result-like by presence of .has_value()/operator bool and .error()
template <typename T, typename = void> struct is_result_like : std::false_type {};
template <typename T>
struct is_result_like<T, std::void_t<decltype(std::declval<T>().operator bool()),
                                     decltype(std::declval<T>().error())>> : std::true_type {};

// record_duration: runs fn(), records elapsed ms into durations[name], and returns fn()'s result.
// Fn should return yams::Result<...> or a value. The function does not catch exceptions from fn.
template <typename Fn, typename Durations>
auto record_duration(const std::string& name, Fn&& fn, Durations& durations) -> decltype(fn()) {
    auto start = std::chrono::steady_clock::now();
    auto res = fn();
    auto end = std::chrono::steady_clock::now();
    uint64_t ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    try {
        durations.emplace(name, ms);
    } catch (...) {
    }
    return res;
}

// with_timeout: runs fn() on a separate thread and waits up to timeout_ms.
// On timeout, returns Error{Timeout, "timeout"}. Fn should return yams::Result<T>.
template <typename T, typename Fn> yams::Result<T> with_timeout(Fn&& fn, int timeout_ms) {
    try {
        auto fut =
            std::async(std::launch::async, [f = std::forward<Fn>(fn)]() mutable { return f(); });
        if (fut.wait_for(std::chrono::milliseconds(timeout_ms)) == std::future_status::ready) {
            return fut.get();
        }
        return yams::Error{yams::ErrorCode::Timeout, "timeout"};
    } catch (const std::exception& e) {
        return yams::Error{yams::ErrorCode::InternalError, e.what()};
    }
}

// Default transient predicate based on error code or message
inline bool default_transient_predicate(const yams::Error& err) {
    if (err.code == yams::ErrorCode::Timeout)
        return true;
    std::string msg = err.message;
    for (auto& c : msg)
        c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
    return msg.find("busy") != std::string::npos || msg.find("lock") != std::string::npos ||
           msg.find("locked") != std::string::npos || msg.find("timeout") != std::string::npos;
}

// with_retry: runs fn() up to attempts times with backoff between attempts when transient.
// backoff_ms(attempt_index) returns milliseconds to sleep before next attempt (attempt_index>=1).
// Fn should return yams::Result<T>.
template <typename T, typename Fn, typename Backoff>
yams::Result<T> with_retry(Fn&& fn, int attempts, Backoff backoff_ms) {
    int tries = attempts < 1 ? 1 : attempts;
    for (int i = 0; i < tries; ++i) {
        auto r = fn();
        if (r)
            return r;
        const auto& err = r.error();
        bool transient = default_transient_predicate(err);
        if (i + 1 >= tries || !transient)
            return r;
        int delay = 0;
        try {
            delay = backoff_ms(i + 1);
        } catch (...) {
        }
        if (delay > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    }
    // Unreachable
    return yams::Error{yams::ErrorCode::Unknown, "retry failed"};
}

// step: executes fn() and logs standardized start/end, returns the result.
// Fn should return yams::Result<T>.
template <typename T, typename Fn> yams::Result<T> step(const std::string& name, Fn&& fn) {
    spdlog::info("[InitStep] {}: start", name);
    auto res = fn();
    if (res) {
        spdlog::info("[InitStep] {}: ok", name);
    } else {
        spdlog::warn("[InitStep] {}: failed: {}", name, res.error().message);
    }
    return res;
}

} // namespace yams::daemon::init

// Awaitable helpers live in the same header for convenience
namespace yams::daemon::init {

// await_record_duration: co_awaits fn() and records elapsed ms under name, returns awaited value
// Fn must be callable with no args and return boost::asio::awaitable<T>.
template <typename Fn, typename Durations, typename Awaitable = std::invoke_result_t<Fn>,
          typename T = typename Awaitable::value_type>
boost::asio::awaitable<T> await_record_duration(const std::string& name, Fn&& fn,
                                                Durations& durations) {
    auto start = std::chrono::steady_clock::now();
    T value = co_await fn();
    auto end = std::chrono::steady_clock::now();
    uint64_t ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    try {
        durations.emplace(name, ms);
    } catch (...) {
    }
    co_return value;
}

// await_step: logs standardized start/end around an awaitable step.
// Fn must return boost::asio::awaitable<T>. If T is Result-like, logs failure detail.
template <typename Fn, typename Awaitable = std::invoke_result_t<Fn>,
          typename T = typename Awaitable::value_type>
boost::asio::awaitable<T> await_step(const std::string& name, Fn&& fn) {
    spdlog::info("[InitStep] {}: start", name);
    T value = co_await fn();
    if constexpr (is_result_like<T>::value) {
        if (value) {
            spdlog::info("[InitStep] {}: ok", name);
        } else {
            spdlog::warn("[InitStep] {}: failed: {}", name, value.error().message);
        }
    } else if constexpr (std::is_same_v<T, bool>) {
        spdlog::info("[InitStep] {}: {}", name, value ? "ok" : "failed");
    } else {
        spdlog::info("[InitStep] {}: ok", name);
    }
    co_return value;
}

// await_with_timeout: runs an awaitable that returns yams::Result<T> with a timeout.
// Fn must return boost::asio::awaitable<yams::Result<T>>.
template <typename T, typename Fn>
boost::asio::awaitable<yams::Result<T>> await_with_timeout(Fn&& fn, int timeout_ms) {
    using namespace boost::asio::experimental::awaitable_operators;
    auto ex = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(ex);
    timer.expires_after(std::chrono::milliseconds(timeout_ms));
    auto which = co_await (fn() || timer.async_wait(boost::asio::use_awaitable));
    if (which.index() == 1) {
        co_return yams::Error{yams::ErrorCode::Timeout, "timeout"};
    }
    co_return std::move(std::get<0>(which));
}

// await_with_retry: runs an awaitable fn up to attempts with backoff between attempts.
// Fn must return boost::asio::awaitable<yams::Result<T>>.
template <typename T, typename Fn, typename Backoff>
boost::asio::awaitable<yams::Result<T>> await_with_retry(Fn&& fn, int attempts,
                                                         Backoff backoff_ms) {
    auto ex = co_await boost::asio::this_coro::executor;
    int tries = attempts < 1 ? 1 : attempts;
    for (int i = 0; i < tries; ++i) {
        auto r = co_await fn();
        if (r)
            co_return r;
        bool transient = default_transient_predicate(r.error());
        if (i + 1 >= tries || !transient)
            co_return r;
        int delay = 0;
        try {
            delay = backoff_ms(i + 1);
        } catch (...) {
        }
        if (delay > 0) {
            boost::asio::steady_timer t(ex);
            t.expires_after(std::chrono::milliseconds(delay));
            co_await t.async_wait(boost::asio::use_awaitable);
        }
    }
    co_return yams::Error{yams::ErrorCode::Unknown, "retry failed"};
}

} // namespace yams::daemon::init
