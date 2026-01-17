// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

/**
 * @file result_helpers.hpp
 * @brief Error handling macros and utilities for Result<T>
 *
 * Provides TRY macros to reduce boilerplate in error handling code.
 * These macros implement early return on error, similar to Rust's ? operator.
 *
 * @see ADR-0004: MetadataRepository Refactor with Templates and Metaprogramming
 */

#include <yams/core/cpp23_features.hpp>
#include <yams/core/types.h>

#include <optional>
#include <utility>

namespace yams::metadata::repository {

// ============================================================================
// TRY Macros - Early return on error
// ============================================================================

/**
 * @def YAMS_TRY(expr)
 * @brief Evaluate expression and return early if it's an error
 *
 * Use this for expressions that return Result<void> when you don't need
 * the value, just need to propagate errors.
 *
 * Example:
 * @code
 * Result<void> doWork() {
 *     YAMS_TRY(step1());  // Returns if step1() fails
 *     YAMS_TRY(step2());  // Returns if step2() fails
 *     return {};          // Success
 * }
 * @endcode
 */
#define YAMS_TRY(expr)                                                                             \
    do {                                                                                           \
        auto _yams_try_result = (expr);                                                            \
        if (!_yams_try_result.has_value()) {                                                       \
            return _yams_try_result.error();                                                       \
        }                                                                                          \
    } while (0)

/**
 * @def YAMS_TRY_ASSIGN(var, expr)
 * @brief Evaluate expression, assign value to var, or return error
 *
 * Use this when you need the value from a Result<T> and want to propagate
 * errors automatically. Each invocation uses a unique variable name via __LINE__.
 *
 * Example:
 * @code
 * Result<std::string> readFile(const std::string& path) {
 *     Result<Handle> handleRes = openFile(path);
 *     YAMS_TRY_ASSIGN(auto handle, handleRes);
 *     return handle.read();
 * }
 * @endcode
 *
 * @note This macro introduces a variable in the current scope. It cannot be
 *       used inside a single-statement if/while/for body without braces.
 * @note Due to macro expansion rules, you should store the expression in a
 *       named Result variable first if it's complex.
 */
#define YAMS_TRY_ASSIGN(var, expr)                                                                 \
    do {                                                                                           \
        auto _yams_try_r = (expr);                                                                 \
        if (!_yams_try_r.has_value()) {                                                            \
            return _yams_try_r.error();                                                            \
        }                                                                                          \
        var = std::move(_yams_try_r).value();                                                      \
    } while (0)

/**
 * @def YAMS_TRY_UNWRAP(var, expr)
 * @brief Declare and initialize variable from Result, returning error if failed
 *
 * This is the preferred macro for declaring a new variable initialized from
 * a Result. Uses if-init statement for cleaner scope.
 *
 * Example:
 * @code
 * Result<int> compute() {
 *     YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));  // stmt is CachedStatement
 *     YAMS_TRY_UNWRAP(hasRow, stmt->step());         // hasRow is bool
 *     return hasRow ? stmt->getInt(0) : 0;
 * }
 * @endcode
 */
#define YAMS_TRY_UNWRAP(var, expr)                                                                 \
    auto _yams_res_##var = (expr);                                                                 \
    if (!_yams_res_##var.has_value()) {                                                            \
        return _yams_res_##var.error();                                                            \
    }                                                                                              \
    auto var = std::move(_yams_res_##var).value()

// ============================================================================
// Result Transformation Helpers
// ============================================================================

/**
 * @brief Transform a Result<T> to Result<U> using a mapping function
 *
 * If the result contains a value, applies the function and returns the result.
 * If the result contains an error, propagates the error unchanged.
 *
 * @param result The input Result<T>
 * @param func A function T -> U
 * @return Result<U>
 *
 * Example:
 * @code
 * Result<int> getCount();
 * Result<std::string> getCountString() {
 *     return transform(getCount(), [](int n) { return std::to_string(n); });
 * }
 * @endcode
 */
template <typename T, typename Func>
auto transform(Result<T>&& result, Func&& func) -> Result<decltype(func(std::declval<T>()))> {
    if (!result.has_value()) {
        return result.error();
    }
    return func(std::move(result).value());
}

template <typename T, typename Func>
auto transform(const Result<T>& result, Func&& func) -> Result<decltype(func(std::declval<T>()))> {
    if (!result.has_value()) {
        return result.error();
    }
    return func(result.value());
}

/**
 * @brief Chain Result operations (and_then / flatMap)
 *
 * If the result contains a value, applies the function which must return a Result.
 * If the result contains an error, propagates the error unchanged.
 *
 * @param result The input Result<T>
 * @param func A function T -> Result<U>
 * @return Result<U>
 *
 * Example:
 * @code
 * Result<Statement> prepare(const std::string& sql);
 * Result<bool> execute(Statement& stmt);
 *
 * Result<bool> prepareAndExecute(const std::string& sql) {
 *     return and_then(prepare(sql), [](Statement& stmt) {
 *         return execute(stmt);
 *     });
 * }
 * @endcode
 */
template <typename T, typename Func>
auto and_then(Result<T>&& result, Func&& func) -> decltype(func(std::declval<T>())) {
    if (!result.has_value()) {
        return result.error();
    }
    return func(std::move(result).value());
}

template <typename T, typename Func>
auto and_then(Result<T>& result, Func&& func) -> decltype(func(std::declval<T&>())) {
    if (!result.has_value()) {
        return result.error();
    }
    return func(result.value());
}

/**
 * @brief Handle error case with a fallback function
 *
 * If the result contains a value, returns it unchanged.
 * If the result contains an error, calls the handler to produce a value.
 *
 * @param result The input Result<T>
 * @param handler A function Error -> T
 * @return T
 *
 * Example:
 * @code
 * int count = or_else(getCount(), [](const Error& e) {
 *     spdlog::warn("Failed to get count: {}", e.message);
 *     return 0; // Default value
 * });
 * @endcode
 */
template <typename T, typename Handler> T or_else(Result<T>&& result, Handler&& handler) {
    if (result.has_value()) {
        return std::move(result).value();
    }
    return handler(result.error());
}

template <typename T, typename Handler> T or_else(const Result<T>& result, Handler&& handler) {
    if (result.has_value()) {
        return result.value();
    }
    return handler(result.error());
}

/**
 * @brief Get value or return a default
 *
 * @param result The input Result<T>
 * @param defaultValue Value to return if result contains error
 * @return T
 */
template <typename T> T value_or(Result<T>&& result, T defaultValue) {
    if (result.has_value()) {
        return std::move(result).value();
    }
    return std::move(defaultValue);
}

template <typename T> T value_or(const Result<T>& result, const T& defaultValue) {
    if (result.has_value()) {
        return result.value();
    }
    return defaultValue;
}

// ============================================================================
// Optional/Result Conversion
// ============================================================================

/**
 * @brief Convert Result<T> to optional<T>, discarding any error
 */
template <typename T> std::optional<T> to_optional(Result<T>&& result) {
    if (result.has_value()) {
        return std::move(result).value();
    }
    return std::nullopt;
}

template <typename T> std::optional<T> to_optional(const Result<T>& result) {
    if (result.has_value()) {
        return result.value();
    }
    return std::nullopt;
}

/**
 * @brief Convert optional<T> to Result<T>, with error if empty
 */
template <typename T>
Result<T> from_optional(std::optional<T>&& opt, const Error& error = Error{ErrorCode::NotFound}) {
    if (opt.has_value()) {
        return std::move(opt).value();
    }
    return error;
}

template <typename T>
Result<T> from_optional(const std::optional<T>& opt,
                        const Error& error = Error{ErrorCode::NotFound}) {
    if (opt.has_value()) {
        return opt.value();
    }
    return error;
}

// ============================================================================
// Result Combination
// ============================================================================

/**
 * @brief Combine multiple Results into a Result of tuple
 *
 * If all results contain values, returns a tuple of all values.
 * If any result contains an error, returns the first error encountered.
 *
 * Example:
 * @code
 * auto combined = combine(getA(), getB(), getC());
 * if (combined) {
 *     auto [a, b, c] = combined.value();
 *     // use a, b, c
 * }
 * @endcode
 */
template <typename... Ts> Result<std::tuple<Ts...>> combine(Result<Ts>&&... results) {
    // Check for first error
    Error* firstError = nullptr;
    ((results.has_value() ? void()
                          : (firstError == nullptr ? firstError = &results.error() : void())),
     ...);

    if (firstError != nullptr) {
        return *firstError;
    }

    return std::make_tuple(std::move(results).value()...);
}

// ============================================================================
// Scope Guard for RAII-style cleanup
// ============================================================================

/**
 * @brief Execute cleanup code on scope exit
 *
 * Useful for ensuring cleanup happens regardless of early returns.
 *
 * Example:
 * @code
 * Result<void> doWork() {
 *     auto cleanup = scope_exit([&] { releaseResource(); });
 *     YAMS_TRY(step1());
 *     YAMS_TRY(step2());
 *     cleanup.dismiss(); // Don't run cleanup on success
 *     return {};
 * }
 * @endcode
 */
template <typename Func> class ScopeGuard {
public:
    explicit ScopeGuard(Func func) : func_(std::move(func)) {}

    ~ScopeGuard() {
        if (active_) {
            func_();
        }
    }

    // Move-only
    ScopeGuard(ScopeGuard&& other) noexcept
        : func_(std::move(other.func_)), active_(other.active_) {
        other.active_ = false;
    }
    ScopeGuard& operator=(ScopeGuard&&) = delete;
    ScopeGuard(const ScopeGuard&) = delete;
    ScopeGuard& operator=(const ScopeGuard&) = delete;

    void dismiss() noexcept { active_ = false; }

private:
    Func func_;
    bool active_ = true;
};

template <typename Func> ScopeGuard<Func> scope_exit(Func func) {
    return ScopeGuard<Func>(std::move(func));
}

} // namespace yams::metadata::repository

// ============================================================================
// Convenience aliases in yams namespace
// ============================================================================

namespace yams {

using metadata::repository::and_then;
using metadata::repository::from_optional;
using metadata::repository::or_else;
using metadata::repository::scope_exit;
using metadata::repository::to_optional;
using metadata::repository::transform;
using metadata::repository::value_or;

} // namespace yams
