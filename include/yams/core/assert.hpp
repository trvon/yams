#pragma once

#include <cstdio>
#include <cstdlib>
#include <source_location>
#include <string_view>

#if defined(__has_include)
#if __has_include(<stacktrace>)
#include <stacktrace>
#if defined(__cpp_lib_stacktrace) && __cpp_lib_stacktrace >= 202011L
#define YAMS_CORE_HAS_STACKTRACE 1
#endif
#endif
#endif

#ifndef YAMS_CORE_HAS_STACKTRACE
#define YAMS_CORE_HAS_STACKTRACE 0
#endif

namespace yams::core::detail {

enum class AssertionKind {
    Assert,
    DebugCheck,
    Precondition,
    Postcondition,
    Unreachable,
};

#if defined(YAMS_DISABLE_DCHECK)
inline constexpr bool kDcheckEnabled = false;
#elif defined(YAMS_ENABLE_DCHECK)
inline constexpr bool kDcheckEnabled = true;
#elif defined(NDEBUG)
inline constexpr bool kDcheckEnabled = false;
#else
inline constexpr bool kDcheckEnabled = true;
#endif

[[nodiscard]] inline constexpr const char* assertionKindName(AssertionKind kind) noexcept {
    switch (kind) {
        case AssertionKind::Assert:
            return "assert";
        case AssertionKind::DebugCheck:
            return "dcheck";
        case AssertionKind::Precondition:
            return "precondition";
        case AssertionKind::Postcondition:
            return "postcondition";
        case AssertionKind::Unreachable:
            return "unreachable";
    }
    return "assertion";
}

inline void printStacktrace() noexcept {
#if YAMS_CORE_HAS_STACKTRACE
    try {
        const auto trace = std::stacktrace::current();
        if (trace.empty()) {
            return;
        }

        std::fputs("[YAMS] stacktrace:\n", stderr);
        for (std::size_t i = 0; i < trace.size(); ++i) {
            const auto description = trace[i].description();
            std::fprintf(stderr, "  [%zu] %s\n", i,
                         description.empty() ? "(unknown)" : description.c_str());
        }
    } catch (...) {
        // Best-effort diagnostic only.
    }
#endif
}

[[noreturn]] inline void
reportAssertionFailure(AssertionKind kind, const char* expression, std::string_view message,
                       std::source_location location = std::source_location::current()) noexcept {
    std::fprintf(stderr, "[YAMS] %s failed\n", assertionKindName(kind));
    std::fprintf(stderr, "  file: %s:%u\n", location.file_name(), location.line());
    std::fprintf(stderr, "  function: %s\n", location.function_name());
    if (expression != nullptr && expression[0] != '\0') {
        std::fprintf(stderr, "  expression: %s\n", expression);
    }
    if (!message.empty()) {
        std::fprintf(stderr, "  message: %.*s\n", static_cast<int>(message.size()), message.data());
    }
    printStacktrace();
    std::fflush(stderr);
    std::abort();
}

} // namespace yams::core::detail

#define YAMS_ASSERT(condition, message)                                                            \
    do {                                                                                           \
        if (!(condition)) [[unlikely]] {                                                           \
            ::yams::core::detail::reportAssertionFailure(                                          \
                ::yams::core::detail::AssertionKind::Assert, #condition, (message),                \
                std::source_location::current());                                                  \
        }                                                                                          \
    } while (false)

#define YAMS_PRECONDITION(condition, message)                                                      \
    do {                                                                                           \
        if (!(condition)) [[unlikely]] {                                                           \
            ::yams::core::detail::reportAssertionFailure(                                          \
                ::yams::core::detail::AssertionKind::Precondition, #condition, (message),          \
                std::source_location::current());                                                  \
        }                                                                                          \
    } while (false)

#define YAMS_POSTCONDITION(condition, message)                                                     \
    do {                                                                                           \
        if (!(condition)) [[unlikely]] {                                                           \
            ::yams::core::detail::reportAssertionFailure(                                          \
                ::yams::core::detail::AssertionKind::Postcondition, #condition, (message),         \
                std::source_location::current());                                                  \
        }                                                                                          \
    } while (false)

#if defined(YAMS_DISABLE_DCHECK)
#define YAMS_DCHECK(condition, message) ((void)sizeof(condition), (void)0)
#elif defined(YAMS_ENABLE_DCHECK) || !defined(NDEBUG)
#define YAMS_DCHECK(condition, message)                                                            \
    do {                                                                                           \
        if (!(condition)) [[unlikely]] {                                                           \
            ::yams::core::detail::reportAssertionFailure(                                          \
                ::yams::core::detail::AssertionKind::DebugCheck, #condition, (message),            \
                std::source_location::current());                                                  \
        }                                                                                          \
    } while (false)
#else
#define YAMS_DCHECK(condition, message) ((void)sizeof(condition), (void)0)
#endif

#define YAMS_UNREACHABLE(message)                                                                  \
    do {                                                                                           \
        ::yams::core::detail::reportAssertionFailure(                                              \
            ::yams::core::detail::AssertionKind::Unreachable, nullptr, (message),                  \
            std::source_location::current());                                                      \
    } while (false)
