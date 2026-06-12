#pragma once

#include <cstdio>
#include <cstdlib>
#include <string_view>

#ifndef YAMS_CORE_HAS_STACKTRACE
#define YAMS_CORE_HAS_STACKTRACE 0
#endif

#if YAMS_CORE_HAS_STACKTRACE
#include <stacktrace>
#endif

namespace yams::core::detail {

struct SourceLocation {
    const char* fileName{"(unknown)"};
    unsigned lineNumber{0};
    const char* functionName{"(unknown)"};

    [[nodiscard]] static constexpr SourceLocation current(
        const char* file = "(unknown)", unsigned line = 0,
        const char* function = "(unknown)") noexcept {
        return SourceLocation{file, line, function};
    }
};

#define YAMS_SOURCE_LOCATION_CURRENT()                                                            \
    ::yams::core::detail::SourceLocation::current(__FILE__, __LINE__, __func__)

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

[[noreturn]] inline void reportAssertionFailure(
    AssertionKind kind, const char* expression, std::string_view message,
    SourceLocation location = SourceLocation::current()) noexcept {
    std::fprintf(stderr, "[YAMS] %s failed\n", assertionKindName(kind));
    std::fprintf(stderr, "  file: %s:%u\n", location.fileName, location.lineNumber);
    std::fprintf(stderr, "  function: %s\n", location.functionName);
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
                YAMS_SOURCE_LOCATION_CURRENT());                                                   \
        }                                                                                          \
    } while (false)

#define YAMS_PRECONDITION(condition, message)                                                      \
    do {                                                                                           \
        if (!(condition)) [[unlikely]] {                                                           \
            ::yams::core::detail::reportAssertionFailure(                                          \
                ::yams::core::detail::AssertionKind::Precondition, #condition, (message),          \
                YAMS_SOURCE_LOCATION_CURRENT());                                                   \
        }                                                                                          \
    } while (false)

#define YAMS_POSTCONDITION(condition, message)                                                     \
    do {                                                                                           \
        if (!(condition)) [[unlikely]] {                                                           \
            ::yams::core::detail::reportAssertionFailure(                                          \
                ::yams::core::detail::AssertionKind::Postcondition, #condition, (message),         \
                YAMS_SOURCE_LOCATION_CURRENT());                                                   \
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
                YAMS_SOURCE_LOCATION_CURRENT());                                                   \
        }                                                                                          \
    } while (false)
#else
#define YAMS_DCHECK(condition, message) ((void)sizeof(condition), (void)0)
#endif

#define YAMS_UNREACHABLE(message)                                                                  \
    do {                                                                                           \
        ::yams::core::detail::reportAssertionFailure(                                              \
            ::yams::core::detail::AssertionKind::Unreachable, nullptr, (message),                  \
            YAMS_SOURCE_LOCATION_CURRENT());                                                       \
    } while (false)
