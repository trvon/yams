// Lightweight formatting compatibility header.
// Chooses std::format when reliably available, otherwise falls back to {fmt}.
// Provides a single helper: yams::fmt_format(fmt, args...)

#pragma once

#include <string>
#include <utility>

// Detect std::format availability robustly (toolchain + standard library)
// Only enable when the standard library declares support via feature-test macro.
// This avoids false-positives on platforms like Ubuntu 22.04 (GCC 11) where
// <format> is incomplete or unavailable.
#ifndef YAMS_STD_FORMAT_AVAILABLE
#if defined(__cpp_lib_format) && (__cpp_lib_format >= 201907)
#define YAMS_STD_FORMAT_AVAILABLE 1
#else
#define YAMS_STD_FORMAT_AVAILABLE 0
#endif
#endif

#if YAMS_STD_FORMAT_AVAILABLE
#include <chrono>
#include <format>
namespace yams {
// Constrain the first parameter to std::format_string to avoid calling the
// consteval constructor with non-constant expressions through a perfect-forwarding pack.
template <typename... Args>
inline std::string fmt_format(std::format_string<Args...> fmt, Args&&... args) {
    return std::format(fmt, std::forward<Args>(args)...);
}
} // namespace yams
#else
// Using {fmt}. If spdlog bundles fmt (SPDLOG_FMT_EXTERNAL=0), include from spdlog path
#if defined(SPDLOG_FMT_EXTERNAL)
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#else
#include <spdlog/fmt/bundled/chrono.h>
#include <spdlog/fmt/bundled/format.h>
#include <spdlog/fmt/bundled/ostream.h>
#endif
namespace yams {
// Mirror std::format_signature using fmt::format_string for compile-time checking when available.
template <typename... Args>
inline std::string fmt_format(fmt::format_string<Args...> fmt, Args&&... args) {
    return fmt::format(fmt, std::forward<Args>(args)...);
}
} // namespace yams
#endif
