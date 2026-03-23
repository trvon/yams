// Unified formatting compatibility header.
// Chooses std::format when reliably available, otherwise falls back to {fmt}.
// Exposes both yams::format(...) and yams::fmt_format(...) for compatibility.

#pragma once

#include <string>
#include <utility>

// Detect std::format availability robustly (toolchain + standard library)
// Only enable when the standard library declares support via feature-test macro.
// This avoids false-positives on platforms like Ubuntu 22.04 (GCC 11) where
// <format> is incomplete or unavailable.
#ifndef YAMS_STD_FORMAT_AVAILABLE
#if defined(YAMS_HAS_STD_FORMAT)
#define YAMS_STD_FORMAT_AVAILABLE YAMS_HAS_STD_FORMAT
#elif defined(__cpp_lib_format) && (__cpp_lib_format >= 201907)
#define YAMS_STD_FORMAT_AVAILABLE 1
#else
#define YAMS_STD_FORMAT_AVAILABLE 0
#endif
#endif

#if YAMS_STD_FORMAT_AVAILABLE
#include <chrono>
#include <format>
namespace yams {
using std::format_args;
using std::format_error;
using std::format_to;
using std::format_to_n;
using std::formatted_size;
using std::make_format_args;
using std::vformat;
using std::vformat_to;

template <typename... Args>
inline std::string format(std::format_string<Args...> fmt, Args&&... args) {
    return std::format(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline std::string fmt_format(std::format_string<Args...> fmt, Args&&... args) {
    return std::format(fmt, std::forward<Args>(args)...);
}
} // namespace yams
#define YAMS_FORMAT_NAMESPACE std
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
using fmt::format_args;
using fmt::format_error;
using fmt::format_to;
using fmt::format_to_n;
using fmt::formatted_size;
using fmt::make_format_args;
using fmt::vformat;
using fmt::vformat_to;

template <typename... Args>
inline std::string format(fmt::format_string<Args...> fmt, Args&&... args) {
    return fmt::format(fmt, std::forward<Args>(args)...);
}

template <typename... Args>
inline std::string fmt_format(fmt::format_string<Args...> fmt, Args&&... args) {
    return fmt::format(fmt, std::forward<Args>(args)...);
}
} // namespace yams
#define YAMS_FORMAT_NAMESPACE fmt
#endif
