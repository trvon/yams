#pragma once

// Compatibility header for std::format
// Uses std::format when available, falls back to fmt library

#if YAMS_HAS_STD_FORMAT
#include <format>
namespace yams {
using std::format;
using std::format_args;
using std::format_error;
using std::format_to;
using std::format_to_n;
using std::formatted_size;
using std::make_format_args;
using std::vformat;
using std::vformat_to;
} // namespace yams
#else
// C++17/pre-C++20 fallback using fmt library
#include <spdlog/fmt/fmt.h>

namespace yams {
// Map fmt functions to std::format interface
using fmt::format;
using fmt::format_args;
using fmt::format_error;
using fmt::format_to;
using fmt::format_to_n;
using fmt::formatted_size;
using fmt::make_format_args;
using fmt::vformat;
using fmt::vformat_to;
} // namespace yams
#endif

// Additional convenience macros for conditional compilation
#if YAMS_HAS_STD_FORMAT
#define YAMS_FORMAT_NAMESPACE std
#else
#define YAMS_FORMAT_NAMESPACE fmt
#endif