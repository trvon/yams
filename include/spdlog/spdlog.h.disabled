#pragma once

// Defer to the next available spdlog header in the include search path.
// Rely on upstream configuration (e.g., Conan) to decide whether to use
// spdlog's bundled {fmt} or an external fmt library.

// Suppress pedantic warning about #include_next being a GCC extension
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif

#include_next <spdlog/spdlog.h>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
