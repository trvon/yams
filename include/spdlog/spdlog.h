#pragma once

// Prefer spdlog bundled {fmt} on iOS to avoid dependency/version skew
// Ensure any externally defined SPDLOG_FMT_EXTERNAL is cleared
#ifdef SPDLOG_FMT_EXTERNAL
#undef SPDLOG_FMT_EXTERNAL
#endif

// Defer to the next available spdlog header in the include search path
#include_next <spdlog/spdlog.h>
