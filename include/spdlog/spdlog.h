#pragma once

// Defer to the next available spdlog header in the include search path.
// Rely on upstream configuration (e.g., Conan) to decide whether to use
// spdlog's bundled {fmt} or an external fmt library.
#include_next <spdlog/spdlog.h>
