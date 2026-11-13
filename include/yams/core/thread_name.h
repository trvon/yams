// SPDX-License-Identifier: GPL-3.0-or-later
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string_view>

#if defined(__linux__) || defined(__APPLE__)
#include <pthread.h>
#endif

namespace yams {

/// Names the calling thread so `top -H`, `ps -L`, gdb and perf can tell pools apart. Linux keeps
/// at most 15 bytes and truncates the rest. Windows is a no-op here: SetThreadDescription needs
/// <windows.h>, which this header must not pull into every translation unit that profiles.
inline void setCurrentThreadName(std::string_view name) noexcept {
#if defined(__linux__)
    char buffer[16] = {};
    std::memcpy(buffer, name.data(), std::min<std::size_t>(name.size(), sizeof(buffer) - 1));
    (void)pthread_setname_np(pthread_self(), buffer);
#elif defined(__APPLE__)
    char buffer[64] = {};
    std::memcpy(buffer, name.data(), std::min<std::size_t>(name.size(), sizeof(buffer) - 1));
    (void)pthread_setname_np(buffer);
#else
    (void)name;
#endif
}

} // namespace yams
