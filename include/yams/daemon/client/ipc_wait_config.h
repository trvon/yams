#pragma once

namespace yams::daemon {

inline constexpr int kIpcWaitWarnMs = 250;

inline bool ipc_wait_trace_enabled() noexcept {
    return false;
}

inline int ipc_wait_warn_ms() noexcept {
    return kIpcWaitWarnMs;
}

} // namespace yams::daemon
