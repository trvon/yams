#pragma once

#include <algorithm>
#include <chrono>

namespace yams::daemon::shutdown_budget {

inline constexpr auto kWorkCoordinatorJoinTimeout = std::chrono::seconds(5);
inline constexpr auto kWorkCoordinatorExtendedJoinTimeout = std::chrono::seconds(30);
inline constexpr auto kGracefulShutdownLifecycleHeadroom = std::chrono::seconds(5);
inline constexpr auto kMinGracefulShutdownWaitTimeout = kWorkCoordinatorJoinTimeout +
                                                        kWorkCoordinatorExtendedJoinTimeout +
                                                        kGracefulShutdownLifecycleHeadroom;

inline std::chrono::milliseconds
clampGracefulShutdownWaitTimeout(std::chrono::milliseconds requested) {
    if (requested.count() <= 0) {
        return requested;
    }
    return std::max(requested, std::chrono::duration_cast<std::chrono::milliseconds>(
                                   kMinGracefulShutdownWaitTimeout));
}

} // namespace yams::daemon::shutdown_budget
