#pragma once

#include <algorithm>
#include <chrono>

namespace yams::daemon::shutdown_budget {

inline constexpr auto kWorkCoordinatorJoinTimeout = std::chrono::seconds(5);
inline constexpr auto kWorkCoordinatorExtendedJoinTimeout = std::chrono::seconds(30);
/// How long WorkCoordinator::stop() waits for every spawnDetached strand to run its posted
/// cancellation before it stops the io_context anyway. A strand busy in synchronous work cannot
/// run the handler; past this budget the idle workers are released and the busy one is named.
inline constexpr auto kDetachedCancelDrainBudget = std::chrono::milliseconds(500);
inline constexpr auto kGracefulShutdownLifecycleHeadroom = std::chrono::seconds(5);
inline constexpr auto kDefaultGracefulShutdownWaitTimeout = std::chrono::seconds(120);
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
