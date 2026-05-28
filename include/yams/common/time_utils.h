#pragma once

#include <chrono>
#include <cstdint>

namespace yams::common {

/**
 * Convert a system_clock time point to whole seconds since the system_clock epoch.
 *
 * YAMS persists these values as Unix-style epoch seconds in metadata and vector-store schemas.
 * Sub-second precision is intentionally truncated toward zero by duration_cast, matching the
 * existing persisted format.
 */
inline std::int64_t
timePointToEpochSeconds(const std::chrono::system_clock::time_point& timePoint) noexcept {
    return std::chrono::duration_cast<std::chrono::seconds>(timePoint.time_since_epoch()).count();
}

/** Convert persisted epoch seconds back to a system_clock time point. */
inline std::chrono::system_clock::time_point
epochSecondsToTimePoint(std::int64_t epochSeconds) noexcept {
    return std::chrono::system_clock::time_point{std::chrono::seconds{epochSeconds}};
}

/** Return the current system_clock time as persisted epoch seconds. */
inline std::int64_t nowEpochSeconds() noexcept {
    return timePointToEpochSeconds(std::chrono::system_clock::now());
}

} // namespace yams::common
