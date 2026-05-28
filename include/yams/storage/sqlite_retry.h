#pragma once

#include <chrono>
#include <string_view>
#include <thread>

#include <sqlite3.h>

namespace yams::storage::sqlite_retry {

struct BusyRetryPolicy {
    int maxRetries;
    std::chrono::milliseconds initialBackoff;
};

inline constexpr BusyRetryPolicy metadataStatementPolicy() {
#if YAMS_LIBSQL_BACKEND
    return {2, std::chrono::milliseconds{5}};
#else
    return {3, std::chrono::milliseconds{10}};
#endif
}

inline constexpr BusyRetryPolicy vectorWritePolicy() {
#if YAMS_LIBSQL_BACKEND
    return {3, std::chrono::milliseconds{5}};
#else
    return {5, std::chrono::milliseconds{10}};
#endif
}

inline bool isBusyOrLocked(int rc) noexcept {
    return rc == SQLITE_BUSY || rc == SQLITE_LOCKED;
}

inline bool isBusyOrLockedMessage(std::string_view message) noexcept {
    return message.find("locked") != std::string_view::npos ||
           message.find("busy") != std::string_view::npos;
}

inline bool canRetry(int rc, int attempt, BusyRetryPolicy policy) noexcept {
    return isBusyOrLocked(rc) && attempt + 1 < policy.maxRetries;
}

inline void sleepAndBackoff(std::chrono::milliseconds& backoff) {
    std::this_thread::sleep_for(backoff);
    backoff *= 2;
}

} // namespace yams::storage::sqlite_retry
