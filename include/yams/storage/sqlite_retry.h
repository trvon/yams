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

struct QueryRetryPolicy {
    int maxRetries;
    int baseDelayMs;
    int maxDelayMs;
};

inline constexpr BusyRetryPolicy metadataStatementPolicy() {
#if YAMS_LIBSQL_BACKEND
    return {2, std::chrono::milliseconds{5}};
#else
    return {3, std::chrono::milliseconds{10}};
#endif
}

inline constexpr QueryRetryPolicy metadataRepositoryQueryRetryPolicy(std::string_view route,
                                                                     std::string_view opTag) {
    if (route == "read") {
        return {3, 25, 500};
    }

    // Atomic document metadata updates are user-facing app writes: once the competing writer
    // clears, a few faster retry windows beat the default long sleeps and keep p95 lower.
    if (opTag.find("document_update") != std::string_view::npos) {
        return {7, 10, 200};
    }

    // Download metadata fan-out is already best-effort at the service layer: a lock should not
    // hold the whole request hostage when the download and content ingest already succeeded.
    if (opTag.find("download_metadata") != std::string_view::npos) {
        return {2, 5, 10};
    }

    return {5, 25, 500};
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
