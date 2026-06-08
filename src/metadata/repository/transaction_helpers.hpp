// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <chrono>
#include <string>

#include <yams/metadata/database.h>
#include <yams/storage/sqlite_retry.h>

namespace yams::metadata::repository {

// Transaction begin helper with backend-appropriate semantics.
// - libsql (MVCC): Uses regular BEGIN since concurrent writers are supported.
// - SQLite: Uses BEGIN IMMEDIATE to reserve the single-writer slot eagerly.
inline Result<void> beginTransaction(Database& db) {
#if YAMS_LIBSQL_BACKEND
    return db.execute("BEGIN");
#else
    return db.execute("BEGIN IMMEDIATE");
#endif
}

// Transaction begin helper with backend-appropriate semantics.
// - libsql (MVCC): Uses regular BEGIN since concurrent writers are supported.
// - SQLite: Uses BEGIN IMMEDIATE with retry/backoff for lock contention.
inline Result<void>
beginTransactionWithRetry(Database& db, int maxRetries = 5,
                          std::chrono::milliseconds initialBackoff = std::chrono::milliseconds(5)) {
#if YAMS_LIBSQL_BACKEND
    // libsql supports MVCC - concurrent writers don't block each other.
    // Use regular BEGIN (deferred) for better concurrency.
    (void)maxRetries;     // unused in libsql mode
    (void)initialBackoff; // unused in libsql mode
    return db.execute("BEGIN");
#else
    // Standard SQLite: single-writer model. BEGIN IMMEDIATE acquires write lock
    // immediately but fails fast when another writer holds a lock.
    // Retry with exponential backoff to handle transient lock contention.
    const storage::sqlite_retry::BusyRetryPolicy retryPolicy{maxRetries, initialBackoff};
    auto backoff = retryPolicy.initialBackoff;
    for (int attempt = 0; attempt < retryPolicy.maxRetries; ++attempt) {
        auto result = db.execute("BEGIN IMMEDIATE");
        if (result) {
            return result;
        }
        // Check if it's a lock error (worth retrying).
        const auto& errMsg = result.error().message;
        if (!storage::sqlite_retry::isBusyOrLockedMessage(errMsg)) {
            // Not a lock error, don't retry.
            return Error{result.error().code, result.error().message};
        }
        if (attempt + 1 < retryPolicy.maxRetries) {
            storage::sqlite_retry::sleepAndBackoff(backoff);
        }
    }
    return Error{ErrorCode::DatabaseError, "BEGIN IMMEDIATE failed after retries: database locked"};
#endif
}

inline void rollbackIgnoringErrors(Database& db) {
    (void)db.execute("ROLLBACK");
}

inline Result<void> commitOrRollback(Database& db) {
    auto commitResult = db.execute("COMMIT");
    if (!commitResult) {
        rollbackIgnoringErrors(db);
        return commitResult.error();
    }
    return Result<void>();
}

} // namespace yams::metadata::repository
