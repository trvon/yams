#pragma once

#include "../../core/types.h"

#include <string>

namespace yams::daemon {

struct DbIntegrityStampDecision {
    bool trustedCleanShutdown{false};
    bool invalidationPersisted{false};
    bool sidecarPresent{false};
    std::string reason;
};

std::string dbIntegrityStampPath(const std::string& dbPath);

template <typename PathLike> std::string dbIntegrityStampPath(const PathLike& dbPath) {
    return dbIntegrityStampPath(dbPath.string());
}

/**
 * Remove SQLite sidecars after a successful truncate checkpoint. The WAL must be
 * empty; the fixed-size SHM coordination file may be removed only after every
 * database connection and writer has stopped. Non-empty WAL state fails closed.
 */
Result<void> clearDbCleanShutdownSidecars(const std::string& dbPath);

template <typename PathLike> Result<void> clearDbCleanShutdownSidecars(const PathLike& dbPath) {
    return clearDbCleanShutdownSidecars(dbPath.string());
}

/**
 * Publish proof that the database completed a clean, checkpointed shutdown.
 *
 * The database must be closed and have no SQLite WAL/SHM sidecars. Failure is
 * safe: the next startup performs the full integrity check.
 */
Result<void> publishDbCleanShutdownStamp(const std::string& dbPath);

template <typename PathLike> Result<void> publishDbCleanShutdownStamp(const PathLike& dbPath) {
    return publishDbCleanShutdownStamp(dbPath.string());
}

/**
 * Consume a prior clean-shutdown stamp and replace it with an in-progress
 * marker before startup proceeds.
 *
 * Missing, malformed, stale, or sidecar-contaminated state fails closed. The
 * returned decision is trusted only when both the prior stamp matched and the
 * in-progress marker was persisted successfully.
 */
DbIntegrityStampDecision consumeDbCleanShutdownStamp(const std::string& dbPath);

template <typename PathLike>
DbIntegrityStampDecision consumeDbCleanShutdownStamp(const PathLike& dbPath) {
    return consumeDbCleanShutdownStamp(dbPath.string());
}

/** Remove any stamp so the next startup must perform the full integrity check. */
Result<void> invalidateDbCleanShutdownStamp(const std::string& dbPath);

template <typename PathLike> Result<void> invalidateDbCleanShutdownStamp(const PathLike& dbPath) {
    return invalidateDbCleanShutdownStamp(dbPath.string());
}

} // namespace yams::daemon
