#pragma once

#include <yams/core/types.h>

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yams::daemon {

struct DbRecoveryResult {
    std::filesystem::path quarantinedPath;
    std::filesystem::path sentinelPath;
    std::string timestamp;
};

Result<DbRecoveryResult> quarantineAndRecreate(const std::filesystem::path& dbPath);

struct DbRecoverySentinel {
    std::filesystem::path quarantinedPath;
    std::string timestamp;
};

std::optional<DbRecoverySentinel> readLatestRecoverySentinel(const std::filesystem::path& dbPath);

/// Infix of a SQLite sidecar that was moved aside because it could not be checkpointed.
inline constexpr std::string_view kSqliteSidecarQuarantineMarker = ".quarantine-";

/**
 * Move SQLite WAL/SHM sidecars of `dbPath` aside instead of deleting them.
 *
 * A WAL that SQLite refuses to checkpoint ("malformed"/"corrupt") may still hold committed but
 * uncheckpointed transactions. Removing it loses that data with no copy, so the sidecars are
 * renamed next to the database as `<db>-wal.quarantine-<UTC timestamp>` (and `-shm` likewise),
 * never overwriting an earlier quarantine. SQLite no longer sees them as sidecars, so the database
 * continues from its last checkpoint while the evidence is preserved for manual recovery.
 *
 * Returns the quarantined paths (empty when no sidecar existed). The WAL is moved first; if it
 * cannot be moved the SHM is left untouched and an error is returned.
 */
Result<std::vector<std::filesystem::path>>
quarantineSqliteSidecars(const std::filesystem::path& dbPath);

} // namespace yams::daemon
