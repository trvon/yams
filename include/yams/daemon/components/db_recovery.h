#pragma once

#include <yams/core/types.h>

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yams::daemon {

/// File name of the metadata database inside the data directory.
inline constexpr std::string_view kMetadataDbFileName = "yams.db";

/// Infix of a quarantined database: `<db file name>.corrupt-<UTC timestamp>[.N]`.
inline constexpr std::string_view kCorruptDbMarker = ".corrupt-";

/// File-name prefix of corrupt-DB artifacts for `dbFileName` (e.g. "yams.db.corrupt-").
inline std::string corruptDbPrefix(std::string_view dbFileName = kMetadataDbFileName) {
    std::string prefix(dbFileName);
    prefix += kCorruptDbMarker;
    return prefix;
}

/// True for the main file of a corrupt-DB artifact of `dbFileName`, not its -wal/-shm companions.
inline bool isCorruptDbFileName(std::string_view name,
                                std::string_view dbFileName = kMetadataDbFileName) {
    const auto prefix = corruptDbPrefix(dbFileName);
    return name.size() > prefix.size() && name.starts_with(prefix) && !name.ends_with("-wal") &&
           !name.ends_with("-shm");
}

/// Main files of the corrupt-DB artifacts of `dbFileName` in `dataDir` (unordered).
std::vector<std::filesystem::path>
listCorruptDbs(const std::filesystem::path& dataDir,
               std::string_view dbFileName = kMetadataDbFileName);

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
