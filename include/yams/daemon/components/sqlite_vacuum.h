#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>

namespace yams::daemon {

/// When a full VACUUM of a SQLite file is worth its cost. Shared by the metadata database
/// (yams.db) startup vacuum and the vector database (vectors.db) maintenance vacuum so both
/// follow the same size and reclaimable-space rules.
struct SqliteVacuumPolicy {
    std::uint64_t minDatabaseBytes{512ULL * 1024 * 1024};
    std::uint64_t minReclaimableBytes{128ULL * 1024 * 1024};
    double minReclaimableRatio{0.10};
};

/// True when the file is large enough and enough of it is free pages (or trailing bytes past
/// the logical end) that rewriting it reclaims a meaningful amount of disk.
[[nodiscard]] bool shouldVacuumSqlite(std::uint64_t databaseBytes, std::uint64_t pageCount,
                                      std::uint64_t freePageCount, std::uint64_t pageSize,
                                      const SqliteVacuumPolicy& policy = {});

/// VACUUM rewrites the live pages into a temporary database and then writes them back
/// through the WAL, so the data directory must have more free space than the current file.
[[nodiscard]] constexpr bool hasSpaceForSqliteVacuum(std::uint64_t databaseBytes,
                                                     std::uint64_t availableBytes) noexcept {
    return availableBytes > databaseBytes;
}

enum class SqliteVacuumStatus {
    NotNeeded,
    InsufficientSpace,
    OpenFailed,
    Busy,
    Interrupted,
    Vacuumed,
};

[[nodiscard]] std::string_view sqliteVacuumStatusName(SqliteVacuumStatus status) noexcept;

struct SqliteVacuumOutcome {
    SqliteVacuumStatus status{SqliteVacuumStatus::NotNeeded};
    std::uint64_t bytesBefore{0};
    std::uint64_t bytesAfter{0};
    std::uint64_t reclaimableBytes{0};
    std::uint64_t availableBytes{0};
    std::string detail;
};

/// Measure `dbPath` on its own connection and VACUUM it when `policy` says it is useful and
/// the data directory and temp directory have room for the rewrite.
///
/// The connection uses file-backed temp storage (never RAM) and a short busy timeout, so a
/// live writer holding the database makes this return Busy instead of waiting. `shouldAbort`
/// is polled while VACUUM runs; returning true rolls the VACUUM back (status Interrupted) so a
/// writer that shows up mid-run is not starved. Readers on other connections are unaffected
/// in WAL mode. The file is never created when missing.
[[nodiscard]] SqliteVacuumOutcome
vacuumSqliteFileIfUseful(const std::filesystem::path& dbPath, const SqliteVacuumPolicy& policy,
                         const std::function<bool()>& shouldAbort);

} // namespace yams::daemon
