#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace yams::daemon {

struct DbSalvageResult {
    size_t documentsSalvaged{0};
    size_t documentsFailed{0};
    size_t documentsSkipped{0};
    std::vector<std::string> diagnostics;
};

using SalvageProgressFn = std::function<void(const std::string& phase, const std::string& message,
                                             uint64_t processed, uint64_t total)>;

Result<DbSalvageResult> salvageFromCorruptDb(const std::filesystem::path& corruptPath,
                                             const std::filesystem::path& freshPath,
                                             SalvageProgressFn progress = nullptr);

struct AggregateSalvageResult {
    DbSalvageResult combined;
    std::vector<std::filesystem::path> salvagedPaths;
};

AggregateSalvageResult salvageFromAllCorruptDbs(const std::filesystem::path& dataDir,
                                                const std::filesystem::path& freshPath,
                                                SalvageProgressFn progress = nullptr);

int64_t countDocumentsInDb(const std::filesystem::path& dbPath);

struct SalvageQuickCheck {
    int64_t currentDocCount{0};
    bool needsSalvage{false};
    int64_t maxCorruptCount{0};
    size_t corruptDbCount{0};
    size_t unreadableCorruptDbCount{0};
};

SalvageQuickCheck quickCheckSalvageNeeded(const std::filesystem::path& dataDir,
                                          const std::filesystem::path& dbPath);

struct RetainedCorruptDb {
    std::filesystem::path path;
    std::string reason;
};

struct CorruptDbCleanup {
    std::vector<std::filesystem::path> removed;
    std::vector<RetainedCorruptDb> retained;
    std::vector<std::string> errors;
};

struct RecoverySentinelCleanup {
    std::vector<std::filesystem::path> removed;
    std::vector<std::string> errors;
};

RecoverySentinelCleanup removeRecoverySentinels(const std::filesystem::path& dbPath);

/**
 * Evidence that salvage from `corruptPath` into `liveDbPath` is complete: true when every document
 * hash in the corrupt DB is present in the live DB. An error means the evidence could not be read
 * (unreadable corrupt DB, missing live DB, ...) and the corrupt DB must be kept.
 */
Result<bool> corruptDbSalvageConfirmed(const std::filesystem::path& corruptPath,
                                       const std::filesystem::path& liveDbPath);

/**
 * Remove corrupt-DB artifacts of `liveDbPath` (and their -wal/-shm) only when both
 *  - they were quarantined at least `minAge` ago (name timestamp, else mtime), and
 *  - corruptDbSalvageConfirmed() proves every document reached the live DB.
 * Everything else is kept and reported in `retained` with the reason.
 */
CorruptDbCleanup removeSalvagedCorruptDbs(
    const std::filesystem::path& liveDbPath, std::chrono::seconds minAge,
    std::chrono::system_clock::time_point now = std::chrono::system_clock::now());

} // namespace yams::daemon
