#pragma once

#include <yams/core/types.h>

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

} // namespace yams::daemon
