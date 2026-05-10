#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <filesystem>
#include <string>
#include <vector>

namespace yams::daemon {

struct DbSalvageResult {
    size_t documentsSalvaged{0};
    size_t documentsFailed{0};
    size_t documentsSkipped{0};
    std::vector<std::string> diagnostics;
};

Result<DbSalvageResult> salvageFromCorruptDb(const std::filesystem::path& corruptPath,
                                             const std::filesystem::path& freshPath);

} // namespace yams::daemon
