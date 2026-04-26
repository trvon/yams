#pragma once

#include <yams/core/types.h>

#include <filesystem>
#include <optional>
#include <string>

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

} // namespace yams::daemon
