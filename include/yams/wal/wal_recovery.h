#pragma once

#include <filesystem>
#include <functional>

#include <yams/core/types.h>
#include <yams/wal/wal_manager.h>

namespace yams::wal {

/**
 * Configuration options for high-level WAL recovery utilities.
 */
struct RecoveryOptions {
    std::filesystem::path walDirectory;    ///< Directory containing WAL log files
    bool createDirectoryIfMissing = false; ///< Create directory if it does not exist
    WALManager::Config managerConfig{};    ///< Additional WAL manager configuration
};

using RecoveryApplyFunction = WALManager::ApplyFunction;

[[nodiscard]] Result<WALManager::RecoveryStats> recoverFromLogs(const RecoveryOptions& options,
                                                                RecoveryApplyFunction applyEntry);

} // namespace yams::wal
