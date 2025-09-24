#include <yams/wal/wal_recovery.h>

#include <spdlog/spdlog.h>

#include <filesystem>
#include <utility>

namespace yams::wal {
namespace {
[[nodiscard]] std::filesystem::path resolveWalDirectory(const RecoveryOptions& options) {
    if (!options.walDirectory.empty()) {
        return options.walDirectory;
    }
    return options.managerConfig.walDirectory;
}
} // namespace

Result<WALManager::RecoveryStats> recoverFromLogs(const RecoveryOptions& options,
                                                  RecoveryApplyFunction applyEntry) {
    if (!applyEntry) {
        return Result<WALManager::RecoveryStats>(
            Error{ErrorCode::InvalidArgument, "WAL recovery requires a valid apply callback"});
    }

    auto walDir = resolveWalDirectory(options);
    if (walDir.empty()) {
        return Result<WALManager::RecoveryStats>(
            Error{ErrorCode::InvalidArgument, "WAL directory not specified"});
    }

    std::error_code fsError;
    if (!std::filesystem::exists(walDir, fsError)) {
        if (options.createDirectoryIfMissing) {
            if (!std::filesystem::create_directories(walDir, fsError) && fsError) {
                return Result<WALManager::RecoveryStats>(
                    Error{ErrorCode::PermissionDenied,
                          "Unable to create WAL directory: " + walDir.string()});
            }
        } else {
            return Result<WALManager::RecoveryStats>(
                Error{ErrorCode::FileNotFound, "WAL directory does not exist: " + walDir.string()});
        }
    } else if (!std::filesystem::is_directory(walDir, fsError)) {
        return Result<WALManager::RecoveryStats>(
            Error{ErrorCode::InvalidPath, "WAL path is not a directory: " + walDir.string()});
    }

    WALManager::Config config = options.managerConfig;
    config.walDirectory = std::move(walDir);

    WALManager manager(config);

    spdlog::info("Starting WAL recovery from {}", config.walDirectory.string());
    auto statsResult = manager.recover(std::move(applyEntry));
    if (!statsResult) {
        spdlog::error("WAL recovery failed: {}", statsResult.error().message);
        return Result<WALManager::RecoveryStats>(statsResult.error());
    }

    const auto& stats = statsResult.value();
    spdlog::info("WAL recovery completed — entries: {}, transactions: {}, rolled back: {}",
                 stats.entriesProcessed, stats.transactionsRecovered, stats.transactionsRolledBack);

    return statsResult;
}

} // namespace yams::wal
