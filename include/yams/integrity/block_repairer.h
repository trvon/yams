#pragma once

#include <yams/core/types.h>
#include <yams/storage/storage_engine.h>

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace yams::integrity {

enum class RepairStrategy { FromBackup, FromP2P, FromParity, FromManifest };

struct BlockRepairerConfig {
    std::function<Result<std::vector<std::byte>>(const std::string&)> backupFetcher;
    std::function<Result<std::vector<std::byte>>(const std::string&)> p2pFetcher;
    std::function<Result<std::vector<std::byte>>(const std::string&)> parityReconstructor;
    std::function<Result<std::vector<std::byte>>(const std::string&)> manifestReconstructor;
    std::vector<RepairStrategy> defaultOrder = {RepairStrategy::FromBackup, RepairStrategy::FromP2P,
                                                RepairStrategy::FromParity,
                                                RepairStrategy::FromManifest};
};

/// Restores a missing or corrupt block from the configured sources (backup, P2P, parity,
/// manifest), storing it only when the recovered bytes hash to the requested block hash.
class BlockRepairer {
public:
    BlockRepairer(storage::IStorageEngine& storage, BlockRepairerConfig config = {});

    BlockRepairer(const BlockRepairer&) = delete;
    BlockRepairer& operator=(const BlockRepairer&) = delete;

    // Repair operations
    [[nodiscard]] bool attemptRepair(const std::string& blockHash,
                                     const std::vector<RepairStrategy>& order = {});

    [[nodiscard]] bool canRepair(const std::string& blockHash) const;

private:
    [[nodiscard]] bool storeIfValid(const std::string& blockHash,
                                    const Result<std::vector<std::byte>>& fetchResult) const;

    storage::IStorageEngine* storage_{nullptr};
    BlockRepairerConfig config_;
};

std::shared_ptr<BlockRepairer> makeBlockRepairer(storage::IStorageEngine& storage,
                                                 BlockRepairerConfig config = {});

} // namespace yams::integrity
