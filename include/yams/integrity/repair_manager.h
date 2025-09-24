#pragma once

#include <yams/core/types.h>
#include <yams/storage/storage_engine.h>

#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace yams::integrity {

enum class RepairStrategy { FromBackup, FromP2P, FromParity, FromManifest };

struct RepairManagerConfig {
    std::function<Result<std::vector<std::byte>>(const std::string&)> backupFetcher;
    std::function<Result<std::vector<std::byte>>(const std::string&)> p2pFetcher;
    std::function<Result<std::vector<std::byte>>(const std::string&)> parityReconstructor;
    std::function<Result<std::vector<std::byte>>(const std::string&)> manifestReconstructor;
    std::vector<RepairStrategy> defaultOrder = {RepairStrategy::FromBackup, RepairStrategy::FromP2P,
                                                RepairStrategy::FromParity,
                                                RepairStrategy::FromManifest};
};

class RepairManager {
public:
    RepairManager(storage::IStorageEngine& storage, RepairManagerConfig config = {});

    RepairManager(const RepairManager&) = delete;
    RepairManager& operator=(const RepairManager&) = delete;

    [[nodiscard]] bool attemptRepair(const std::string& blockHash,
                                     const std::vector<RepairStrategy>& order = {});

    [[nodiscard]] bool canRepair(const std::string& blockHash) const;

private:
    [[nodiscard]] bool storeIfValid(const std::string& blockHash,
                                    const Result<std::vector<std::byte>>& fetchResult) const;

private:
    storage::IStorageEngine& storage_;
    RepairManagerConfig config_;
};

std::shared_ptr<RepairManager> makeRepairManager(storage::IStorageEngine& storage,
                                                 RepairManagerConfig config = {});

} // namespace yams::integrity
