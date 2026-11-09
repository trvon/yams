#include <yams/crypto/hasher.h>
#include <yams/integrity/block_repairer.h>

#include <spdlog/spdlog.h>

#include <span>

namespace yams::integrity {

BlockRepairer::BlockRepairer(storage::IStorageEngine& storage, BlockRepairerConfig config)
    : storage_(&storage), config_(std::move(config)) {}

bool BlockRepairer::attemptRepair(const std::string& blockHash,
                                  const std::vector<RepairStrategy>& order) {
    const auto& strategies = order.empty() ? config_.defaultOrder : order;
    for (auto strategy : strategies) {
        switch (strategy) {
            case RepairStrategy::FromBackup: {
                if (!config_.backupFetcher)
                    continue;
                auto data = config_.backupFetcher(blockHash);
                if (storeIfValid(blockHash, data)) {
                    spdlog::info("Repair succeeded for {} using backup", blockHash.substr(0, 8));
                    return true;
                }
                break;
            }
            case RepairStrategy::FromP2P: {
                if (!config_.p2pFetcher)
                    continue;
                auto data = config_.p2pFetcher(blockHash);
                if (storeIfValid(blockHash, data)) {
                    spdlog::info("Repair succeeded for {} via P2P", blockHash.substr(0, 8));
                    return true;
                }
                break;
            }
            case RepairStrategy::FromParity: {
                if (!config_.parityReconstructor)
                    continue;
                auto data = config_.parityReconstructor(blockHash);
                if (storeIfValid(blockHash, data)) {
                    spdlog::info("Repair succeeded for {} via parity reconstruction",
                                 blockHash.substr(0, 8));
                    return true;
                }
                break;
            }
            case RepairStrategy::FromManifest: {
                if (!config_.manifestReconstructor)
                    continue;
                auto data = config_.manifestReconstructor(blockHash);
                if (storeIfValid(blockHash, data)) {
                    spdlog::info("Repair succeeded for {} via manifest reconstruction",
                                 blockHash.substr(0, 8));
                    return true;
                }
                break;
            }
        }
    }

    spdlog::warn("Repair attempts failed for block {}", blockHash.substr(0, 8));
    return false;
}

bool BlockRepairer::canRepair(const std::string& blockHash) const {
    (void)blockHash;
    return config_.backupFetcher || config_.p2pFetcher || config_.parityReconstructor ||
           config_.manifestReconstructor;
}

bool BlockRepairer::storeIfValid(const std::string& blockHash,
                                 const Result<std::vector<std::byte>>& fetchResult) const {
    if (!storage_) {
        spdlog::error("Storage engine not available for repair");
        return false;
    }

    if (!fetchResult.has_value()) {
        spdlog::warn("Repair fetch failed for {}: {}", blockHash.substr(0, 8),
                     fetchResult.error().message);
        return false;
    }

    auto hasher = yams::crypto::createSHA256Hasher();
    hasher->init();
    hasher->update(
        std::span<const std::byte>(fetchResult.value().data(), fetchResult.value().size()));
    auto computed = hasher->finalize();
    if (computed != blockHash) {
        spdlog::warn("Repaired data hash mismatch for {} (computed {})", blockHash.substr(0, 8),
                     computed.substr(0, 8));
        return false;
    }

    auto storeRes =
        storage_->store(blockHash, std::span<const std::byte>(fetchResult.value().data(),
                                                              fetchResult.value().size()));
    if (!storeRes.has_value()) {
        spdlog::error("Failed to store repaired block {}: {}", blockHash.substr(0, 8),
                      storeRes.error().message);
        return false;
    }
    return true;
}

std::shared_ptr<BlockRepairer> makeBlockRepairer(storage::IStorageEngine& storage,
                                                 BlockRepairerConfig config) {
    return std::make_shared<BlockRepairer>(storage, std::move(config));
}

} // namespace yams::integrity
