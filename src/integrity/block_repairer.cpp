#include <yams/crypto/hasher.h>
#include <yams/integrity/block_repairer.h>

#include <spdlog/spdlog.h>

#include <span>

namespace yams::integrity {

namespace {

std::string sha256Of(std::span<const std::byte> bytes) {
    auto hasher = yams::crypto::createSHA256Hasher();
    hasher->init();
    hasher->update(bytes);
    return hasher->finalize();
}

} // namespace

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

bool BlockRepairer::hasRepairSources() const {
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

    const std::span<const std::byte> bytes(fetchResult.value().data(), fetchResult.value().size());
    auto computed = sha256Of(bytes);
    if (computed != blockHash) {
        spdlog::warn("Repaired data hash mismatch for {} (computed {})", blockHash.substr(0, 8),
                     computed.substr(0, 8));
        return false;
    }

    // store() keeps an object that already exists (content addressing assumes the bytes match
    // the key), so a corrupt copy has to be removed before the verified bytes can replace it.
    auto exists = storage_->exists(blockHash);
    if (exists && exists.value()) {
        auto current = storage_->retrieve(blockHash);
        if (current && sha256Of(current.value()) == blockHash) {
            return true;
        }
        auto removed = storage_->remove(blockHash);
        if (!removed && removed.error().code != ErrorCode::ChunkNotFound) {
            spdlog::error("Failed to remove corrupt block {} before repair: {}",
                          blockHash.substr(0, 8), removed.error().message);
            return false;
        }
    }

    auto storeRes = storage_->store(blockHash, bytes);
    if (!storeRes.has_value()) {
        spdlog::error("Failed to store repaired block {}: {}", blockHash.substr(0, 8),
                      storeRes.error().message);
        return false;
    }

    auto stored = storage_->retrieve(blockHash);
    if (!stored || sha256Of(stored.value()) != blockHash) {
        spdlog::error("Repaired block {} does not read back with its hash", blockHash.substr(0, 8));
        return false;
    }
    return true;
}

std::shared_ptr<BlockRepairer> makeBlockRepairer(storage::IStorageEngine& storage,
                                                 BlockRepairerConfig config) {
    return std::make_shared<BlockRepairer>(storage, std::move(config));
}

} // namespace yams::integrity
