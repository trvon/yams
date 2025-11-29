#include <yams/core/magic_numbers.hpp>
#include <yams/crypto/hasher.h>
#include <yams/integrity/repair_manager.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <filesystem>
#include <ranges>
#include <span>

namespace yams::integrity {

RepairManager::RepairManager(storage::IStorageEngine& storage, RepairManagerConfig config)
    : storage_(&storage), config_(std::move(config)), repo_(nullptr) {}

RepairManager::RepairManager(storage::IStorageEngine& storage, metadata::MetadataRepository& repo,
                             RepairManagerConfig config)
    : storage_(&storage), config_(std::move(config)), repo_(&repo) {}

RepairManager::RepairManager(metadata::MetadataRepository& repo)
    : storage_(nullptr), config_(), repo_(&repo) {}

bool RepairManager::attemptRepair(const std::string& blockHash,
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

bool RepairManager::canRepair(const std::string& blockHash) const {
    (void)blockHash;
    return config_.backupFetcher || config_.p2pFetcher || config_.parityReconstructor ||
           config_.manifestReconstructor;
}

bool RepairManager::storeIfValid(const std::string& blockHash,
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

std::shared_ptr<RepairManager> makeRepairManager(storage::IStorageEngine& storage,
                                                 RepairManagerConfig config) {
    return std::make_shared<RepairManager>(storage, std::move(config));
}

// ============================================================================
// Prune Operations
// ============================================================================

std::vector<PruneCandidate>
RepairManager::queryCandidatesForPrune(metadata::MetadataRepository& repo,
                                       const PruneConfig& config) {
    std::vector<PruneCandidate> candidates;

    // Query all documents
    auto docsResult = repo.queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        spdlog::error("Failed to query documents: {}", docsResult.error().message);
        return candidates;
    }

    auto& allDocs = docsResult.value();
    spdlog::debug("Prune query: scanning {} documents", allDocs.size());

    // Filter documents (static helper functions)
    auto matchesCategory = [&](const metadata::DocumentInfo& doc) -> bool {
        if (config.categories.empty())
            return true;
        auto category = magic::getPruneCategory(doc.fileName, doc.fileExtension);
        return std::ranges::any_of(config.categories, [category](const auto& cat) {
            return magic::matchesPruneGroup(category, cat);
        });
    };

    auto meetsAgeCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        if (config.minAge.count() == 0)
            return true;
        auto now = std::chrono::system_clock::now();
        auto fileAge = std::chrono::duration_cast<std::chrono::seconds>(now - doc.modifiedTime);
        return fileAge >= config.minAge;
    };

    auto meetsSizeCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        return doc.fileSize >= config.minSize && doc.fileSize <= config.maxSize;
    };

    auto meetsExtensionCriteria = [&](const metadata::DocumentInfo& doc) -> bool {
        if (config.extensions.empty())
            return true;
        return std::ranges::any_of(config.extensions, [&](const auto& ext) {
            return doc.fileExtension == ext || doc.fileExtension == ("." + ext);
        });
    };

    // Filter and build candidates
    for (const auto& doc : allDocs) {
        if (!matchesCategory(doc))
            continue;
        if (!meetsAgeCriteria(doc))
            continue;
        if (!meetsSizeCriteria(doc))
            continue;
        if (!meetsExtensionCriteria(doc))
            continue;

        PruneCandidate candidate;
        candidate.hash = doc.sha256Hash;
        candidate.path = doc.filePath;
        candidate.fileSize = doc.fileSize;
        candidate.modifiedTime = doc.modifiedTime;
        candidate.category =
            magic::getPruneCategoryName(magic::getPruneCategory(doc.fileName, doc.fileExtension));

        candidates.push_back(std::move(candidate));
    }

    spdlog::info("Prune query: found {} candidates", candidates.size());
    return candidates;
}

bool RepairManager::matchesCategory(const metadata::DocumentInfo& doc,
                                    std::span<const std::string> categories) const noexcept {
    if (categories.empty())
        return true;

    auto category = magic::getPruneCategory(doc.fileName, doc.fileExtension);
    return std::ranges::any_of(categories, [category](const auto& cat) {
        return magic::matchesPruneGroup(category, cat);
    });
}

bool RepairManager::meetsAgeCriteria(const metadata::DocumentInfo& doc,
                                     std::chrono::seconds minAge) const noexcept {
    if (minAge.count() == 0)
        return true;

    auto now = std::chrono::system_clock::now();
    auto fileAge = std::chrono::duration_cast<std::chrono::seconds>(now - doc.modifiedTime);
    return fileAge >= minAge;
}

bool RepairManager::meetsSizeCriteria(const metadata::DocumentInfo& doc, int64_t minSize,
                                      int64_t maxSize) const noexcept {
    return doc.fileSize >= minSize && doc.fileSize <= maxSize;
}

Result<PruneResult> RepairManager::pruneFiles(const PruneConfig& config,
                                              std::function<void(uint64_t, uint64_t)> progress) {
    if (!repo_) {
        return Error{ErrorCode::InvalidArgument, "MetadataRepository not initialized"};
    }

    // Query all documents
    auto docsResult = repo_->queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        spdlog::error("Failed to query documents: {}", docsResult.error().message);
        return docsResult.error();
    }

    auto& allDocs = docsResult.value();
    spdlog::info("Prune: scanning {} documents", allDocs.size());

    // Filter using std::ranges (lazy evaluation)
    auto candidates =
        allDocs |
        std::views::filter([&](const auto& d) { return matchesCategory(d, config.categories); }) |
        std::views::filter([&](const auto& d) { return meetsAgeCriteria(d, config.minAge); }) |
        std::views::filter(
            [&](const auto& d) { return meetsSizeCriteria(d, config.minSize, config.maxSize); });

    // Materialize candidates for processing
    std::vector<metadata::DocumentInfo> toProcess;
    std::ranges::copy(candidates, std::back_inserter(toProcess));

    spdlog::info("Prune: found {} candidates (dry_run={})", toProcess.size(), config.dryRun);

    PruneResult result;
    uint64_t processed = 0;
    const uint64_t total = toProcess.size();

    for (const auto& doc : toProcess) {
        // Report progress
        if (progress && processed % 100 == 0) {
            progress(processed, total);
        }

        // Track category stats
        auto categoryName = std::string(
            magic::getPruneCategoryName(magic::getPruneCategory(doc.fileName, doc.fileExtension)));
        result.categoryCounts[categoryName]++;
        result.categorySizes[categoryName] += doc.fileSize;

        if (!config.dryRun) {
            // Delete filesystem file
            std::error_code ec;
            if (std::filesystem::exists(doc.filePath, ec)) {
                if (std::filesystem::remove(doc.filePath, ec)) {
                    result.deletedPaths.push_back(doc.filePath);
                } else {
                    spdlog::warn("Failed to delete {}: {}", doc.filePath, ec.message());
                    result.failedPaths.push_back(doc.filePath);
                    result.filesFailed++;
                    processed++;
                    continue;
                }
            }

            // Delete from metadata
            auto delResult = repo_->deleteDocument(doc.id);
            if (!delResult) {
                spdlog::warn("Failed to delete metadata for {}: {}", doc.filePath,
                             delResult.error().message);
                result.failedPaths.push_back(doc.filePath);
                result.filesFailed++;
                processed++;
                continue;
            }

            result.filesDeleted++;
            result.totalBytesFreed += doc.fileSize;
        } else {
            // Dry run: just count
            result.filesDeleted++;
            result.totalBytesFreed += doc.fileSize;
        }

        processed++;
    }

    // Final progress report
    if (progress) {
        progress(total, total);
    }

    spdlog::info("Prune complete: deleted={} failed={} bytes_freed={}", result.filesDeleted,
                 result.filesFailed, result.totalBytesFreed);

    return result;
}

// ============================================================================
// Path Tree Repair Operations
// ============================================================================

Result<PathTreeRepairResult>
RepairManager::repairPathTree(std::function<void(uint64_t, uint64_t)> progress) {
    if (!repo_) {
        return Error{ErrorCode::InvalidArgument, "MetadataRepository not initialized"};
    }

    PathTreeRepairResult result;

    // Query all documents
    auto docsResult = repo_->queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        spdlog::error("Failed to query documents for path tree repair: {}",
                      docsResult.error().message);
        return docsResult.error();
    }

    auto& allDocs = docsResult.value();
    const uint64_t total = allDocs.size();
    spdlog::info("Path tree repair: scanning {} documents", total);

    uint64_t processed = 0;
    for (const auto& doc : allDocs) {
        result.documentsScanned++;

        // Report progress
        if (progress && processed % 100 == 0) {
            progress(processed, total);
        }

        // Skip documents without a valid path
        if (doc.filePath.empty()) {
            processed++;
            continue;
        }

        // Check if this document already has a path tree entry
        auto existingNode = repo_->findPathTreeNodeByFullPath(doc.filePath);
        if (existingNode && existingNode.value().has_value()) {
            // Node exists, skip (or could update doc_count if needed)
            processed++;
            continue;
        }

        // Create path tree entry for this document
        try {
            auto treeRes = repo_->upsertPathTreeForDocument(doc, doc.id, true /* isNewDocument */,
                                                            std::span<const float>());
            if (treeRes) {
                result.nodesCreated++;
                spdlog::debug("Path tree repair: created entry for '{}'", doc.filePath);
            } else {
                result.errors++;
                spdlog::warn("Path tree repair: failed for '{}': {}", doc.filePath,
                             treeRes.error().message);
            }
        } catch (const std::exception& e) {
            result.errors++;
            spdlog::warn("Path tree repair: exception for '{}': {}", doc.filePath, e.what());
        }

        processed++;
    }

    // Final progress report
    if (progress) {
        progress(total, total);
    }

    spdlog::info("Path tree repair complete: scanned={} created={} errors={}",
                 result.documentsScanned, result.nodesCreated, result.errors);

    return result;
}

} // namespace yams::integrity
