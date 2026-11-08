#include <yams/api/content_store.h>
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

namespace {

constexpr std::size_t kPruneSignatureBytes = 4096;

magic::PruneCategory
classifyPruneCandidate(const metadata::DocumentInfo& doc,
                       const std::shared_ptr<api::IContentStore>& contentStore) {
    auto category = magic::getPruneCategory(doc.filePath, doc.fileExtension, doc.mimeType);
    // For extensionless files, a definitive signature is more specific than a generic path rule
    // such as a parent build directory.
    const bool needsSignature =
        doc.fileExtension.empty() || std::filesystem::path(doc.filePath).extension().empty();
    if (!needsSignature || !contentStore || doc.sha256Hash.empty()) {
        return category;
    }

    auto bytes = contentStore->retrieveBytesPrefix(doc.sha256Hash, kPruneSignatureBytes);
    if (!bytes || bytes.value().empty()) {
        return category;
    }
    const auto* data = reinterpret_cast<const uint8_t*>(bytes.value().data());
    const auto detectedMime =
        magic::detect_mime_type(std::span<const uint8_t>(data, bytes.value().size()));
    const auto signatureCategory = magic::getPruneCategory({}, {}, detectedMime);
    return signatureCategory != magic::PruneCategory::None ? signatureCategory : category;
}

std::vector<PruneCandidate>
buildPruneCandidates(std::span<const metadata::DocumentInfo> docs, const PruneConfig& config,
                     const std::shared_ptr<api::IContentStore>& contentStore) {
    std::vector<PruneCandidate> candidates;

    auto matchesCategory = [&](magic::PruneCategory category) -> bool {
        if (config.categories.empty())
            return true;
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

    for (const auto& doc : docs) {
        if (!meetsAgeCriteria(doc))
            continue;
        if (!meetsSizeCriteria(doc))
            continue;
        if (!meetsExtensionCriteria(doc))
            continue;
        const auto category = classifyPruneCandidate(doc, contentStore);
        if (!matchesCategory(category))
            continue;

        PruneCandidate candidate;
        candidate.hash = doc.sha256Hash;
        candidate.path = doc.filePath;
        candidate.fileSize = doc.fileSize;
        candidate.modifiedTime = doc.modifiedTime;
        candidate.category = magic::getPruneCategoryName(category);

        candidates.push_back(std::move(candidate));
    }

    return candidates;
}

} // namespace

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
                                       const PruneConfig& config,
                                       const std::shared_ptr<api::IContentStore>& contentStore) {
    // Query all documents
    auto docsResult = repo.queryDocuments(metadata::DocumentQueryOptions{});
    if (!docsResult) {
        spdlog::error("Failed to query documents: {}", docsResult.error().message);
        return {};
    }

    auto& allDocs = docsResult.value();
    spdlog::debug("Prune query: scanning {} documents", allDocs.size());

    auto candidates = buildPruneCandidates(allDocs, config, contentStore);

    spdlog::info("Prune query: found {} candidates", candidates.size());
    return candidates;
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

    // Use SQL anti-join to find only docs missing from path_tree instead of
    // loading all documents and checking each one individually (O(N) → O(missing)).
    auto missingDocs = repo_->findDocsMissingPathTree();
    if (!missingDocs) {
        spdlog::error("Failed to query documents missing from path tree: {}",
                      missingDocs.error().message);
        return missingDocs.error();
    }

    auto& docs = missingDocs.value();
    const uint64_t total = docs.size();
    spdlog::info("Path tree repair: {} documents missing from path tree", total);

    if (total == 0) {
        // Quick path: nothing to do. Still log the scan count from a lightweight count.
        auto countRes = repo_->countDocsMissingPathTree();
        // countRes should be 0, but even if it fails we're done.
        result.documentsScanned = 0;
        spdlog::info("Path tree repair complete: all documents have path tree entries");
        return result;
    }

    result.documentsScanned = total;

    uint64_t processed = 0;
    for (const auto& doc : docs) {
        // Report progress
        if (progress && processed % 100 == 0) {
            progress(processed, total);
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
