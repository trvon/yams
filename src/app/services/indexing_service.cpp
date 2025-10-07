#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <thread>
#include <yams/app/services/services.hpp>
#include <yams/common/pattern_utils.h>
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/metadata/tree_builder.h>

namespace yams::app::services {

class IndexingServiceImpl : public IIndexingService {
public:
    explicit IndexingServiceImpl(const AppContext& ctx) : ctx_(ctx) {}

    Result<AddDirectoryResponse> addDirectory(const AddDirectoryRequest& req) override {
        try {
            AddDirectoryResponse response;
            response.directoryPath = req.directoryPath;
            response.collection = req.collection;

            // Check if directory exists
            std::filesystem::path dirPath(req.directoryPath);
            if (!std::filesystem::exists(dirPath) || !std::filesystem::is_directory(dirPath)) {
                return Error{ErrorCode::InvalidArgument,
                             "Directory does not exist: " + req.directoryPath};
            }

            spdlog::info("[IndexingService] addDirectory: root='{}' recursive={} include={} "
                         "exclude={} tags={} collection='{}'",
                         req.directoryPath, req.recursive, req.includePatterns.size(),
                         req.excludePatterns.size(), req.tags.size(), req.collection);

            // Collect file entries first
            std::vector<std::filesystem::directory_entry> entries;

            // Fast path: allow explicit relative file paths in includePatterns (no wildcards)
            // This lets callers reference specific files even when recursive=false.
            auto hasWildcard = [](const std::string& s) {
                return s.find('*') != std::string::npos || s.find('?') != std::string::npos ||
                       s.find('[') != std::string::npos;
            };
            std::vector<std::filesystem::directory_entry> explicitEntries;
            for (const auto& pat : req.includePatterns) {
                // Treat patterns containing a path separator and no wildcards as explicit paths
                if (!hasWildcard(pat) &&
                    (pat.find('/') != std::string::npos || pat.find('\\') != std::string::npos)) {
                    std::filesystem::path p = dirPath / std::filesystem::path(pat);
                    std::error_code ec2;
                    if (std::filesystem::is_regular_file(p, ec2) && !ec2) {
                        explicitEntries.emplace_back(p);
                    }
                }
            }

            if (!explicitEntries.empty()) {
                entries = std::move(explicitEntries);
            } else {
                std::error_code ec;
                // Configure directory iteration options to handle problematic files gracefully
                auto options = std::filesystem::directory_options::skip_permission_denied;

                if (req.recursive) {
                    // Use error_code version to avoid exceptions from circular symlinks
                    std::filesystem::recursive_directory_iterator it(dirPath, options, ec);
                    std::filesystem::recursive_directory_iterator end;

                    while (it != end) {
                        std::error_code entry_ec;

                        // Check if entry is accessible before processing
                        try {
                            if (it->is_regular_file(entry_ec) && !entry_ec) {
                                entries.push_back(*it);
                            } else if (entry_ec) {
                                // Log but don't fail - common with broken/circular symlinks
                                spdlog::warn(
                                    "[IndexingService] Skipping inaccessible entry '{}': {}",
                                    it->path().string(), entry_ec.message());
                            }
                        } catch (const std::filesystem::filesystem_error& e) {
                            // Handle circular symlinks and other iteration errors
                            spdlog::warn("[IndexingService] Skipping problematic entry: {}",
                                         e.what());
                        }

                        // Advance iterator with error handling
                        it.increment(ec);
                        if (ec) {
                            spdlog::warn("[IndexingService] Error advancing iterator: {}",
                                         ec.message());
                            ec.clear();
                        }
                    }
                } else {
                    std::filesystem::directory_iterator it(dirPath, options, ec);
                    std::filesystem::directory_iterator end;

                    if (ec) {
                        spdlog::error("[IndexingService] Failed to open directory '{}': {}",
                                      dirPath.string(), ec.message());
                    } else {
                        while (it != end) {
                            std::error_code entry_ec;
                            try {
                                if (it->is_regular_file(entry_ec) && !entry_ec) {
                                    entries.push_back(*it);
                                } else if (entry_ec) {
                                    spdlog::warn(
                                        "[IndexingService] Skipping inaccessible entry '{}': {}",
                                        it->path().string(), entry_ec.message());
                                }
                            } catch (const std::filesystem::filesystem_error& e) {
                                spdlog::warn("[IndexingService] Skipping problematic entry: {}",
                                             e.what());
                            }

                            it.increment(ec);
                            if (ec) {
                                spdlog::warn("[IndexingService] Error advancing iterator: {}",
                                             ec.message());
                                ec.clear();
                            }
                        }
                    }
                }
            }

            spdlog::info("[IndexingService] addDirectory: collected {} candidate files under '{}'",
                         entries.size(), req.directoryPath);

            const std::size_t backlog = entries.size();
            if (backlog == 0) {
                publishIngestMetrics(0, 0);
            } else {
                std::atomic<size_t> idx{0};
                std::atomic<size_t> remaining{backlog};
                size_t workers = resolveWorkerCount(backlog);
                if (workers == 0)
                    workers = 1;
                publishIngestMetrics(backlog, workers);
                std::mutex respMutex;
                auto workerFn = [&]() {
                    while (true) {
                        size_t i = idx.fetch_add(1, std::memory_order_relaxed);
                        if (i >= backlog)
                            break;
                        AddDirectoryResponse localResp; // partial counts
                        const auto& ent = entries[i];
                        bool willInclude =
                            shouldIncludeFile(req.directoryPath, ent.path().string(),
                                              req.includePatterns, req.excludePatterns);
                        spdlog::info("[IndexingService] candidate: '{}' -> {}", ent.path().string(),
                                     willInclude ? "include" : "skip");
                        processDirectoryEntry(ent, req, localResp);
                        auto leftBefore = remaining.fetch_sub(1, std::memory_order_relaxed);
                        if (leftBefore > 0)
                            publishIngestQueued(leftBefore - 1);
                        std::lock_guard<std::mutex> lk(respMutex);
                        response.filesProcessed += localResp.filesProcessed;
                        response.filesIndexed += localResp.filesIndexed;
                        response.filesSkipped += localResp.filesSkipped;
                        response.filesFailed += localResp.filesFailed;
                        for (auto& r : localResp.results)
                            response.results.push_back(std::move(r));
                    }
                };
                std::vector<std::thread> threads;
                threads.reserve(workers);
                for (size_t t = 0; t < workers; ++t)
                    threads.emplace_back(workerFn);
                for (auto& th : threads)
                    th.join();
                publishIngestMetrics(0, 0);
            }

            spdlog::info("[IndexingService] addDirectory: done. processed={} indexed={} skipped={} "
                         "failed={}",
                         response.filesProcessed, response.filesIndexed, response.filesSkipped,
                         response.filesFailed);

            // Generate automatic snapshot for directory operations
            response.snapshotId = generateSnapshotId();
            response.snapshotLabel = req.snapshotLabel;

            // Detect git metadata
            auto gitMeta = detectGitMetadata(dirPath);

            // TODO: Build Merkle tree for the directory (PBI-043)
            // TreeBuilder integration requires IStorageEngine interface
            // For now, tree_root_hash will be NULL until we add adapter layer
            std::string treeRootHash;

            // Store snapshot metadata in tree_snapshots table
            if (ctx_.metadataRepo && !response.snapshotId.empty()) {
                try {
                    metadata::TreeSnapshotRecord snapshot;
                    snapshot.snapshotId = response.snapshotId;
                    snapshot.rootTreeHash = treeRootHash;
                    snapshot.createdTime = std::chrono::duration_cast<std::chrono::seconds>(
                                               std::chrono::system_clock::now().time_since_epoch())
                                               .count();
                    snapshot.fileCount = response.filesIndexed;
                    snapshot.totalBytes = 0; // TODO: accumulate from file sizes

                    // Store metadata fields
                    snapshot.metadata["directory_path"] = req.directoryPath;
                    if (!req.snapshotLabel.empty()) {
                        snapshot.metadata["snapshot_label"] = req.snapshotLabel;
                    }
                    if (!gitMeta.commitHash.empty()) {
                        snapshot.metadata["git_commit"] = gitMeta.commitHash;
                    }
                    if (!gitMeta.branch.empty()) {
                        snapshot.metadata["git_branch"] = gitMeta.branch;
                    }
                    if (!gitMeta.remoteUrl.empty()) {
                        snapshot.metadata["git_remote"] = gitMeta.remoteUrl;
                    }
                    if (!req.collection.empty()) {
                        snapshot.metadata["collection"] = req.collection;
                    }

                    auto storeResult = ctx_.metadataRepo->upsertTreeSnapshot(snapshot);
                    if (!storeResult) {
                        spdlog::warn("[IndexingService] Failed to store snapshot metadata: {}",
                                     storeResult.error().message);
                    } else {
                        spdlog::info("[IndexingService] Stored snapshot metadata: id={}",
                                     response.snapshotId);
                        // PBI-043: Populate KG with path/blob nodes and relationships
                        if (ctx_.kgStore && !response.results.empty()) {
                            try {
                                std::size_t kgNodesCreated = 0;
                                std::size_t kgEdgesCreated = 0;

                                for (const auto& result : response.results) {
                                    if (result.hash.empty() || result.path.empty()) {
                                        continue; // Skip incomplete results
                                    }

                                    // Create blob node for content hash
                                    auto blobNodeResult = ctx_.kgStore->ensureBlobNode(result.hash);
                                    if (!blobNodeResult) {
                                        spdlog::debug(
                                            "[IndexingService] KG ensureBlobNode failed for {}: {}",
                                            result.hash.substr(0, 8),
                                            blobNodeResult.error().message);
                                        continue;
                                    }
                                    kgNodesCreated++;

                                    // Create path node for this file in this snapshot
                                    metadata::PathNodeDescriptor pathDesc;
                                    pathDesc.snapshotId = response.snapshotId;
                                    pathDesc.path = result.path;
                                    pathDesc.rootTreeHash = treeRootHash;
                                    pathDesc.isDirectory = false;

                                    auto pathNodeResult = ctx_.kgStore->ensurePathNode(pathDesc);
                                    if (!pathNodeResult) {
                                        spdlog::debug(
                                            "[IndexingService] KG ensurePathNode failed for {}: {}",
                                            result.path, pathNodeResult.error().message);
                                        continue;
                                    }
                                    kgNodesCreated++;

                                    // Link path version to blob (has_version edge)
                                    // Note: diffId not available yet (would need TreeDiffer
                                    // integration)
                                    auto linkResult = ctx_.kgStore->linkPathVersion(
                                        pathNodeResult.value(), blobNodeResult.value(), 0);
                                    if (!linkResult) {
                                        spdlog::debug(
                                            "[IndexingService] KG linkPathVersion failed: {}",
                                            linkResult.error().message);
                                        continue;
                                    }
                                    kgEdgesCreated++;
                                }

                                if (kgNodesCreated > 0 || kgEdgesCreated > 0) {
                                    spdlog::info(
                                        "[IndexingService] KG populated: {} nodes, {} edges",
                                        kgNodesCreated, kgEdgesCreated);
                                }
                            } catch (const std::exception& e) {
                                spdlog::warn("[IndexingService] Exception populating KG: {}",
                                             e.what());
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("[IndexingService] Exception storing snapshot: {}", e.what());
                }
            }

            // Log summary
            if (!gitMeta.commitHash.empty()) {
                spdlog::info(
                    "[IndexingService] Snapshot {} created for {} files (git: {}, tree: {})",
                    response.snapshotId, response.filesIndexed, gitMeta.commitHash.substr(0, 8),
                    treeRootHash.empty() ? "none" : treeRootHash.substr(0, 8));
            } else {
                spdlog::info("[IndexingService] Snapshot {} created for {} files (tree: {})",
                             response.snapshotId, response.filesIndexed,
                             treeRootHash.empty() ? "none" : treeRootHash.substr(0, 8));
            }

            return response;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Add directory failed: ") + e.what()};
        }
    }

private:
    AppContext ctx_;

    /**
     * Generate ISO 8601 timestamp-based snapshot ID with microsecond precision.
     * Format: 2025-10-01T14:30:00.123456Z
     */
    std::string generateSnapshotId() const {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        auto micros =
            std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() %
            1000000;

        std::tm tm_utc;
#ifdef _WIN32
        gmtime_s(&tm_utc, &time_t_now);
#else
        gmtime_r(&time_t_now, &tm_utc);
#endif

        std::ostringstream oss;
        oss << std::put_time(&tm_utc, "%Y-%m-%dT%H:%M:%S") << '.' << std::setfill('0')
            << std::setw(6) << micros << 'Z';
        return oss.str();
    }

    /**
     * Detect git repository metadata (commit hash, branch, remote URL).
     * Returns empty values if not in a git repository.
     */
    struct GitMetadata {
        std::string commitHash;
        std::string branch;
        std::string remoteUrl;
    };

    GitMetadata detectGitMetadata(const std::filesystem::path& path) const {
        GitMetadata meta;

        // Find git directory by walking up from path
        std::filesystem::path gitDir;
        auto current = std::filesystem::absolute(path);
        while (current.has_parent_path()) {
            auto candidate = current / ".git";
            if (std::filesystem::exists(candidate)) {
                gitDir = candidate;
                break;
            }
            auto parent = current.parent_path();
            if (parent == current)
                break;
            current = parent;
        }

        if (gitDir.empty()) {
            return meta; // Not in a git repository
        }

        // Read HEAD to get branch
        try {
            auto headPath = gitDir / "HEAD";
            if (std::filesystem::exists(headPath)) {
                std::ifstream headFile(headPath);
                std::string line;
                if (std::getline(headFile, line)) {
                    if (line.starts_with("ref: refs/heads/")) {
                        meta.branch = line.substr(16); // Skip "ref: refs/heads/"
                    } else if (line.size() == 40) {
                        meta.commitHash = line; // Detached HEAD
                    }
                }
            }

            // Read commit hash from branch ref
            if (!meta.branch.empty() && meta.commitHash.empty()) {
                auto refPath = gitDir / "refs" / "heads" / meta.branch;
                if (std::filesystem::exists(refPath)) {
                    std::ifstream refFile(refPath);
                    std::getline(refFile, meta.commitHash);
                }
            }

            // Read remote URL from config
            auto configPath = gitDir / "config";
            if (std::filesystem::exists(configPath)) {
                std::ifstream configFile(configPath);
                std::string line;
                bool inRemoteSection = false;
                while (std::getline(configFile, line)) {
                    // Simple parser: look for [remote "origin"] then url =
                    if (line.find("[remote \"origin\"]") != std::string::npos) {
                        inRemoteSection = true;
                    } else if (inRemoteSection && line.find("url = ") != std::string::npos) {
                        auto pos = line.find("url = ");
                        meta.remoteUrl = line.substr(pos + 6);
                        break;
                    } else if (line.starts_with("[") && inRemoteSection) {
                        break; // Left remote section
                    }
                }
            }
        } catch (const std::exception& e) {
            spdlog::debug("Failed to read git metadata: {}", e.what());
        }

        return meta;
    }

    void processDirectoryEntry(const std::filesystem::directory_entry& entry,
                               const AddDirectoryRequest& req, AddDirectoryResponse& response) {
        if (!entry.is_regular_file()) {
            return;
        }

        response.filesProcessed++;

        std::string filePath = entry.path().string();

        // Apply include/exclude filters
        if (!shouldIncludeFile(req.directoryPath, filePath, req.includePatterns,
                               req.excludePatterns)) {
            response.filesSkipped++;
            return;
        }

        // Store the file
        IndexedFileResult fileResult;
        fileResult.path = filePath;

        try {
            // Use document service to store the file
            StoreDocumentRequest storeReq;
            storeReq.path = filePath;
            storeReq.metadata = req.metadata;
            // Propagate collection and tags to each stored file
            storeReq.collection = req.collection;
            storeReq.tags = req.tags;
            if (!req.collection.empty()) {
                storeReq.metadata["collection"] = req.collection;
            }
            // If caller requested deferred extraction, propagate to DocumentService
            storeReq.deferExtraction = req.deferExtraction;

            auto docService = makeDocumentService(ctx_);
            auto result = docService->store(storeReq);

            if (result) {
                fileResult.hash = result.value().hash;
                fileResult.sizeBytes = result.value().bytesStored;
                fileResult.success = true;
                response.filesIndexed++;

                // Optional post-add verification
                if (req.verify) {
                    bool ok = true;
                    std::string verr;
                    try {
                        // Verify content hash by hashing source file; also verify existence in
                        // store
                        auto hasher = yams::crypto::createSHA256Hasher();
                        std::string fileHash = hasher->hashFile(entry.path());
                        if (!fileHash.empty() && !fileResult.hash.empty() &&
                            fileHash != fileResult.hash) {
                            ok = false;
                            verr = "hash mismatch (computed vs stored)";
                        }
                        if (ok && ctx_.store) {
                            auto er = ctx_.store->exists(fileResult.hash);
                            if (!er || !er.value()) {
                                ok = false;
                                verr = er ? std::string("content not found in store")
                                          : er.error().message;
                            }
                        }
                    } catch (const std::exception& ex) {
                        ok = false;
                        verr = ex.what();
                    } catch (...) {
                        ok = false;
                        verr = "unknown verification error";
                    }
                    if (!ok) {
                        fileResult.success = false;
                        fileResult.error = verr;
                        // Adjust counts: this file should be considered failed
                        if (response.filesIndexed > 0)
                            --response.filesIndexed;
                        response.filesFailed++;
                    }
                }
            } else {
                fileResult.error = result.error().message;
                fileResult.success = false;
                response.filesFailed++;
            }
        } catch (const std::exception& e) {
            fileResult.error = e.what();
            fileResult.success = false;
            response.filesFailed++;
        }

        response.results.push_back(std::move(fileResult));
    }

    bool shouldIncludeFile(const std::string& rootDir, const std::string& absPath,
                           const std::vector<std::string>& includePatterns,
                           const std::vector<std::string>& excludePatterns) {
        std::error_code ec;
        std::filesystem::path root(rootDir);
        std::filesystem::path p(absPath);
        std::filesystem::path rel = std::filesystem::relative(p, root, ec);
        std::string relPath = ec ? p.generic_string() : rel.generic_string();
        std::string filename = p.filename().string();

        // Normalize for matching
        relPath = yams::common::normalize_path(relPath);
        std::string nfile = yams::common::normalize_path(filename);

        // Exclude: if any exclude matches filename or rel path, skip
        if (!excludePatterns.empty()) {
            if (yams::common::matches_any_path(nfile, excludePatterns) ||
                yams::common::matches_any_path(relPath, excludePatterns)) {
                return false;
            }
        }

        // If no include patterns, include by default
        if (includePatterns.empty())
            return true;

        // Include if any include pattern matches filename or rel path
        if (yams::common::matches_any(nfile, includePatterns)) {
            return true;
        }
        if (yams::common::matches_any_path(relPath, includePatterns)) {
            return true;
        }
        return false;
    }

    std::size_t resolveWorkerCount(std::size_t backlog) const {
        if (backlog == 0)
            return 1;
        if (ctx_.service_manager) {
            auto target = ctx_.service_manager->ingestWorkerTarget();
            return target == 0 ? 1 : target;
        }
        if (!yams::daemon::TuneAdvisor::enableParallelIngest())
            return 1;
        std::size_t cap = yams::daemon::TuneAdvisor::maxIngestWorkers();
        if (cap == 0)
            cap = std::max<std::size_t>(1, std::thread::hardware_concurrency());
        std::size_t storageCap = yams::daemon::TuneAdvisor::storagePoolSize();
        if (storageCap > 0)
            cap = std::min(cap, storageCap);
        if (cap == 0)
            cap = 1;
        const std::size_t perWorker = std::max<std::size_t>(
            1, static_cast<std::size_t>(yams::daemon::TuneAdvisor::ingestBacklogPerWorker()));
        std::size_t desired = (backlog + perWorker - 1) / perWorker;
        if (desired == 0)
            desired = 1;
        if (desired > cap)
            desired = cap;
        return desired;
    }

    void publishIngestMetrics(std::size_t queued, std::size_t active) const {
        if (ctx_.service_manager)
            ctx_.service_manager->publishIngestMetrics(queued, active);
    }

    void publishIngestQueued(std::size_t queued) const {
        if (ctx_.service_manager)
            ctx_.service_manager->publishIngestQueued(queued);
    }
};

std::shared_ptr<IIndexingService> makeIndexingService(const AppContext& ctx) {
    return std::make_shared<IndexingServiceImpl>(ctx);
}

} // namespace yams::app::services
