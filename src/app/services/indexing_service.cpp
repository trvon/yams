#include <spdlog/spdlog.h>
#include <filesystem>
#include <yams/app/services/services.hpp>
#include <yams/common/pattern_utils.h>
#include <yams/crypto/hasher.h>

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

            // Parallel processing with bounded workers
            std::atomic<size_t> idx{0};
            // Note: Underlying store and extraction paths may not be fully thread-safe across
            // platforms/tests. Process serially to ensure correctness and avoid races.
            size_t workers = 1;
            std::mutex respMutex;
            auto workerFn = [&]() {
                while (true) {
                    size_t i = idx.fetch_add(1);
                    if (i >= entries.size())
                        break;
                    AddDirectoryResponse localResp; // partial counts
                    const auto& ent = entries[i];
                    // Pre-log include/exclude decision for visibility
                    bool willInclude = shouldIncludeFile(req.directoryPath, ent.path().string(),
                                                         req.includePatterns, req.excludePatterns);
                    spdlog::info("[IndexingService] candidate: '{}' -> {}", ent.path().string(),
                                 willInclude ? "include" : "skip");
                    processDirectoryEntry(ent, req, localResp);
                    // Merge local results into shared response
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

            spdlog::info("[IndexingService] addDirectory: done. processed={} indexed={} skipped={} "
                         "failed={}",
                         response.filesProcessed, response.filesIndexed, response.filesSkipped,
                         response.filesFailed);

            return response;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Add directory failed: ") + e.what()};
        }
    }

private:
    AppContext ctx_;

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
        if (yams::common::matches_any_path(nfile, includePatterns))
            return true;
        if (yams::common::matches_any_path(relPath, includePatterns))
            return true;
        return false;
    }
};

std::shared_ptr<IIndexingService> makeIndexingService(const AppContext& ctx) {
    return std::make_shared<IndexingServiceImpl>(ctx);
}

} // namespace yams::app::services
