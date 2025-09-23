#include <spdlog/spdlog.h>
#include <unordered_set>
#include <yams/app/services/services.hpp>
#ifdef YAMS_ENABLE_DAEMON_FEATURES
#include <yams/daemon/components/PoolManager.h>
#endif

namespace yams::app::services {

class StatsServiceImpl : public IStatsService {
public:
    explicit StatsServiceImpl(const AppContext& ctx) : ctx_(ctx) {}

    Result<StatsResponse> getStats(const StatsRequest& req) override {
        try {
            StatsResponse response;

            // Get basic stats from content store
            if (ctx_.store) {
                auto storeStats = ctx_.store->getStats();
                response.totalObjects = storeStats.totalObjects;
                response.totalBytes = storeStats.totalBytes;
                response.deduplicationSavings = storeStats.deduplicatedBytes;
            }

            // Get unique hash count from metadata repository
            if (ctx_.metadataRepo) {
                auto docsResult = ctx_.metadataRepo->findDocumentsByPath("%");
                if (docsResult) {
                    std::unordered_set<std::string> uniqueHashes;
                    for (const auto& doc : docsResult.value()) {
                        uniqueHashes.insert(doc.sha256Hash);
                    }
                    response.uniqueHashes = uniqueHashes.size();
                }
            }

            // Collect file type statistics if requested
            if (req.fileTypes && ctx_.metadataRepo) {
                auto docsResult = ctx_.metadataRepo->findDocumentsByPath("%");
                if (docsResult) {
                    std::unordered_map<std::string, FileTypeStats> typeMap;

                    for (const auto& doc : docsResult.value()) {
                        std::string extension = getFileExtension(doc.fileName);
                        if (extension.empty()) {
                            extension = "(no extension)";
                        }

                        auto& stats = typeMap[extension];
                        stats.extension = extension;
                        stats.count++;
                        stats.totalBytes += doc.fileSize;
                    }

                    // Convert to vector and sort by count
                    for (auto& [ext, stats] : typeMap) {
                        response.fileTypes.push_back(std::move(stats));
                    }

                    std::sort(response.fileTypes.begin(), response.fileTypes.end(),
                              [](const FileTypeStats& a, const FileTypeStats& b) {
                                  return a.count > b.count;
                              });
                }
            }

            // Add additional verbose stats if requested
            if (req.verbose) {
                if (ctx_.searchExecutor) {
                    // Could add search index stats here
                    response.additionalStats["search_indexed_documents"] = 0; // placeholder
                }
                if (ctx_.hybridEngine) {
                    // Could add vector database stats here
                    response.additionalStats["vector_embeddings"] = 0; // placeholder
                }
#ifdef YAMS_ENABLE_DAEMON_FEATURES
                // PoolManager stats (IPC component)
                try {
                    auto stats = yams::daemon::PoolManager::instance().stats("ipc");
                    response.additionalStats["pool_ipc_size"] =
                        static_cast<std::uint64_t>(stats.current_size);
                    response.additionalStats["pool_ipc_resizes"] =
                        static_cast<std::uint64_t>(stats.resize_events);
                    response.additionalStats["pool_ipc_rejected_on_cap"] =
                        static_cast<std::uint64_t>(stats.rejected_on_cap);
                    response.additionalStats["pool_ipc_throttled_on_cooldown"] =
                        static_cast<std::uint64_t>(stats.throttled_on_cooldown);
                } catch (...) {
                    // ignore if PoolManager not available
                }
#endif
            }

            return response;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError, std::string("Get stats failed: ") + e.what()};
        }
    }

private:
    AppContext ctx_;

    std::string getFileExtension(const std::string& filename) {
        auto pos = filename.find_last_of('.');
        if (pos != std::string::npos && pos < filename.length() - 1) {
            return filename.substr(pos);
        }
        return "";
    }
};

std::shared_ptr<IStatsService> makeStatsService(const AppContext& ctx) {
    return std::make_shared<StatsServiceImpl>(ctx);
}

} // namespace yams::app::services
