#include <spdlog/spdlog.h>
#include <filesystem>
#include <yams/app/services/services.hpp>
#include <yams/common/pattern_utils.h>

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

            // Collect file entries first
            std::vector<std::filesystem::directory_entry> entries;
            if (req.recursive) {
                for (const auto& entry : std::filesystem::recursive_directory_iterator(dirPath)) {
                    if (entry.is_regular_file()) entries.push_back(entry);
                }
            } else {
                for (const auto& entry : std::filesystem::directory_iterator(dirPath)) {
                    if (entry.is_regular_file()) entries.push_back(entry);
                }
            }

            // Parallel processing with bounded workers
            std::atomic<size_t> idx{0};
            size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
            size_t workers = std::min(hw, entries.size() > 0 ? entries.size() : size_t{1});
            std::mutex respMutex;
            auto workerFn = [&]() {
                while (true) {
                    size_t i = idx.fetch_add(1);
                    if (i >= entries.size()) break;
                    AddDirectoryResponse localResp; // partial counts
                    processDirectoryEntry(entries[i], req, localResp);
                    // Merge local results into shared response
                    std::lock_guard<std::mutex> lk(respMutex);
                    response.filesProcessed += localResp.filesProcessed;
                    response.filesIndexed += localResp.filesIndexed;
                    response.filesSkipped += localResp.filesSkipped;
                    response.filesFailed += localResp.filesFailed;
                    for (auto& r : localResp.results) response.results.push_back(std::move(r));
                }
            };
            std::vector<std::thread> threads; threads.reserve(workers);
            for (size_t t = 0; t < workers; ++t) threads.emplace_back(workerFn);
            for (auto& th : threads) th.join();

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
        if (!shouldIncludeFile(filePath, req.includePatterns, req.excludePatterns)) {
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
            if (!req.collection.empty()) {
                storeReq.metadata["collection"] = req.collection;
            }

            auto docService = makeDocumentService(ctx_);
            auto result = docService->store(storeReq);

            if (result) {
                fileResult.hash = result.value().hash;
                fileResult.sizeBytes = result.value().bytesStored;
                fileResult.success = true;
                response.filesIndexed++;
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

    bool shouldIncludeFile(const std::string& path, const std::vector<std::string>& includePatterns,
                           const std::vector<std::string>& excludePatterns) {
        std::filesystem::path filePath(path);
        std::string filename = filePath.filename().string();

        // Check exclude patterns first
        for (const auto& pattern : excludePatterns) {
            if (yams::common::wildcard_match(filename, pattern)) {
                return false;
            }
        }

        // If no include patterns, include by default
        if (includePatterns.empty()) {
            return true;
        }

        // Check include patterns
        for (const auto& pattern : includePatterns) {
            if (yams::common::wildcard_match(filename, pattern)) {
                return true;
            }
        }

        return false;
    }
};

std::shared_ptr<IIndexingService> makeIndexingService(const AppContext& ctx) {
    return std::make_shared<IndexingServiceImpl>(ctx);
}

} // namespace yams::app::services
