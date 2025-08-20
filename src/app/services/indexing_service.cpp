#include <spdlog/spdlog.h>
#include <filesystem>
#include <yams/app/services/services.hpp>

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

            // Iterate through directory
            if (req.recursive) {
                std::filesystem::recursive_directory_iterator dirIter(dirPath);
                for (const auto& entry : dirIter) {
                    processDirectoryEntry(entry, req, response);
                }
            } else {
                std::filesystem::directory_iterator dirIter(dirPath);
                for (const auto& entry : dirIter) {
                    processDirectoryEntry(entry, req, response);
                }
            }

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
        // Simple pattern matching - could be enhanced with proper glob support
        std::filesystem::path filePath(path);
        std::string filename = filePath.filename().string();
        std::string extension = filePath.extension().string();

        // Check exclude patterns first
        for (const auto& pattern : excludePatterns) {
            if (filename.find(pattern) != std::string::npos || extension == pattern) {
                return false;
            }
        }

        // If no include patterns, include by default
        if (includePatterns.empty()) {
            return true;
        }

        // Check include patterns
        for (const auto& pattern : includePatterns) {
            if (filename.find(pattern) != std::string::npos || extension == pattern) {
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