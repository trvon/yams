#pragma once

#include <yams/api/content_store.h>

#include <functional>
#include <future>
#include <memory>
#include <optional>

namespace yams::api {

// Async callback types
using StoreCallback = std::function<void(const Result<StoreResult>&)>;
using RetrieveCallback = std::function<void(const Result<RetrieveResult>&)>;
using BoolCallback = std::function<void(const Result<bool>&)>;
using MetadataCallback = std::function<void(const Result<ContentMetadata>&)>;
using VoidCallback = std::function<void(const Result<void>&)>;

// Async wrapper for content store operations
class AsyncContentStore {
public:
    explicit AsyncContentStore(std::shared_ptr<IContentStore> store);
    ~AsyncContentStore();
    
    // Delete copy, enable move
    AsyncContentStore(const AsyncContentStore&) = delete;
    AsyncContentStore& operator=(const AsyncContentStore&) = delete;
    AsyncContentStore(AsyncContentStore&&) noexcept;
    AsyncContentStore& operator=(AsyncContentStore&&) noexcept;
    
    // Future-based async operations
    [[nodiscard]] std::future<Result<StoreResult>> storeAsync(
        const std::filesystem::path& path,
        const ContentMetadata& metadata = {},
        ProgressCallback progress = nullptr);
    
    [[nodiscard]] std::future<Result<RetrieveResult>> retrieveAsync(
        const std::string& hash,
        const std::filesystem::path& outputPath,
        ProgressCallback progress = nullptr);
    
    [[nodiscard]] std::future<Result<StoreResult>> storeStreamAsync(
        std::istream& stream,
        const ContentMetadata& metadata = {},
        ProgressCallback progress = nullptr);
    
    [[nodiscard]] std::future<Result<RetrieveResult>> retrieveStreamAsync(
        const std::string& hash,
        std::ostream& output,
        ProgressCallback progress = nullptr);
    
    [[nodiscard]] std::future<Result<bool>> existsAsync(const std::string& hash);
    [[nodiscard]] std::future<Result<bool>> removeAsync(const std::string& hash);
    
    [[nodiscard]] std::future<Result<ContentMetadata>> getMetadataAsync(
        const std::string& hash);
    
    [[nodiscard]] std::future<Result<void>> updateMetadataAsync(
        const std::string& hash,
        const ContentMetadata& metadata);
    
    // Callback-based async operations
    void storeAsync(
        const std::filesystem::path& path,
        StoreCallback callback,
        const ContentMetadata& metadata = {},
        ProgressCallback progress = nullptr);
    
    void retrieveAsync(
        const std::string& hash,
        const std::filesystem::path& outputPath,
        RetrieveCallback callback,
        ProgressCallback progress = nullptr);
    
    void existsAsync(
        const std::string& hash,
        BoolCallback callback);
    
    void removeAsync(
        const std::string& hash,
        BoolCallback callback);
    
    void getMetadataAsync(
        const std::string& hash,
        MetadataCallback callback);
    
    void updateMetadataAsync(
        const std::string& hash,
        const ContentMetadata& metadata,
        VoidCallback callback);
    
    // Batch async operations
    [[nodiscard]] std::future<std::vector<Result<StoreResult>>> storeBatchAsync(
        const std::vector<std::filesystem::path>& paths,
        const std::vector<ContentMetadata>& metadata = {});
    
    [[nodiscard]] std::future<std::vector<Result<bool>>> removeBatchAsync(
        const std::vector<std::string>& hashes);
    
    // Maintenance async operations
    [[nodiscard]] std::future<Result<void>> verifyAsync(
        ProgressCallback progress = nullptr);
    
    [[nodiscard]] std::future<Result<void>> compactAsync(
        ProgressCallback progress = nullptr);
    
    [[nodiscard]] std::future<Result<void>> garbageCollectAsync(
        ProgressCallback progress = nullptr);
    
    // Get underlying store
    [[nodiscard]] std::shared_ptr<IContentStore> getStore() const { return store_; }
    
    // Set concurrency limit
    void setMaxConcurrentOperations(size_t max);
    [[nodiscard]] size_t getMaxConcurrentOperations() const;
    
    // Get pending operation count
    [[nodiscard]] size_t getPendingOperations() const;
    
    // Wait for all pending operations
    void waitAll();
    
    // Cancel all pending operations (if possible)
    void cancelAll();
    
private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
    std::shared_ptr<IContentStore> store_;
};

// Create async content store with optional configuration
std::unique_ptr<AsyncContentStore> createAsyncContentStore(
    std::shared_ptr<IContentStore> store,
    size_t maxConcurrentOps = 10);

} // namespace yams::api