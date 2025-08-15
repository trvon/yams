#pragma once

#include <yams/api/content_store.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <memory>
#include <optional>

namespace yams::api {

// Builder pattern for creating content store instances
class ContentStoreBuilder {
public:
    ContentStoreBuilder();
    ~ContentStoreBuilder();

    // Delete copy, enable move
    ContentStoreBuilder(const ContentStoreBuilder&) = delete;
    ContentStoreBuilder& operator=(const ContentStoreBuilder&) = delete;
    ContentStoreBuilder(ContentStoreBuilder&&) noexcept;
    ContentStoreBuilder& operator=(ContentStoreBuilder&&) noexcept;

    // Configuration methods
    ContentStoreBuilder& withConfig(const ContentStoreConfig& config);
    ContentStoreBuilder& withStoragePath(const std::filesystem::path& path);
    ContentStoreBuilder& withChunkSize(size_t size);
    ContentStoreBuilder& withCompression(bool enable);
    ContentStoreBuilder& withCompressionType(const std::string& type);
    ContentStoreBuilder& withCompressionLevel(size_t level);
    ContentStoreBuilder& withDeduplication(bool enable);
    ContentStoreBuilder& withMaxConcurrentOperations(size_t max);
    ContentStoreBuilder& withIntegrityChecks(bool enable);
    ContentStoreBuilder& withGarbageCollectionInterval(std::chrono::seconds interval);

    // Component injection (for testing and customization)
    ContentStoreBuilder& withStorageEngine(std::shared_ptr<storage::IStorageEngine> engine);
    ContentStoreBuilder& withChunker(std::shared_ptr<chunking::IChunker> chunker);
    ContentStoreBuilder& withHasher(std::shared_ptr<crypto::IHasher> hasher);
    ContentStoreBuilder& withManifestManager(std::shared_ptr<manifest::IManifestManager> manager);
    ContentStoreBuilder& withReferenceCounter(std::shared_ptr<storage::IReferenceCounter> counter);

    // Build the content store
    [[nodiscard]] Result<std::unique_ptr<IContentStore>> build();

    // Build with validation
    [[nodiscard]] Result<std::unique_ptr<IContentStore>> buildAndValidate();

    // Static factory methods
    static Result<std::unique_ptr<IContentStore>>
    createDefault(const std::filesystem::path& storagePath);

    static Result<std::unique_ptr<IContentStore>>
    createFromConfig(const ContentStoreConfig& config);

    static Result<std::unique_ptr<IContentStore>> createInMemory();

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

// Factory function with sensible defaults
Result<std::unique_ptr<IContentStore>> createContentStore(const std::filesystem::path& storagePath);

// Factory function with full configuration
Result<std::unique_ptr<IContentStore>> createContentStore(const ContentStoreConfig& config);

} // namespace yams::api