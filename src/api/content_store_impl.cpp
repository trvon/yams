#include <yams/api/content_store.h>
#include <yams/api/content_store_error.h>
#include <yams/api/progress_reporter.h>
#include <yams/chunking/chunker.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <spdlog/spdlog.h>

#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace yams::api {

// Implementation of the content store
class ContentStore : public IContentStore {
public:
    ContentStore(std::shared_ptr<storage::IStorageEngine> storage,
                 std::shared_ptr<chunking::IChunker> chunker,
                 std::shared_ptr<crypto::IHasher> hasher,
                 std::shared_ptr<manifest::IManifestManager> manifestManager,
                 std::shared_ptr<storage::IReferenceCounter> refCounter, ContentStoreConfig config)
        : storage_(std::move(storage)), chunker_(std::move(chunker)), hasher_(std::move(hasher)),
          manifestManager_(std::move(manifestManager)), refCounter_(std::move(refCounter)),
          config_(std::move(config)) {
        spdlog::debug("ContentStore initialized with storage path: {}",
                      config_.storagePath.string());
    }

    // File-based store operation
    Result<StoreResult> store(const std::filesystem::path& path, const ContentMetadata& metadata,
                              ProgressCallback progress) override {
        auto startTime = std::chrono::steady_clock::now();

        // Validate file exists
        if (!std::filesystem::exists(path)) {
            spdlog::error("File not found: {}", path.string());
            return Result<StoreResult>(ErrorCode::FileNotFound);
        }

        // Get file info
        uint64_t fileSize = std::filesystem::file_size(path);
        ProgressReporter reporter(fileSize);
        if (progress) {
            reporter.setCallback(progress);
        }

        // Hash the complete file
        std::string fileHash = hasher_->hashFile(path);
        spdlog::debug("File hash: {} for {}", fileHash, path.string());

        // Create file info
        FileInfo fileInfo{.hash = fileHash,
                          .size = fileSize,
                          .mimeType = metadata.mimeType,
                          .createdAt = metadata.createdAt,
                          .originalName =
                              metadata.name.empty() ? path.filename().string() : metadata.name};

        // Chunk the file
        auto chunks = chunker_->chunkFile(path);
        spdlog::debug("File chunked into {} chunks", chunks.size());

        // Store chunks and track deduplication
        uint64_t bytesStored = 0;
        uint64_t bytesDeduped = 0;

        // Begin reference counting transaction
        auto transaction = refCounter_->beginTransaction();

        for (const auto& chunk : chunks) {
            // Check if chunk already exists
            auto existsResult = storage_->exists(chunk.hash);
            if (!existsResult) {
                transaction->rollback();
                return Result<StoreResult>(existsResult.error());
            }

            if (existsResult.value()) {
                // Chunk already exists, just increment reference
                bytesDeduped += chunk.size;

                // Get on-disk (compressed) size for reference counting
                auto blockSizeResult = storage_->getBlockSize(chunk.hash);
                size_t compressedSize =
                    blockSizeResult ? static_cast<size_t>(blockSizeResult.value()) : chunk.size;

                auto incResult = refCounter_->increment(chunk.hash, compressedSize, chunk.size);
                if (!incResult) {
                    transaction->rollback();
                    return Result<StoreResult>(incResult.error());
                }
            } else {
                // Store new chunk
                auto storeResult = storage_->store(
                    chunk.hash, std::span<const std::byte>(chunk.data.data(), chunk.data.size()));
                if (!storeResult) {
                    transaction->rollback();
                    return Result<StoreResult>(storeResult.error());
                }

                bytesStored += chunk.size;

                // Get on-disk (compressed) size for reference counting
                auto blockSizeResult = storage_->getBlockSize(chunk.hash);
                size_t compressedSize =
                    blockSizeResult ? static_cast<size_t>(blockSizeResult.value()) : chunk.size;

                // Add initial reference with both compressed and uncompressed sizes
                auto incResult = refCounter_->increment(chunk.hash, compressedSize, chunk.size);
                if (!incResult) {
                    transaction->rollback();
                    return Result<StoreResult>(incResult.error());
                }
            }

            // Report progress
            reporter.addProgress(chunk.size);
        }

        // Convert Chunk vector to ChunkRef vector
        std::vector<manifest::ChunkRef> chunkRefs;
        chunkRefs.reserve(chunks.size());
        for (const auto& chunk : chunks) {
            chunkRefs.push_back({chunk.hash, chunk.offset, static_cast<uint32_t>(chunk.size)});
        }

        // Create and store manifest
        auto manifestResult = manifestManager_->createManifest(fileInfo, chunkRefs);
        if (!manifestResult) {
            transaction->rollback();
            return Result<StoreResult>(manifestResult.error());
        }

        auto& manifest = manifestResult.value();

        // Store manifest metadata
        {
            std::unique_lock lock(metadataMutex_);
            metadataStore_[fileHash] = metadata;
        }

        // Store manifest
        auto manifestData = manifestManager_->serialize(manifest);
        if (!manifestData) {
            transaction->rollback();
            return Result<StoreResult>(manifestData.error());
        }

        auto manifestHash = fileHash + ".manifest";
        auto manifestStoreResult =
            storage_->store(manifestHash, std::span<const std::byte>(manifestData.value()));
        if (!manifestStoreResult) {
            transaction->rollback();
            return Result<StoreResult>(manifestStoreResult.error());
        }

        // Commit transaction
        auto commitResult = transaction->commit();
        if (!commitResult) {
            return Result<StoreResult>(commitResult.error());
        }

        // Update statistics
        updateStats(bytesStored, bytesDeduped, 0, 1, 1, 0, 0);

        auto endTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        StoreResult result{.contentHash = fileHash,
                           .bytesStored = fileSize,
                           .bytesDeduped = bytesDeduped,
                           .duration = duration};

        spdlog::debug("Stored file {} with hash {}, dedup ratio: {:.2f}%", path.string(), fileHash,
                      result.dedupRatio() * 100);

        return result;
    }

    // File-based retrieve operation
    Result<RetrieveResult> retrieve(const std::string& hash,
                                    const std::filesystem::path& outputPath,
                                    ProgressCallback progress) override {
        auto startTime = std::chrono::steady_clock::now();

        // Validate hash format
        if (hash.length() != HASH_STRING_SIZE) {
            spdlog::error(
                "Invalid hash format for retrieve: expected {} characters, got {} for hash '{}'",
                HASH_STRING_SIZE, hash.length(), hash);
            return Result<RetrieveResult>(ErrorCode::InvalidArgument);
        }

        // Retrieve manifest
        auto manifestHash = hash + ".manifest";
        auto manifestResult = storage_->retrieve(manifestHash);
        if (!manifestResult) {
            return Result<RetrieveResult>(manifestResult.error());
        }

        // Deserialize manifest
        auto manifest =
            manifestManager_->deserialize(std::span<const std::byte>(manifestResult.value()));
        if (!manifest) {
            return Result<RetrieveResult>(manifest.error());
        }

        // Setup progress reporting
        ProgressReporter reporter(manifest.value().fileSize);
        if (progress) {
            reporter.setCallback(progress);
        }

        // Create chunk provider implementation
        class ChunkProvider : public manifest::IChunkProvider {
        private:
            const storage::IStorageEngine* storage_;

        public:
            explicit ChunkProvider(const storage::IStorageEngine* storage) : storage_(storage) {}
            Result<std::vector<std::byte>> getChunk(const std::string& hash) const override {
                return storage_->retrieve(hash);
            }
        };
        ChunkProvider chunkProvider(storage_.get());

        // Reconstruct file
        auto reconstructResult =
            manifestManager_->reconstructFile(manifest.value(), outputPath, chunkProvider);
        if (!reconstructResult) {
            return Result<RetrieveResult>(reconstructResult.error());
        }

        // Get metadata
        ContentMetadata metadata;
        {
            std::shared_lock lock(metadataMutex_);
            auto it = metadataStore_.find(hash);
            if (it != metadataStore_.end()) {
                metadata = it->second;
                metadata.accessedAt = std::chrono::system_clock::now(); // Update access time
            }
        }

        // Update statistics
        updateStats(0, 0, manifest.value().fileSize, 0, 0, 1, 0);

        auto endTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        RetrieveResult result{.found = true,
                              .size = manifest.value().fileSize,
                              .metadata = metadata,
                              .duration = duration};

        spdlog::debug("Retrieved file with hash {} to {}", hash, outputPath.string());

        return result;
    }

    // Stream-based store operation
    Result<StoreResult> storeStream(std::istream& stream, const ContentMetadata& metadata,
                                    ProgressCallback progress) override {
        // Create temporary file for streaming
        auto tempPath = config_.storagePath / "temp" /
                        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        std::filesystem::create_directories(tempPath.parent_path());

        // Write stream to temporary file
        std::ofstream tempFile(tempPath, std::ios::binary);
        if (!tempFile) {
            return Result<StoreResult>(ErrorCode::PermissionDenied);
        }

        // Copy stream to file
        tempFile << stream.rdbuf();
        tempFile.close();

        // Store using file-based method
        auto result = store(tempPath, metadata, progress);

        // Clean up temporary file
        std::filesystem::remove(tempPath);

        return result;
    }

    // Stream-based retrieve operation
    Result<RetrieveResult> retrieveStream(const std::string& hash, std::ostream& output,
                                          ProgressCallback progress) override {
        // Create temporary file for retrieval
        auto tempPath =
            config_.storagePath / "temp" /
            (hash + "_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(tempPath.parent_path());

        // Retrieve to temporary file
        auto result = retrieve(hash, tempPath, progress);
        if (!result) {
            return result;
        }

        // Stream file to output
        std::ifstream tempFile(tempPath, std::ios::binary);
        if (!tempFile) {
            std::filesystem::remove(tempPath);
            return Result<RetrieveResult>(ErrorCode::Unknown);
        }

        output << tempFile.rdbuf();
        tempFile.close();

        // Clean up temporary file
        std::filesystem::remove(tempPath);

        return result;
    }

    // Memory-based store operation
    Result<StoreResult> storeBytes(std::span<const std::byte> data,
                                   const ContentMetadata& metadata) override {
        auto startTime = std::chrono::steady_clock::now();

        // Hash the data using stream interface
        hasher_->init();
        hasher_->update(data);
        std::string dataHash = hasher_->finalize();

        // For small data, store directly without chunking
        if (data.size() <= config_.chunkSize) {
            auto storeResult = storage_->store(dataHash, data);
            if (!storeResult) {
                return Result<StoreResult>(storeResult.error());
            }

            // Store metadata
            {
                std::unique_lock lock(metadataMutex_);
                metadataStore_[dataHash] = metadata;
            }

            updateStats(data.size(), 0, 0, 1, 1, 0, 0);

            auto endTime = std::chrono::steady_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

            return StoreResult{.contentHash = dataHash,
                               .bytesStored = data.size(),
                               .bytesDeduped = 0,
                               .duration = duration};
        }

        // For larger data, use in-memory chunking directly (avoid temp file I/O)
        auto chunks = chunker_->chunkData(data);
        spdlog::debug("Data chunked into {} chunks (in-memory)", chunks.size());

        // Store chunks and track deduplication
        uint64_t bytesStored = 0;
        uint64_t bytesDeduped = 0;

        // Begin reference counting transaction
        auto transaction = refCounter_->beginTransaction();

        for (const auto& chunk : chunks) {
            // Check if chunk already exists
            auto existsResult = storage_->exists(chunk.hash);
            if (!existsResult) {
                transaction->rollback();
                return Result<StoreResult>(existsResult.error());
            }

            if (existsResult.value()) {
                // Chunk already exists, just increment reference
                bytesDeduped += chunk.size;

                // Get on-disk (compressed) size for reference counting
                auto blockSizeResult = storage_->getBlockSize(chunk.hash);
                size_t compressedSize =
                    blockSizeResult ? static_cast<size_t>(blockSizeResult.value()) : chunk.size;

                auto incResult = refCounter_->increment(chunk.hash, compressedSize, chunk.size);
                if (!incResult) {
                    transaction->rollback();
                    return Result<StoreResult>(incResult.error());
                }
            } else {
                // Store new chunk
                auto storeResult = storage_->store(
                    chunk.hash, std::span<const std::byte>(chunk.data.data(), chunk.data.size()));
                if (!storeResult) {
                    transaction->rollback();
                    return Result<StoreResult>(storeResult.error());
                }

                bytesStored += chunk.size;

                // Get on-disk (compressed) size for reference counting
                auto blockSizeResult = storage_->getBlockSize(chunk.hash);
                size_t compressedSize =
                    blockSizeResult ? static_cast<size_t>(blockSizeResult.value()) : chunk.size;

                // Add initial reference with both compressed and uncompressed sizes
                auto incResult = refCounter_->increment(chunk.hash, compressedSize, chunk.size);
                if (!incResult) {
                    transaction->rollback();
                    return Result<StoreResult>(incResult.error());
                }
            }
        }

        // Create file info for manifest
        FileInfo fileInfo{.hash = dataHash,
                          .size = data.size(),
                          .mimeType = metadata.mimeType,
                          .createdAt = metadata.createdAt,
                          .originalName = metadata.name};

        // Convert Chunk vector to ChunkRef vector
        std::vector<manifest::ChunkRef> chunkRefs;
        chunkRefs.reserve(chunks.size());
        for (const auto& chunk : chunks) {
            chunkRefs.push_back({chunk.hash, chunk.offset, static_cast<uint32_t>(chunk.size)});
        }

        // Create and store manifest
        auto manifestResult = manifestManager_->createManifest(fileInfo, chunkRefs);
        if (!manifestResult) {
            transaction->rollback();
            return Result<StoreResult>(manifestResult.error());
        }

        auto& manifest = manifestResult.value();

        // Store manifest metadata
        {
            std::unique_lock lock(metadataMutex_);
            metadataStore_[dataHash] = metadata;
        }

        // Serialize and store manifest
        auto manifestData = manifestManager_->serialize(manifest);
        if (!manifestData) {
            transaction->rollback();
            return Result<StoreResult>(manifestData.error());
        }

        auto manifestHash = dataHash + ".manifest";
        auto manifestStoreResult =
            storage_->store(manifestHash, std::span<const std::byte>(manifestData.value()));
        if (!manifestStoreResult) {
            transaction->rollback();
            return Result<StoreResult>(manifestStoreResult.error());
        }

        // Commit transaction
        auto commitResult = transaction->commit();
        if (!commitResult) {
            return Result<StoreResult>(commitResult.error());
        }

        // Update statistics
        updateStats(bytesStored, bytesDeduped, 0, 1, 1, 0, 0);

        auto endTime = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        StoreResult result{.contentHash = dataHash,
                           .bytesStored = data.size(),
                           .bytesDeduped = bytesDeduped,
                           .duration = duration};

        spdlog::debug("Stored data with hash {}, dedup ratio: {:.2f}%", dataHash,
                      result.dedupRatio() * 100);

        return result;
    }

    Result<RawContent> retrieveRaw(const std::string& hash) override {
        auto storageRes = storage_->retrieveRaw(hash);
        if (!storageRes) {
            return storageRes.error();
        }
        auto rawObj = std::move(storageRes).value();
        RawContent out;
        out.data = std::move(rawObj.data);
        if (rawObj.header) {
            out.header = rawObj.header;
        }
        return out;
    }

    std::future<Result<RawContent>> retrieveRawAsync(const std::string& hash) override {
        return std::async(std::launch::async, [this, hash]() { return retrieveRaw(hash); });
    }

    // Memory-based retrieve operation
    Result<std::vector<std::byte>> retrieveBytes(const std::string& hash) override {
        // First try to retrieve manifest (chunked content)
        auto manifestHash = hash + ".manifest";
        auto manifestResult = storage_->retrieve(manifestHash);

        if (manifestResult) {
            // Content is chunked - reconstruct from manifest
            auto manifest =
                manifestManager_->deserialize(std::span<const std::byte>(manifestResult.value()));
            if (!manifest) {
                return manifest.error();
            }

            // Reconstruct content in memory
            std::vector<std::byte> reconstructed;
            reconstructed.reserve(manifest.value().fileSize);

            for (const auto& chunk : manifest.value().chunks) {
                auto chunkData = storage_->retrieve(chunk.hash);
                if (!chunkData) {
                    spdlog::warn("Failed to retrieve chunk {} for hash {}", chunk.hash, hash);
                    return chunkData.error();
                }
                reconstructed.insert(reconstructed.end(), chunkData.value().begin(),
                                     chunkData.value().end());
            }

            updateStats(0, 0, reconstructed.size(), 0, 0, 1, 0);
            return reconstructed;
        }

        // Fallback: try direct retrieval (small unchunked content)
        auto dataResult = storage_->retrieve(hash);
        if (!dataResult) {
            return dataResult.error();
        }
        auto data = std::move(dataResult.value());
        updateStats(0, 0, data.size(), 0, 0, 1, 0);
        return data;
    }

    // Check if content exists
    Result<bool> exists(const std::string& hash) const override {
        // First check for manifest (chunked content)
        // Any errors here are treated as "not found" to keep exists() lenient.
        if (auto manifestRes = storage_->exists(hash + ".manifest"); manifestRes) {
            if (manifestRes.value()) {
                return true;
            }
        }

        // Fallback: check for direct object (small unchunked items)
        auto objectRes = storage_->exists(hash);
        if (!objectRes) {
            // Treat invalid input as "not found" rather than surfacing an error.
            // This matches the typical semantics of an existence check and
            // avoids failing callers that probe with non-canonical ids.
            if (objectRes.error().code == ErrorCode::InvalidArgument) {
                return false;
            }
            // For other error conditions, propagate the error.
            return objectRes.error();
        }
        return objectRes.value();
    }

    // Remove content
    Result<bool> remove(const std::string& hash) override {
        // Retrieve manifest first
        auto manifestHash = hash + ".manifest";
        auto manifestResult = storage_->retrieve(manifestHash);
        if (!manifestResult) {
            return Result<bool>(false); // Not found
        }

        // Deserialize manifest
        auto manifest =
            manifestManager_->deserialize(std::span<const std::byte>(manifestResult.value()));
        if (!manifest) {
            return Result<bool>(manifest.error());
        }

        // Begin transaction
        auto transaction = refCounter_->beginTransaction();

        // Decrement references for all chunks
        for (const auto& chunk : manifest.value().chunks) {
            auto decResult = refCounter_->decrement(chunk.hash);
            if (!decResult) {
                transaction->rollback();
                return Result<bool>(decResult.error());
            }
        }

        // Remove manifest
        auto removeResult = storage_->remove(manifestHash);
        if (!removeResult) {
            transaction->rollback();
            return Result<bool>(removeResult.error());
        }

        // Remove metadata
        {
            std::unique_lock lock(metadataMutex_);
            metadataStore_.erase(hash);
        }

        // Commit transaction
        auto commitResult = transaction->commit();
        if (!commitResult) {
            return Result<bool>(commitResult.error());
        }

        updateStats(0, 0, 0, 0, 0, 0, 1);

        spdlog::info("Removed content with hash {}", hash);

        return Result<bool>(true);
    }

    // Get metadata
    Result<ContentMetadata> getMetadata(const std::string& hash) const override {
        std::shared_lock lock(metadataMutex_);
        auto it = metadataStore_.find(hash);
        if (it != metadataStore_.end()) {
            return Result<ContentMetadata>(it->second);
        }
        return Result<ContentMetadata>(ErrorCode::FileNotFound);
    }

    // Update metadata
    Result<void> updateMetadata(const std::string& hash, const ContentMetadata& metadata) override {
        // Verify content exists
        auto existsResult = exists(hash);
        if (!existsResult) {
            return Result<void>(existsResult.error());
        }

        if (!existsResult.value()) {
            return Result<void>(ErrorCode::FileNotFound);
        }

        std::unique_lock lock(metadataMutex_);
        metadataStore_[hash] = metadata;

        return Result<void>();
    }

    // Batch store operation
    std::vector<Result<StoreResult>>
    storeBatch(const std::vector<std::filesystem::path>& paths,
               const std::vector<ContentMetadata>& metadata) override {
        std::vector<Result<StoreResult>> results;
        results.reserve(paths.size());

        for (size_t i = 0; i < paths.size(); ++i) {
            ContentMetadata meta = (i < metadata.size()) ? metadata[i] : ContentMetadata{};
            results.push_back(store(paths[i], meta, nullptr));
        }

        return results;
    }

    // Batch remove operation
    std::vector<Result<bool>> removeBatch(const std::vector<std::string>& hashes) override {
        std::vector<Result<bool>> results;
        results.reserve(hashes.size());

        for (const auto& hash : hashes) {
            results.push_back(remove(hash));
        }

        return results;
    }

    ContentStoreStats getStats() const override {
        std::shared_lock lock(statsMutex_);
        ContentStoreStats currentStats = stats_;

        if (currentStats.totalObjects == 0 || currentStats.totalBytes == 0) {
            auto storageStats = storage_->getStats();
            if (storageStats.totalObjects > 0) {
                currentStats.totalObjects = storageStats.totalObjects.load();
                currentStats.totalBytes = storageStats.totalBytes.load();
            }
        }

        // Get unique block count and total bytes from reference counter
        if (refCounter_) {
            auto refStats = refCounter_->getStats();
            if (refStats) {
                currentStats.uniqueBlocks = refStats.value().totalBlocks;
                // Use ref counter's byte count if we don't have one
                if (currentStats.totalBytes == 0 && refStats.value().totalBytes > 0) {
                    currentStats.totalBytes = refStats.value().totalBytes;
                }
                // Get uncompressed bytes from persistent storage
                currentStats.totalUncompressedBytes = refStats.value().totalUncompressedBytes;
            }
        }

        return currentStats;
    }

    // Check health
    HealthStatus checkHealth() const override {
        HealthStatus status;
        status.lastCheck = std::chrono::system_clock::now();

        // Check storage engine
        [[maybe_unused]] auto storageStats = storage_->getStats();

        // Check available space
        auto spaceInfo = std::filesystem::space(config_.storagePath);
        uint64_t availableBytes = spaceInfo.available;
        uint64_t totalBytes = spaceInfo.capacity;

        if (availableBytes < 1024 * 1024 * 100) { // Less than 100MB
            status.errors.push_back("Critical: Less than 100MB storage available");
            status.isHealthy = false;
        } else if (availableBytes < totalBytes * 0.1) { // Less than 10%
            status.warnings.push_back("Warning: Less than 10% storage available");
        }

        // Check if storage path is accessible
        if (!std::filesystem::exists(config_.storagePath)) {
            status.errors.push_back("Storage path does not exist");
            status.isHealthy = false;
        }

        if (status.isHealthy && status.warnings.empty()) {
            status.status = "Healthy";
        } else if (status.isHealthy) {
            status.status = "Healthy with warnings";
        } else {
            status.status = "Unhealthy";
        }

        return status;
    }

    // Verify integrity
    Result<void> verify([[maybe_unused]] ProgressCallback progress) override {
        spdlog::info("Starting content store verification");

        // TODO: Implement full verification
        // This would involve:
        // 1. Checking all manifests
        // 2. Verifying all chunks exist and match their hashes
        // 3. Checking reference counts

        return Result<void>();
    }

    // Compact storage
    Result<void> compact([[maybe_unused]] ProgressCallback progress) override {
        spdlog::info("Starting content store compaction");

        // Delegate to storage engine (cast to concrete type)
        if (auto* concreteStorage = dynamic_cast<storage::StorageEngine*>(storage_.get())) {
            return concreteStorage->compact();
        }

        return Result<void>(
            Error{ErrorCode::NotSupported, "Compact not supported by this storage engine"});
    }

    // Garbage collection
    Result<void> garbageCollect([[maybe_unused]] ProgressCallback progress) override {
        spdlog::info("Starting garbage collection");

        // Cast to concrete types for GarbageCollector access
        auto* concreteRefCounter = dynamic_cast<storage::ReferenceCounter*>(refCounter_.get());
        auto* concreteStorage = dynamic_cast<storage::StorageEngine*>(storage_.get());

        if (!concreteRefCounter || !concreteStorage) {
            spdlog::warn("Garbage collection not supported: requires concrete ReferenceCounter and "
                         "StorageEngine implementations");
            return Result<void>();
        }

        // Create GarbageCollector and run collection
        storage::GCOptions gcOptions;
        gcOptions.maxBlocksPerRun = 1000;
        gcOptions.minAgeSeconds = static_cast<size_t>(config_.gcInterval.count());
        gcOptions.dryRun = false;

        if (progress) {
            gcOptions.progressCallback = [&progress](const std::string& /*hash*/, size_t count) {
                Progress p;
                p.bytesProcessed = count;
                p.totalBytes = 0; // Unknown total
                p.percentage = 0.0;
                p.currentOperation = "garbage_collection";
                progress(p);
            };
        }

        auto gc = storage::createGarbageCollector(*concreteRefCounter, *concreteStorage);
        auto result = gc->collect(gcOptions);

        if (!result) {
            spdlog::error("Garbage collection failed");
            return Result<void>(result.error());
        }

        const auto& stats = result.value();
        spdlog::info(
            "Garbage collection complete: {} blocks scanned, {} deleted, {} bytes reclaimed",
            stats.blocksScanned, stats.blocksDeleted, stats.bytesReclaimed);

        // Update our stats
        if (stats.blocksDeleted > 0) {
            updateStats(0, 0, 0, 0, 0, 0, stats.blocksDeleted);
        }

        return Result<void>();
    }

private:
    // Components
    std::shared_ptr<storage::IStorageEngine> storage_;
    std::shared_ptr<chunking::IChunker> chunker_;
    std::shared_ptr<crypto::IHasher> hasher_;
    std::shared_ptr<manifest::IManifestManager> manifestManager_;
    std::shared_ptr<storage::IReferenceCounter> refCounter_;
    ContentStoreConfig config_;

    // Metadata storage (in-memory for now)
    mutable std::shared_mutex metadataMutex_;
    std::unordered_map<std::string, ContentMetadata> metadataStore_;

    // Statistics
    mutable std::shared_mutex statsMutex_;
    ContentStoreStats stats_;

    // Update statistics
    void updateStats(uint64_t bytesStored, uint64_t bytesDeduped,
                     [[maybe_unused]] uint64_t bytesRetrieved, uint64_t objectsAdded,
                     uint64_t storeOps, uint64_t retrieveOps, uint64_t deleteOps) {
        std::unique_lock lock(statsMutex_);
        stats_.totalBytes += bytesStored;
        stats_.deduplicatedBytes += bytesDeduped;
        stats_.totalObjects += objectsAdded;
        stats_.storeOperations += storeOps;
        stats_.retrieveOperations += retrieveOps;
        stats_.deleteOperations += deleteOps;
        stats_.lastOperation = std::chrono::system_clock::now();
    }
};

// Factory function implementation
std::unique_ptr<IContentStore> createContentStore(
    std::shared_ptr<storage::IStorageEngine> storage, std::shared_ptr<chunking::IChunker> chunker,
    std::shared_ptr<crypto::IHasher> hasher,
    std::shared_ptr<manifest::IManifestManager> manifestManager,
    std::shared_ptr<storage::IReferenceCounter> refCounter, const ContentStoreConfig& config) {
    return std::make_unique<ContentStore>(std::move(storage), std::move(chunker), std::move(hasher),
                                          std::move(manifestManager), std::move(refCounter),
                                          config);
}

} // namespace yams::api
