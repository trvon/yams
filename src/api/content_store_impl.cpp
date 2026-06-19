#include <yams/api/content_store.h>
#include <yams/api/content_store_error.h>
#include <yams/api/progress_reporter.h>
#include <yams/chunking/chunker.h>
#include <yams/common/fs_utils.h>
#include <yams/crypto/hasher.h>
#include <yams/manifest/manifest_manager.h>
#include <yams/profiling.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/storage_engine.h>

#include <spdlog/spdlog.h>

#include <array>
#include <atomic>
#include <charconv>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace yams::api {

namespace {

constexpr std::string_view kTrustedHashHintTag = "__yams_trusted_hash_hint";
constexpr std::string_view kHashHintMtimeNsTag = "__yams_hash_hint_mtime_ns";

std::optional<int64_t> parseInt64(std::string_view value) {
    int64_t out = 0;
    const auto* begin = value.data();
    const auto* end = begin + value.size();
    auto [ptr, ec] = std::from_chars(begin, end, out);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return out;
}

int64_t fileTimeNs(const std::filesystem::file_time_type& time) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count();
}

ContentMetadata sanitizeStoredMetadata(ContentMetadata metadata, const std::string& hash,
                                       uint64_t size) {
    metadata.contentHash = hash;
    metadata.size = size;
    metadata.tags.erase(std::string{kTrustedHashHintTag});
    metadata.tags.erase(std::string{kHashHintMtimeNsTag});
    return metadata;
}

struct AtomicContentStorePhaseTiming {
    std::atomic<std::uint64_t> calls{0};
    std::atomic<std::uint64_t> totalMs{0};
    std::atomic<std::uint64_t> maxMs{0};
};

constexpr std::array<std::string_view, 9> kContentStorePhaseNames{
    "file_stat",      "hash",       "chunk_file",     "chunk_store_refs", "manifest_create",
    "manifest_store", "ref_commit", "metadata_stats", "store_total"};

std::array<AtomicContentStorePhaseTiming, kContentStorePhaseNames.size()>& contentStoreTimings() {
    static std::array<AtomicContentStorePhaseTiming, kContentStorePhaseNames.size()> timings;
    return timings;
}

std::optional<std::size_t> contentStorePhaseIndex(std::string_view phase) {
    for (std::size_t i = 0; i < kContentStorePhaseNames.size(); ++i) {
        if (kContentStorePhaseNames[i] == phase) {
            return i;
        }
    }
    return std::nullopt;
}

void updateMaxRelaxed(std::atomic<std::uint64_t>& target, std::uint64_t value) {
    auto current = target.load(std::memory_order_relaxed);
    while (current < value &&
           !target.compare_exchange_weak(current, value, std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
    }
}

void recordContentStorePhase(std::string_view phase, std::chrono::steady_clock::time_point start) {
    const auto index = contentStorePhaseIndex(phase);
    if (!index) {
        return;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
    const auto ms = static_cast<std::uint64_t>(std::max<int64_t>(0, elapsed));
    auto& timing = contentStoreTimings()[*index];
    timing.calls.fetch_add(1, std::memory_order_relaxed);
    timing.totalMs.fetch_add(ms, std::memory_order_relaxed);
    updateMaxRelaxed(timing.maxMs, ms);
}

} // namespace

void resetContentStorePhaseTimings() {
    for (auto& timing : contentStoreTimings()) {
        timing.calls.store(0, std::memory_order_relaxed);
        timing.totalMs.store(0, std::memory_order_relaxed);
        timing.maxMs.store(0, std::memory_order_relaxed);
    }
}

std::unordered_map<std::string, ContentStorePhaseTiming> getContentStorePhaseTimingsSnapshot() {
    std::unordered_map<std::string, ContentStorePhaseTiming> snapshot;
    snapshot.reserve(kContentStorePhaseNames.size());
    const auto& timings = contentStoreTimings();
    for (std::size_t i = 0; i < kContentStorePhaseNames.size(); ++i) {
        snapshot.emplace(std::string(kContentStorePhaseNames[i]),
                         ContentStorePhaseTiming{timings[i].calls.load(std::memory_order_relaxed),
                                                 timings[i].totalMs.load(std::memory_order_relaxed),
                                                 timings[i].maxMs.load(std::memory_order_relaxed)});
    }
    return snapshot;
}

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

        const auto fileStatStart = std::chrono::steady_clock::now();
        // Validate file exists
        if (!std::filesystem::exists(path)) {
            spdlog::error("File not found: {}", path.string());
            return Result<StoreResult>(ErrorCode::FileNotFound);
        }

        // Get file info
        uint64_t fileSize = std::filesystem::file_size(path);
        recordContentStorePhase("file_stat", fileStatStart);
        ProgressReporter reporter(fileSize);
        if (progress) {
            reporter.setCallback(progress);
        }

        const auto hashStart = std::chrono::steady_clock::now();
        std::string fileHash;
        const auto trustedHint = metadata.tags.find(std::string{kTrustedHashHintTag});
        if (trustedHint != metadata.tags.end() && trustedHint->second == "1" &&
            !metadata.contentHash.empty() && metadata.size == fileSize) {
            bool mtimeMatches = true;
            const auto mtimeIt = metadata.tags.find(std::string{kHashHintMtimeNsTag});
            if (mtimeIt != metadata.tags.end()) {
                std::error_code ec;
                const auto currentMtime = std::filesystem::last_write_time(path, ec);
                const auto expectedMtime = parseInt64(mtimeIt->second);
                mtimeMatches = !ec && expectedMtime && *expectedMtime == fileTimeNs(currentMtime);
            }
            if (mtimeMatches) {
                fileHash = metadata.contentHash;
                spdlog::debug("Reusing precomputed file hash: {} for {}", fileHash, path.string());
            }
        }
        if (fileHash.empty()) {
            // ContentStore is shared across daemon worker threads. Hash with a per-operation hasher
            // instead of serializing all ingest workers behind the injected mutable hasher.
            auto fileHasher = crypto::createSHA256Hasher();
            fileHash = fileHasher->hashFile(path);
        }
        recordContentStorePhase("hash", hashStart);
        spdlog::debug("File hash: {} for {}", fileHash, path.string());

        // Create file info
        FileInfo fileInfo{.hash = fileHash,
                          .size = fileSize,
                          .mimeType = metadata.mimeType,
                          .createdAt = metadata.createdAt,
                          .originalName =
                              metadata.name.empty() ? path.filename().string() : metadata.name};

        // Chunk the file
        const auto chunkStart = std::chrono::steady_clock::now();
        std::vector<chunking::Chunk> chunks;
        try {
            chunks = chunker_->chunkFile(path);
        } catch (const std::exception& e) {
            spdlog::error("Failed to chunk file {}: {}", path.string(), e.what());
            return Result<StoreResult>(
                Error{ErrorCode::InternalError, "Failed to chunk file: " + std::string(e.what())});
        } catch (...) {
            spdlog::error("Failed to chunk file {}: unknown error", path.string());
            return Result<StoreResult>(
                Error{ErrorCode::InternalError, "Failed to chunk file: unknown error"});
        }
        recordContentStorePhase("chunk_file", chunkStart);
        spdlog::debug("File chunked into {} chunks", chunks.size());

        // Store chunks and track deduplication
        uint64_t bytesStored = 0;
        uint64_t bytesDeduped = 0;

        // Begin reference counting transaction
        auto transaction = refCounter_->beginTransaction();
        std::vector<std::string> storedChunks;

        auto rollbackStore = [&]() {
            transaction->rollback();
            // Chunk objects are content-addressed — do not delete by hash on rollback.
        };

        const auto chunkStoreStart = std::chrono::steady_clock::now();
        for (const auto& chunk : chunks) {
            // Check if chunk already exists
            auto existsResult = storage_->exists(chunk.hash);
            if (!existsResult) {
                rollbackStore();
                return Result<StoreResult>(existsResult.error());
            }

            if (existsResult.value()) {
                // Chunk already exists, just increment reference
                bytesDeduped += chunk.size;

                // Get on-disk (compressed) size for reference counting
                auto blockSizeResult = storage_->getBlockSize(chunk.hash);
                size_t compressedSize =
                    blockSizeResult ? static_cast<size_t>(blockSizeResult.value()) : chunk.size;

                transaction->increment(chunk.hash, compressedSize, chunk.size);
            } else {
                // Store new chunk
                auto storeResult = storage_->store(
                    chunk.hash, std::span<const std::byte>(chunk.data.data(), chunk.data.size()));
                if (!storeResult) {
                    rollbackStore();
                    return Result<StoreResult>(storeResult.error());
                }
                storedChunks.push_back(chunk.hash);

                bytesStored += chunk.size;

                // Get on-disk (compressed) size for reference counting
                auto blockSizeResult = storage_->getBlockSize(chunk.hash);
                size_t compressedSize =
                    blockSizeResult ? static_cast<size_t>(blockSizeResult.value()) : chunk.size;

                // Add initial reference with both compressed and uncompressed sizes
                transaction->increment(chunk.hash, compressedSize, chunk.size);
            }

            // Report progress
            reporter.addProgress(chunk.size);
        }
        recordContentStorePhase("chunk_store_refs", chunkStoreStart);

        const auto manifestCreateStart = std::chrono::steady_clock::now();
        // Convert Chunk vector to ChunkRef vector
        std::vector<manifest::ChunkRef> chunkRefs;
        chunkRefs.reserve(chunks.size());
        for (const auto& chunk : chunks) {
            chunkRefs.push_back({chunk.hash, chunk.offset, static_cast<uint32_t>(chunk.size)});
        }

        // Create and store manifest
        auto manifestResult = manifestManager_->createManifest(fileInfo, chunkRefs);
        if (!manifestResult) {
            rollbackStore();
            return Result<StoreResult>(manifestResult.error());
        }

        auto& manifest = manifestResult.value();

        // Store manifest
        auto manifestData = manifestManager_->serialize(manifest);
        if (!manifestData) {
            rollbackStore();
            return Result<StoreResult>(manifestData.error());
        }

        recordContentStorePhase("manifest_create", manifestCreateStart);

        const auto manifestStoreStart = std::chrono::steady_clock::now();
        auto manifestHash = fileHash + ".manifest";
        auto manifestStoreResult =
            storage_->store(manifestHash, std::span<const std::byte>(manifestData.value()));
        if (!manifestStoreResult) {
            if (reconcileAmbiguousManifestStore(manifestHash, manifestData.value(),
                                                manifestStoreResult.error())) {
                auto commitResult2 = transaction->commit();
                if (!commitResult2) {
                    transaction->rollback();
                    cleanupStoredObject(manifestHash);
                    cleanupStoredObjects(storedChunks);
                    return Result<StoreResult>(commitResult2.error());
                }
                {
                    std::unique_lock lock(metadataMutex_);
                    metadataStore_[fileHash] = sanitizeStoredMetadata(metadata, fileHash, fileSize);
                }
                updateStats(bytesStored, bytesDeduped, 0, 1, 1, 0, 0);
                auto endTime = std::chrono::steady_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
                return StoreResult{.contentHash = fileHash,
                                   .bytesStored = fileSize,
                                   .bytesDeduped = bytesDeduped,
                                   .duration = duration};
            }
            rollbackStore();
            return Result<StoreResult>(manifestStoreResult.error());
        }
        recordContentStorePhase("manifest_store", manifestStoreStart);

        // Commit transaction
        const auto refCommitStart = std::chrono::steady_clock::now();
        auto commitResult = transaction->commit();
        if (!commitResult) {
            transaction->rollback();
            cleanupStoredObject(manifestHash);
            return Result<StoreResult>(commitResult.error());
        }
        recordContentStorePhase("ref_commit", refCommitStart);

        const auto metadataStatsStart = std::chrono::steady_clock::now();
        // Store manifest metadata only after durable data and refs commit.
        {
            std::unique_lock lock(metadataMutex_);
            metadataStore_[fileHash] = sanitizeStoredMetadata(metadata, fileHash, fileSize);
        }

        // Update statistics
        updateStats(bytesStored, bytesDeduped, 0, 1, 1, 0, 0);
        recordContentStorePhase("metadata_stats", metadataStatsStart);

        auto endTime = std::chrono::steady_clock::now();
        recordContentStorePhase("store_total", startTime);
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
        yams::common::ensureDirectories(tempPath.parent_path());

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
        yams::common::ensureDirectories(tempPath.parent_path());

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

        const std::string dataHash = crypto::SHA256Hasher::hash(data);

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

        // For larger data, use in-memory chunking directly (avoid temp file I/O).
        // chunkDataLazy produces chunks with offset/size/hash but no data copy —
        // we reference the source buffer directly for storage.
        auto chunks = chunker_->chunkDataLazy(data);
        spdlog::debug("Data chunked into {} chunks (in-memory)", chunks.size());

        // Store chunks and track deduplication
        uint64_t bytesStored = 0;
        uint64_t bytesDeduped = 0;

        // Begin reference counting transaction
        auto transaction = refCounter_->beginTransaction();
        std::vector<std::string> storedChunks;

        auto rollbackStore = [&]() {
            transaction->rollback();
            // Chunk objects are content-addressed — do not delete by hash on rollback.
        };

        for (const auto& chunk : chunks) {
            // Check if chunk already exists
            auto existsResult = storage_->exists(chunk.hash);
            if (!existsResult) {
                rollbackStore();
                return Result<StoreResult>(existsResult.error());
            }

            if (existsResult.value()) {
                // Chunk already exists, just increment reference
                bytesDeduped += chunk.size;

                // Get on-disk (compressed) size for reference counting
                auto blockSizeResult = storage_->getBlockSize(chunk.hash);
                size_t compressedSize =
                    blockSizeResult ? static_cast<size_t>(blockSizeResult.value()) : chunk.size;

                transaction->increment(chunk.hash, compressedSize, chunk.size);
            } else {
                // Store new chunk — use source buffer subspan (lazy chunks have no data copy)
                auto storeResult =
                    storage_->store(chunk.hash, data.subspan(chunk.offset, chunk.size));
                if (!storeResult) {
                    rollbackStore();
                    return Result<StoreResult>(storeResult.error());
                }
                storedChunks.push_back(chunk.hash);

                bytesStored += chunk.size;

                // Get on-disk (compressed) size for reference counting
                auto blockSizeResult = storage_->getBlockSize(chunk.hash);
                size_t compressedSize =
                    blockSizeResult ? static_cast<size_t>(blockSizeResult.value()) : chunk.size;

                // Add initial reference with both compressed and uncompressed sizes
                transaction->increment(chunk.hash, compressedSize, chunk.size);
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
            rollbackStore();
            return Result<StoreResult>(manifestResult.error());
        }

        auto& manifest = manifestResult.value();

        // Serialize and store manifest
        auto manifestData = manifestManager_->serialize(manifest);
        if (!manifestData) {
            rollbackStore();
            return Result<StoreResult>(manifestData.error());
        }

        auto manifestHash = dataHash + ".manifest";
        auto manifestStoreResult =
            storage_->store(manifestHash, std::span<const std::byte>(manifestData.value()));
        if (!manifestStoreResult) {
            if (reconcileAmbiguousManifestStore(manifestHash, manifestData.value(),
                                                manifestStoreResult.error())) {
                auto commitResult2 = transaction->commit();
                if (!commitResult2) {
                    transaction->rollback();
                    cleanupStoredObject(manifestHash);
                    cleanupStoredObjects(storedChunks);
                    return Result<StoreResult>(commitResult2.error());
                }
                {
                    std::unique_lock lock(metadataMutex_);
                    metadataStore_[dataHash] = metadata;
                }
                updateStats(bytesStored, bytesDeduped, 0, 1, 1, 0, 0);
                auto endTime = std::chrono::steady_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
                return StoreResult{.contentHash = dataHash,
                                   .bytesStored = data.size(),
                                   .bytesDeduped = bytesDeduped,
                                   .duration = duration};
            }
            rollbackStore();
            return Result<StoreResult>(manifestStoreResult.error());
        }

        // Commit transaction
        auto commitResult = transaction->commit();
        if (!commitResult) {
            transaction->rollback();
            cleanupStoredObject(manifestHash);
            return Result<StoreResult>(commitResult.error());
        }

        // Store manifest metadata only after durable data and refs commit.
        {
            std::unique_lock lock(metadataMutex_);
            metadataStore_[dataHash] = metadata;
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
        return retrieveBytesBounded(hash, std::numeric_limits<std::size_t>::max());
    }

    Result<std::vector<std::byte>> retrieveBytesPrefix(const std::string& hash,
                                                       std::size_t maxBytes) override {
        return retrieveBytesBounded(hash, maxBytes);
    }

    Result<std::vector<std::byte>> retrieveBytesBounded(const std::string& hash,
                                                        std::size_t maxBytes) {
        YAMS_ZONE_SCOPED_N("content_store::retrieve_bytes");
        if (maxBytes == 0) {
            return std::vector<std::byte>{};
        }
        auto manifestHash = hash + ".manifest";
        auto manifestResult = storage_->retrieve(manifestHash);

        if (manifestResult) {
            auto manifest =
                manifestManager_->deserialize(std::span<const std::byte>(manifestResult.value()));
            if (!manifest) {
                return manifest.error();
            }

            std::vector<std::byte> reconstructed;
            const auto fileSize = manifest.value().fileSize;
            const std::size_t cap =
                std::min<std::size_t>(maxBytes, static_cast<std::size_t>(fileSize));
            reconstructed.reserve(cap);

            for (const auto& chunk : manifest.value().chunks) {
                if (reconstructed.size() >= maxBytes) {
                    break;
                }
                auto chunkData = storage_->retrieve(chunk.hash);
                if (!chunkData) {
                    spdlog::warn("Failed to retrieve chunk {} for hash {}", chunk.hash, hash);
                    return chunkData.error();
                }
                const auto& bytes = chunkData.value();
                const std::size_t remaining = maxBytes - reconstructed.size();
                const std::size_t take = std::min<std::size_t>(bytes.size(), remaining);
                reconstructed.insert(reconstructed.end(), bytes.begin(), bytes.begin() + take);
            }

            updateStats(0, 0, reconstructed.size(), 0, 0, 1, 0);
            return reconstructed;
        }

        auto dataResult = storage_->retrieve(hash);
        if (!dataResult) {
            return dataResult.error();
        }
        auto data = std::move(dataResult.value());
        if (data.size() > maxBytes) {
            data.resize(maxBytes);
        }
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

        // Commit the decrement so subsequent ref-count probes observe the new counts.
        // Physical chunk eviction is the garbage collector's job for blocks at zero refs.
        auto commitResult = transaction->commit();
        if (!commitResult) {
            return Result<bool>(commitResult.error());
        }

        // The manifest (and content metadata) must only be removed when the last reference to
        // this content is dropped. If any chunk is still referenced by another document sharing
        // the same hash, keep the manifest so the surviving document remains retrievable.
        bool stillReferenced = false;
        for (const auto& chunk : manifest.value().chunks) {
            auto refCount = refCounter_->getRefCount(chunk.hash);
            if (refCount && refCount.value() > 0) {
                stillReferenced = true;
                break;
            }
        }
        if (stillReferenced) {
            spdlog::debug("Dropped one reference to content {}; still referenced, manifest kept",
                          hash);
            return Result<bool>(true);
        }

        // Last reference dropped: remove manifest and metadata.
        auto removeResult = storage_->remove(manifestHash);
        if (!removeResult) {
            return Result<bool>(removeResult.error());
        }
        {
            std::unique_lock lock(metadataMutex_);
            metadataStore_.erase(hash);
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

        constexpr uint64_t kCriticalFreeBytes = 100ULL * 1024ULL * 1024ULL;
        if (availableBytes < kCriticalFreeBytes) { // Less than 100MB
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
    Result<void> verify(ProgressCallback progress) override {
        spdlog::info("Starting content store verification");

        size_t checkedItems = 0;
        size_t errors = 0;

        auto fail = [&errors](const std::string& message) {
            ++errors;
            spdlog::warn("Content store verification failed: {}", message);
        };

        if (auto* concreteStorage = dynamic_cast<storage::StorageEngine*>(storage_.get())) {
            auto storageVerify = concreteStorage->verify();
            if (!storageVerify) {
                fail("storage engine object verification failed: " + storageVerify.error().message);
            }
        }

        if (auto* concreteRefCounter =
                dynamic_cast<storage::ReferenceCounter*>(refCounter_.get())) {
            auto refIntegrity = concreteRefCounter->verifyIntegrity();
            if (!refIntegrity || !refIntegrity.value()) {
                fail("reference counter integrity check failed");
            }
        }

        std::vector<std::string> knownHashes;
        {
            std::shared_lock lock(metadataMutex_);
            knownHashes.reserve(metadataStore_.size());
            for (const auto& [hash, _] : metadataStore_) {
                knownHashes.push_back(hash);
            }
        }

        for (const auto& hash : knownHashes) {
            ++checkedItems;
            if (progress) {
                Progress p;
                p.bytesProcessed = checkedItems;
                p.totalBytes = knownHashes.size();
                p.percentage = knownHashes.empty() ? 100.0
                                                   : (static_cast<double>(checkedItems) * 100.0 /
                                                      static_cast<double>(knownHashes.size()));
                p.currentOperation = "verify";
                progress(p);
            }

            const auto manifestHash = hash + ".manifest";
            auto manifestExists = storage_->exists(manifestHash);
            if (!manifestExists) {
                fail("manifest existence check failed for " + hash + ": " +
                     manifestExists.error().message);
                continue;
            }

            if (!manifestExists.value()) {
                auto direct = storage_->retrieve(hash);
                if (!direct) {
                    fail("direct object missing for " + hash + ": " + direct.error().message);
                    continue;
                }
                const auto actualHash = crypto::SHA256Hasher::hash(
                    std::span<const std::byte>(direct.value().data(), direct.value().size()));
                if (actualHash != hash) {
                    fail("direct object hash mismatch for " + hash + ": calculated " + actualHash);
                }
                continue;
            }

            auto manifestBytes = storage_->retrieve(manifestHash);
            if (!manifestBytes) {
                fail("manifest retrieve failed for " + hash + ": " + manifestBytes.error().message);
                continue;
            }

            auto manifest = manifestManager_->deserialize(std::span<const std::byte>(
                manifestBytes.value().data(), manifestBytes.value().size()));
            if (!manifest) {
                fail("manifest deserialize failed for " + hash + ": " + manifest.error().message);
                continue;
            }

            auto validManifest = manifestManager_->validateManifest(manifest.value());
            if (!validManifest || !validManifest.value()) {
                fail("manifest validation failed for " + hash);
                continue;
            }

            if (manifest.value().fileHash != hash) {
                fail("manifest file hash mismatch for " + hash + ": manifest has " +
                     manifest.value().fileHash);
                continue;
            }

            std::vector<std::byte> reconstructed;
            if (manifest.value().fileSize <=
                static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
                reconstructed.reserve(static_cast<size_t>(manifest.value().fileSize));
            }

            for (const auto& chunk : manifest.value().chunks) {
                auto exists = storage_->exists(chunk.hash);
                if (!exists || !exists.value()) {
                    fail("chunk missing for " + hash + ": " + chunk.hash);
                    continue;
                }

                auto chunkBytes = storage_->retrieve(chunk.hash);
                if (!chunkBytes) {
                    fail("chunk retrieve failed for " + chunk.hash + ": " +
                         chunkBytes.error().message);
                    continue;
                }
                if (chunkBytes.value().size() != chunk.size) {
                    fail("chunk size mismatch for " + chunk.hash);
                    continue;
                }

                const auto actualChunkHash = crypto::SHA256Hasher::hash(std::span<const std::byte>(
                    chunkBytes.value().data(), chunkBytes.value().size()));
                if (actualChunkHash != chunk.hash) {
                    fail("chunk hash mismatch for " + chunk.hash + ": calculated " +
                         actualChunkHash);
                    continue;
                }

                auto refCount = refCounter_->getRefCount(chunk.hash);
                if (!refCount || refCount.value() == 0) {
                    fail("chunk has no reference count for " + chunk.hash);
                    continue;
                }

                reconstructed.insert(reconstructed.end(), chunkBytes.value().begin(),
                                     chunkBytes.value().end());
            }

            if (reconstructed.size() != manifest.value().fileSize) {
                fail("reconstructed size mismatch for " + hash);
                continue;
            }

            const auto reconstructedHash = crypto::SHA256Hasher::hash(
                std::span<const std::byte>(reconstructed.data(), reconstructed.size()));
            if (reconstructedHash != hash) {
                fail("reconstructed content hash mismatch for " + hash + ": calculated " +
                     reconstructedHash);
            }
        }

        spdlog::info("Content store verification complete: {} known objects checked, {} errors",
                     checkedItems, errors);
        return errors == 0 ? Result<void>{} : Result<void>(ErrorCode::CorruptedData);
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
        spdlog::debug("Starting garbage collection");

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
        spdlog::debug(
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

    void cleanupStoredObjects(std::span<const std::string> hashes) {
        for (auto it = hashes.rbegin(); it != hashes.rend(); ++it) {
            cleanupStoredObject(*it);
        }
    }

    void cleanupStoredObject(const std::string& hash) {
        auto removed = storage_->remove(hash);
        if (!removed) {
            spdlog::warn("Failed to clean up partial store object {}: {}", hash,
                         removed.error().message);
        }
    }

    bool reconcileAmbiguousManifestStore(const std::string& manifestHash,
                                         const std::vector<std::byte>& intendedBytes,
                                         const Error& storeError) {
        if (storeError.code != ErrorCode::NetworkError && storeError.code != ErrorCode::Timeout) {
            return false;
        }

        auto retrieved = storage_->retrieve(manifestHash);
        if (!retrieved) {
            return false;
        }

        if (retrieved.value().size() != intendedBytes.size() ||
            !std::equal(retrieved.value().begin(), retrieved.value().end(),
                        intendedBytes.begin())) {
            cleanupStoredObject(manifestHash);
            return false;
        }

        spdlog::info("Reconciled ambiguous manifest store for {}", manifestHash);
        return true;
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
