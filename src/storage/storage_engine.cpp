#include <spdlog/spdlog.h>
#include <yams/common/fs_utils.h>
#include <yams/compat/thread_stop_compat.h>
#include <yams/compression/compression_utils.h>
#include <yams/compression/compressor_interface.h>
#include <yams/core/assert.hpp>
#include <yams/core/atomic_utils.h>
#include <yams/crypto/hasher.h>
#include <yams/storage/storage_engine.h>
#if defined(YAMS_HAS_STD_FORMAT) && YAMS_HAS_STD_FORMAT
#include <format>
namespace yamsfmt = std;
#else
#include <spdlog/fmt/fmt.h>
namespace yamsfmt = fmt;
#endif

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <optional>
#include <random>
#include <shared_mutex>
#include <span>
#include <system_error>
#include <yams/compat/unistd.h>
#ifdef _WIN32
#include <windows.h> // CreateFileW / FlushFileBuffers (NOMINMAX/WIN32_LEAN_AND_MEAN set project-wide)
#endif

namespace yams::storage {

namespace {

bool isHexHash(std::string_view hash) noexcept {
    return hash.size() == HASH_STRING_SIZE &&
           std::ranges::all_of(hash, [](unsigned char c) { return std::isxdigit(c) != 0; });
}

std::string errnoMessage(int err) {
    return std::error_code(err, std::generic_category()).message();
}

bool isValidStorageKey(std::string_view key) noexcept {
    if (isHexHash(key)) {
        return true;
    }
    constexpr std::string_view manifestSuffix = ".manifest";
    if (!key.ends_with(manifestSuffix)) {
        return false;
    }
    return isHexHash(key.substr(0, key.size() - manifestSuffix.size()));
}

std::string storageHashFromObjectPath(const std::filesystem::path& objectsRoot,
                                      const std::filesystem::path& objectPath) {
    std::error_code ec;
    const auto relative = std::filesystem::relative(objectPath, objectsRoot, ec);
    if (ec || relative.empty()) {
        return {};
    }

    std::string hash;
    for (const auto& part : relative) {
        hash += part.string();
    }
    return hash;
}

std::optional<compression::CompressionHeader>
parseExactCompressionHeader(std::span<const std::byte> data) {
    auto parsed = compression::CompressionHeader::parse(data);
    if (!parsed) {
        return std::nullopt;
    }
    const auto& header = parsed.value();
    if (header.compressedSize >
        std::numeric_limits<size_t>::max() - compression::CompressionHeader::SIZE) {
        return std::nullopt;
    }
    const auto expectedSize =
        compression::CompressionHeader::SIZE + static_cast<size_t>(header.compressedSize);
    if (data.size() != expectedSize) {
        return std::nullopt;
    }
    return header;
}

Result<std::vector<std::byte>> bytesForContentHash(std::span<const std::byte> storedBytes) {
    auto header = parseExactCompressionHeader(storedBytes);
    if (!header) {
        return std::vector<std::byte>(storedBytes.begin(), storedBytes.end());
    }

    const auto compressedBytes = storedBytes.subspan(compression::CompressionHeader::SIZE,
                                                     static_cast<size_t>(header->compressedSize));
    if (compression::calculateCRC32(compressedBytes) != header->compressedCRC32) {
        return Error(ErrorCode::HashMismatch, "Compressed object CRC mismatch");
    }

    const auto algorithm = static_cast<compression::CompressionAlgorithm>(header->algorithm);
    auto compressor = compression::CompressionRegistry::instance().createCompressor(algorithm);
    if (!compressor) {
        return Error(ErrorCode::InvalidArgument, "Compressor not available for stored object");
    }

    auto decompressed = compressor->decompress(compressedBytes, header->uncompressedSize);
    if (!decompressed) {
        return decompressed.error();
    }
    if (decompressed.value().size() != header->uncompressedSize) {
        return Error(ErrorCode::CorruptedData, "Decompressed object size mismatch");
    }
    if (compression::calculateCRC32(std::span<const std::byte>(decompressed.value())) !=
        header->uncompressedCRC32) {
        return Error(ErrorCode::HashMismatch, "Decompressed object CRC mismatch");
    }

    return std::move(decompressed.value());
}

Result<size_t> cleanupStaleTempFiles(const std::filesystem::path& tempDir) {
    size_t cleanedCount = 0;
    std::error_code ec;

    if (!std::filesystem::exists(tempDir, ec)) {
        if (ec) {
            return Error{ErrorCode::IOError, "Failed to check temp directory: " + tempDir.string() +
                                                 ": " + ec.message()};
        }
        return cleanedCount;
    }

    for (const auto& entry : std::filesystem::directory_iterator(tempDir, ec)) {
        if (ec) {
            return Error{ErrorCode::IOError, "Failed to iterate temp directory: " +
                                                 tempDir.string() + ": " + ec.message()};
        }
        if (!entry.is_regular_file()) {
            continue;
        }

        const auto lastWrite = entry.last_write_time(ec);
        if (ec) {
            return Error{ErrorCode::IOError, "Failed to read temp file timestamp: " +
                                                 entry.path().string() + ": " + ec.message()};
        }

        const auto now = std::filesystem::file_time_type::clock::now();
        if (now - lastWrite <= std::chrono::hours(1)) {
            continue;
        }

        std::filesystem::remove(entry.path(), ec);
        if (ec) {
            return Error{ErrorCode::IOError, "Failed to remove temp file: " +
                                                 entry.path().string() + ": " + ec.message()};
        }
        ++cleanedCount;
    }

    return cleanedCount;
}

Result<size_t> pruneEmptyDirectories(const std::filesystem::path& root) {
    std::error_code ec;
    if (!std::filesystem::exists(root, ec)) {
        if (ec) {
            return Error{ErrorCode::IOError,
                         "Failed to check directory: " + root.string() + ": " + ec.message()};
        }
        return size_t{0};
    }

    std::vector<std::filesystem::path> directories;
    for (std::filesystem::recursive_directory_iterator it(root, ec), end; it != end;
         it.increment(ec)) {
        if (ec) {
            return Error{ErrorCode::IOError, "Failed to iterate directory tree: " + root.string() +
                                                 ": " + ec.message()};
        }
        if (it->is_directory()) {
            directories.push_back(it->path());
        }
    }

    std::ranges::sort(directories, [](const auto& lhs, const auto& rhs) {
        return lhs.native().size() > rhs.native().size();
    });

    size_t removedCount = 0;
    for (const auto& dir : directories) {
        const bool isEmpty = std::filesystem::is_empty(dir, ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         "Failed to inspect directory: " + dir.string() + ": " + ec.message()};
        }
        if (!isEmpty) {
            continue;
        }

        std::filesystem::remove(dir, ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         "Failed to prune directory: " + dir.string() + ": " + ec.message()};
        }
        ++removedCount;
    }

    return removedCount;
}

#ifdef YAMS_TESTING
std::atomic<size_t> gAtomicWriteFailureAfterBytes{std::numeric_limits<size_t>::max()};
std::atomic<bool> gFileOpenFailure{false};
std::atomic<bool> gRenameFailure{false};
#endif

} // namespace

// Constants
constexpr size_t TEMP_NAME_LENGTH = 16;
// Removed unused retry helpers; reintroduce if backoff is implemented

// Implementation details
struct StorageEngine::Impl {
    StorageConfig config;
    mutable std::shared_mutex globalMutex;
    mutable StorageStats stats;

    // Mutex pool for per-hash write synchronization
    struct MutexPool {
        std::vector<std::unique_ptr<std::mutex>> mutexes;

        explicit MutexPool(size_t size) {
            YAMS_ASSERT(size > 0, "mutexPoolSize must be positive");
            mutexes.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                mutexes.emplace_back(std::make_unique<std::mutex>());
            }
        }

        std::mutex& getMutex(std::string_view hash) {
            auto hashValue = std::hash<std::string_view>{}(hash);
            auto index = hashValue % mutexes.size();
            return *mutexes[index];
        }
    } writeMutexPool;

    explicit Impl(StorageConfig cfg)
        : config(std::move(cfg)), writeMutexPool(config.mutexPoolSize) {
        // Initialize storage directory
        std::error_code ec;
        if (!yams::common::ensureDirectories(config.basePath / "objects", ec)) {
            throw std::runtime_error(
                yamsfmt::format("Failed to create storage directory: {}", ec.message()));
        }

        if (!yams::common::ensureDirectories(config.basePath / "temp", ec)) {
            throw std::runtime_error(
                yamsfmt::format("Failed to create temp directory: {}", ec.message()));
        }
    }
};

StorageEngine::StorageEngine(StorageConfig config)
    : pImpl(std::make_unique<Impl>(std::move(config))) {
    spdlog::debug("Initialized storage engine at: {}", pImpl->config.basePath.string());
}

StorageEngine::~StorageEngine() = default;

StorageEngine::StorageEngine(StorageEngine&&) noexcept = default;
StorageEngine& StorageEngine::operator=(StorageEngine&&) noexcept = default;

std::filesystem::path StorageEngine::getObjectPath(std::string_view hash) const {
    // For manifest files, use a different path structure
    if (hash.ends_with(".manifest")) {
        // Store manifests in manifests/ab/cdef...manifest
        auto baseHash = hash.substr(0, hash.length() - 9); // Remove ".manifest"
        if (baseHash.length() < pImpl->config.shardDepth) {
            // DCHECK aborts in dev/CI; in release it compiles out and the throw
            // preserves backward-compatible catchable std::invalid_argument.
            YAMS_DCHECK(false, "Manifest hash too short for sharding");
            throw std::invalid_argument("Hash too short for sharding");
        }
        auto prefix = baseHash.substr(0, pImpl->config.shardDepth);
        auto suffix = baseHash.substr(pImpl->config.shardDepth);
        return pImpl->config.basePath / "manifests" / std::string(prefix) /
               (std::string(suffix) + ".manifest");
    }

    if (hash.length() < pImpl->config.shardDepth) {
        // DCHECK aborts in dev/CI; in release it compiles out and the throw
        // preserves backward-compatible catchable std::invalid_argument.
        YAMS_DCHECK(false, "Hash too short for sharding");
        throw std::invalid_argument("Hash too short for sharding");
    }

    // Create sharded path: objects/ab/cdef...
    auto prefix = hash.substr(0, pImpl->config.shardDepth);
    auto suffix = hash.substr(pImpl->config.shardDepth);

    return pImpl->config.basePath / "objects" / std::string(prefix) / std::string(suffix);
}

std::filesystem::path StorageEngine::getTempPath() const {
    // Temp directories can disappear under external cleanup of ephemeral stores (for example
    // /tmp cleanup between construction and a later write). Recreate it here instead of treating
    // that as a process-fatal invariant.
    std::error_code ec;
    if (!yams::common::ensureDirectories(pImpl->config.basePath / "temp", ec)) {
        throw std::runtime_error(
            yamsfmt::format("Failed to create temp directory: {}", ec.message()));
    }

    // Generate random temp filename using pre-computed hex table
    static thread_local std::random_device rd;
    static thread_local std::mt19937 gen(rd());
    static thread_local std::uniform_int_distribution<unsigned> dis(0, 15);
    static constexpr char kHexChars[] = "0123456789abcdef";

    std::string tempName;
    tempName.reserve(TEMP_NAME_LENGTH);

    for (size_t i = 0; i < TEMP_NAME_LENGTH; ++i) {
        tempName.push_back(kHexChars[dis(gen)]);
    }
    YAMS_DCHECK(tempName.size() == TEMP_NAME_LENGTH,
                "Temp name must be exactly TEMP_NAME_LENGTH chars");

    return pImpl->config.basePath / "temp" / tempName;
}

Result<void> StorageEngine::ensureDirectoryExists(const std::filesystem::path& path) const {
    std::error_code ec;
    if (!yams::common::ensureDirectories(path.parent_path(), ec)) {
        spdlog::error("Failed to create directory {}: {}", path.parent_path().string(),
                      ec.message());
        return Result<void>(ErrorCode::PermissionDenied);
    }

    return {};
}

Result<void> StorageEngine::atomicWrite(const std::filesystem::path& path,
                                        std::span<const std::byte> data) {
    // Ensure parent directory exists
    if (auto result = ensureDirectoryExists(path); !result) {
        return result;
    }

    // Write to temporary file
    auto tempPath = getTempPath();

#ifdef YAMS_TESTING
    if (gFileOpenFailure.load(std::memory_order_relaxed)) {
        return Error{ErrorCode::PermissionDenied, "Injected file open failure"};
    }
#endif

    {
        std::ofstream file(tempPath, std::ios::binary);
        if (!file) {
            spdlog::error("Failed to create temp file: {}", tempPath.string());
            return Result<void>(ErrorCode::PermissionDenied);
        }

#ifdef YAMS_TESTING
        const auto failAfter = gAtomicWriteFailureAfterBytes.load(std::memory_order_relaxed);
        if (failAfter != std::numeric_limits<size_t>::max() && failAfter < data.size()) {
            file.write(reinterpret_cast<const char*>(data.data()),
                       static_cast<std::streamsize>(failAfter));
            file.flush();
            file.close();
            std::filesystem::remove(tempPath);
            return Error{ErrorCode::WriteError, "Injected partial atomic write failure"};
        }
#endif

        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));
        if (!file) {
            std::filesystem::remove(tempPath);
            return Result<void>(ErrorCode::Unknown);
        }
    }

    // fsync temp file before rename: guarantees written data is durable
    // before the atomic rename commits. Without this, a crash between rename
    // and OS writeback loses data. SquirrelFS (2024) validates that
    // write→fsync→rename is crash-consistent on modern filesystems.
    if (pImpl->config.fsyncBeforeRename) {
#ifdef _WIN32
        // Windows has no fsync(); open the temp file and FlushFileBuffers to
        // force its contents to stable storage before the rename commits.
        HANDLE h = ::CreateFileW(tempPath.wstring().c_str(), GENERIC_READ | GENERIC_WRITE,
                                 FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
                                 OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h == INVALID_HANDLE_VALUE) {
            spdlog::warn("Failed to open temp file for fsync: {}", tempPath.string());
            std::filesystem::remove(tempPath);
            return Result<void>(ErrorCode::PermissionDenied);
        }
        if (!::FlushFileBuffers(h)) {
            spdlog::warn("FlushFileBuffers failed on {}", tempPath.string());
            ::CloseHandle(h);
            std::filesystem::remove(tempPath);
            return Result<void>(ErrorCode::IOError);
        }
        ::CloseHandle(h);
#else
        int fd = ::open(tempPath.c_str(), O_RDONLY);
        if (fd < 0) {
            spdlog::warn("Failed to open temp file for fsync: {} ({})", tempPath.string(),
                         errnoMessage(errno));
            std::filesystem::remove(tempPath);
            return Result<void>(ErrorCode::PermissionDenied);
        }
#if __APPLE__
        // F_FULLFSYNC is required on macOS for durable writes —
        // fsync() alone only flushes to the drive cache, not to
        // physical media. See Apple TN2250 and man fcntl.
        if (::fcntl(fd, F_FULLFSYNC) != 0) {
            spdlog::warn("F_FULLFSYNC failed on {}: {}", tempPath.string(), errnoMessage(errno));
            // Fall through to fsync as best-effort.
            if (::fsync(fd) != 0) {
                spdlog::warn("fsync failed on {}: {}", tempPath.string(), errnoMessage(errno));
                ::close(fd);
                std::filesystem::remove(tempPath);
                return Result<void>(ErrorCode::IOError);
            }
        }
#else
        if (::fsync(fd) != 0) {
            spdlog::warn("fsync failed on {}: {}", tempPath.string(), errnoMessage(errno));
            ::close(fd);
            std::filesystem::remove(tempPath);
            return Result<void>(ErrorCode::IOError);
        }
#endif
        ::close(fd);
#endif
    }

    // Temp file must exist and contain data before rename.
    YAMS_DCHECK(std::filesystem::exists(tempPath), "Temp file must exist after successful write");

    // Atomic rename
    std::error_code ec;
#ifdef YAMS_TESTING
    if (gRenameFailure.load(std::memory_order_relaxed)) {
        ec = std::make_error_code(std::errc::permission_denied);
    } else {
        std::filesystem::rename(tempPath, path, ec);
    }
#else
    std::filesystem::rename(tempPath, path, ec);
#endif

    if (ec) {
        // Clean up temp file
        std::filesystem::remove(tempPath);

        // Check if file already exists (not an error for content-addressed storage)
        if (std::filesystem::exists(path)) {
            return {};
        }

        spdlog::error("Failed to rename {} to {}: {}", tempPath.string(), path.string(),
                      ec.message());
        return Result<void>(ErrorCode::Unknown);
    }

    YAMS_DCHECK(std::filesystem::exists(path),
                "Target file must exist after successful atomic rename");
    return {};
}

std::filesystem::path StorageEngine::getBasePath() const {
    return pImpl->config.basePath;
}

Result<void> StorageEngine::store(std::string_view hash, std::span<const std::byte> data) {
    // Allow manifest keys (hash.manifest) and regular hashes. Both forms must be hex-only
    // to keep sharded paths below the storage root.
    if (!isValidStorageKey(hash)) {
        spdlog::error(
            "Invalid storage key for store: expected hex hash or hex hash manifest, got '{}'",
            hash);
        return Result<void>(ErrorCode::InvalidArgument);
    }

    auto objectPath = getObjectPath(hash);

    // Quick existence check without locking
    if (std::filesystem::exists(objectPath)) {
        spdlog::debug("Object {} already exists", hash);
        return {};
    }

    // Block compaction from pruning shard directories while a write is in flight.
    std::shared_lock<std::shared_mutex> storageLock(pImpl->globalMutex);

    // Acquire per-hash write lock
    auto& hashMutex = pImpl->writeMutexPool.getMutex(hash);
    std::lock_guard<std::mutex> lock(hashMutex);

    // Double-check after acquiring lock
    if (std::filesystem::exists(objectPath)) {
        return {};
    }

    // Perform atomic write
    auto result = atomicWrite(objectPath, data);

    if (result) {
        // Update statistics
        pImpl->stats.totalObjects.fetch_add(1);
        pImpl->stats.totalBytes.fetch_add(data.size());
        pImpl->stats.writeOperations.fetch_add(1);
        YAMS_DCHECK(std::filesystem::exists(objectPath),
                    "Stored object must exist on disk after successful write");
        spdlog::debug("Stored object {} ({} bytes)", hash, data.size());
    } else {
        pImpl->stats.failedOperations.fetch_add(1);
    }

    return result;
}

std::vector<Result<void>> StorageEngine::storeBatch(
    const std::vector<std::pair<std::string, std::vector<std::byte>>>& items) {
    if (items.empty())
        return {};

    // Reuse the caller and a bounded set of workers. Per-item std::async calls copy every payload
    // and create one thread per object even when the concurrency cap forces them into waves.
    const std::size_t workerCount =
        std::min(items.size(), std::max<std::size_t>(1, pImpl->config.maxConcurrentWriters));
    std::vector<Result<void>> results(items.size());
    std::atomic<std::size_t> next{0};
    auto writeNext = [&]() {
        for (;;) {
            const auto index = next.fetch_add(1, std::memory_order_relaxed);
            if (index >= items.size()) {
                return;
            }
            const auto& [hash, data] = items[index];
            results[index] = store(hash, std::span<const std::byte>(data.data(), data.size()));
        }
    };

    {
        std::vector<yams::compat::jthread> workers;
        workers.reserve(workerCount - 1);
        for (std::size_t i = 1; i < workerCount; ++i) {
            workers.emplace_back(writeNext);
        }
        writeNext();
    } // Join before results can be moved to the caller.

    return results;
}

Result<IStorageEngine::RawObject> StorageEngine::retrieveRaw(std::string_view hash) const {
    // Allow manifest keys (hash.manifest) and regular hashes. Both forms must be hex-only
    // to keep sharded paths below the storage root.
    if (!isValidStorageKey(hash)) {
        spdlog::error(
            "Invalid storage key for retrieve: expected hex hash or hex hash manifest, got '{}'",
            hash);
        return Result<IStorageEngine::RawObject>(ErrorCode::InvalidArgument);
    }

    auto objectPath = getObjectPath(hash);

    // Check existence
    if (!std::filesystem::exists(objectPath)) {
        return Result<IStorageEngine::RawObject>(ErrorCode::ChunkNotFound);
    }

    // Read file
    std::ifstream file(objectPath, std::ios::binary | std::ios::ate);
    if (!file) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Result<IStorageEngine::RawObject>(ErrorCode::PermissionDenied);
    }

    auto fileSize = file.tellg();
    if (fileSize < 0) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Result<IStorageEngine::RawObject>(ErrorCode::IOError);
    }
    file.seekg(0, std::ios::beg);

    // Avoid zero-initialization: allocate uninitialized, read directly, then copy into vector.
    // Guard against UB: make_unique_for_overwrite(0) may return null; assign(null,null) is UB.
    if (fileSize == 0) {
        pImpl->stats.readOperations.fetch_add(1);
        IStorageEngine::RawObject obj;
        obj.header = std::nullopt;
        return obj;
    }

    auto buf = std::make_unique_for_overwrite<std::byte[]>(static_cast<size_t>(fileSize));
    YAMS_DCHECK(buf != nullptr, "make_unique_for_overwrite must return non-null for positive size");
    file.read(reinterpret_cast<char*>(buf.get()), fileSize);

    if (!file) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Result<IStorageEngine::RawObject>(ErrorCode::CorruptedData);
    }

    YAMS_DCHECK(static_cast<size_t>(file.tellg()) == static_cast<size_t>(fileSize),
                "File read should consume entire file");
    // Update statistics
    pImpl->stats.readOperations.fetch_add(1);

    IStorageEngine::RawObject obj;
    obj.data.assign(buf.get(), buf.get() + static_cast<size_t>(fileSize));
    YAMS_DCHECK(obj.data.size() == static_cast<size_t>(fileSize),
                "Assigned vector size must match file size");
    obj.header = std::nullopt;
    return obj;
}

Result<std::vector<std::byte>> StorageEngine::retrieve(std::string_view hash) const {
    auto rawResult = retrieveRaw(hash);
    if (!rawResult) {
        return rawResult.error();
    }
    auto& data = rawResult.value().data;

    // Optional content-integrity verification: recompute hash and compare.
    if (pImpl->config.verifyReads) {
        auto computed = crypto::SHA256Hasher::hash(data);
        if (computed != hash) {
            spdlog::error("Retrieve hash mismatch for {}: computed={}", hash, computed);
            return Result<std::vector<std::byte>>(ErrorCode::HashMismatch);
        }
    }

    return std::move(data);
}

Result<bool> StorageEngine::exists(std::string_view hash) const noexcept {
    try {
        // Allow manifest keys (hash.manifest) and regular hashes. Both forms must be hex-only
        // to keep sharded paths below the storage root.
        if (!isValidStorageKey(hash)) {
            spdlog::error(
                "Invalid storage key for exists: expected hex hash or hex hash manifest, got '{}'",
                hash);
            return Result<bool>(ErrorCode::InvalidArgument);
        }

        auto objectPath = getObjectPath(hash);

        // Check existence
        return std::filesystem::exists(objectPath);

    } catch (const std::exception& e) {
        spdlog::error("Error checking existence of {}: {}", hash, e.what());
        return Result<bool>(ErrorCode::Unknown);
    }
}

Result<void> StorageEngine::remove(std::string_view hash) {
    // Allow manifest keys (hash.manifest) and regular hashes. Both forms must be hex-only
    // to keep sharded paths below the storage root.
    if (!isValidStorageKey(hash)) {
        spdlog::error(
            "Invalid storage key for remove: expected hex hash or hex hash manifest, got '{}'",
            hash);
        return Result<void>(ErrorCode::InvalidArgument);
    }

    auto objectPath = getObjectPath(hash);

    // Block compaction from pruning shard directories while a remove is in flight.
    std::shared_lock<std::shared_mutex> storageLock(pImpl->globalMutex);

    // Acquire per-hash write lock
    auto& hashMutex = pImpl->writeMutexPool.getMutex(hash);
    std::lock_guard<std::mutex> lock(hashMutex);

    std::error_code ec;
    bool pathExists = std::filesystem::exists(objectPath, ec);
    if (ec) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Error{ErrorCode::IOError, "Failed to check object path: " + ec.message()};
    }
    if (!pathExists) {
        return Result<void>(ErrorCode::ChunkNotFound);
    }

    bool isFile = std::filesystem::is_regular_file(objectPath, ec);
    if (ec) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Error{ErrorCode::IOError, "Failed to stat object path: " + ec.message()};
    }
    if (!isFile) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Error{ErrorCode::IOError, "Object path is not a regular file"};
    }

    // Get file size before deletion
    auto fileSize = std::filesystem::file_size(objectPath, ec);
    if (ec) {
        pImpl->stats.failedOperations.fetch_add(1);
        return Error{ErrorCode::IOError, "Failed to size object before removal: " + ec.message()};
    }

    // Remove file
    if (!std::filesystem::remove(objectPath, ec)) {
        pImpl->stats.failedOperations.fetch_add(1);
        if (ec) {
            return Error{ErrorCode::PermissionDenied, "Failed to remove object: " + ec.message()};
        }
        return Error{ErrorCode::IOError, "Object was not removed"};
    }

    // Update statistics using saturating subtraction to prevent underflow
    core::saturating_sub(pImpl->stats.totalObjects, uint64_t{1});
    core::saturating_sub(pImpl->stats.totalBytes, static_cast<uint64_t>(fileSize));
    pImpl->stats.deleteOperations.fetch_add(1);

    spdlog::debug("Removed object {} ({} bytes)", hash, fileSize);

    return {};
}

Result<uint64_t> StorageEngine::getBlockSize(std::string_view hash) const {
    // Allow manifest keys (hash.manifest) and regular hashes. Both forms must be hex-only
    // to keep sharded paths below the storage root.
    if (!isValidStorageKey(hash)) {
        spdlog::error("Invalid storage key for getBlockSize: expected hex hash or hex hash "
                      "manifest, got '{}'",
                      hash);
        return Result<uint64_t>(ErrorCode::InvalidArgument);
    }

    auto objectPath = getObjectPath(hash);

    std::error_code ec;
    if (!std::filesystem::exists(objectPath, ec)) {
        return Result<uint64_t>(ErrorCode::ChunkNotFound);
    }

    auto fileSize = std::filesystem::file_size(objectPath, ec);
    if (ec) {
        return Result<uint64_t>(ErrorCode::IOError);
    }

    return static_cast<uint64_t>(fileSize);
}

std::future<Result<void>> StorageEngine::storeAsync(std::string_view hash,
                                                    std::span<const std::byte> data) {
    // Create a copy of the data for async operation
    std::vector<std::byte> dataCopy(data.begin(), data.end());
    std::string hashCopy(hash);

    return std::async(std::launch::async,
                      [this, hash = std::move(hashCopy), data = std::move(dataCopy)]() {
                          return store(hash, data);
                      });
}

std::future<Result<std::vector<std::byte>>>
StorageEngine::retrieveAsync(std::string_view hash) const {
    std::string hashCopy(hash);

    return std::async(std::launch::async,
                      [this, hash = std::move(hashCopy)]() { return retrieve(hash); });
}

std::future<Result<IStorageEngine::RawObject>>
StorageEngine::retrieveRawAsync(std::string_view hash) const {
    std::string hashCopy(hash);

    return std::async(std::launch::async,
                      [this, hash = std::move(hashCopy)]() { return retrieveRaw(hash); });
}

StorageStats StorageEngine::getStats() const noexcept {
    return pImpl->stats;
}

Result<uint64_t> StorageEngine::getStorageSize() const {
    try {
        uint64_t totalSize = 0;

        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(pImpl->config.basePath / "objects")) {
            if (entry.is_regular_file()) {
                totalSize += entry.file_size();
            }
        }

        return totalSize;

    } catch (const std::exception& e) {
        spdlog::error("Failed to calculate storage size: {}", e.what());
        return Result<uint64_t>(ErrorCode::Unknown);
    }
}

Result<std::vector<std::string>> StorageEngine::list(std::string_view prefix) const {
    std::vector<std::string> keys;
    const auto objectsRoot = pImpl->config.basePath / "objects";
    const auto manifestsRoot = pImpl->config.basePath / "manifests";

    auto collect = [&keys, prefix](const std::filesystem::path& root,
                                   bool manifests) -> Result<void> {
        std::error_code ec;
        if (!std::filesystem::exists(root, ec)) {
            return {};
        }
        if (ec) {
            return Error{ErrorCode::IOError, "Failed to check storage root: " + ec.message()};
        }

        for (const auto& entry : std::filesystem::recursive_directory_iterator(root, ec)) {
            if (ec) {
                return Error{ErrorCode::IOError, "Failed to list storage root: " + ec.message()};
            }
            if (!entry.is_regular_file(ec) || ec) {
                ec.clear();
                continue;
            }

            auto key = storageHashFromObjectPath(root, entry.path());
            if (manifests && !key.ends_with(".manifest")) {
                key += ".manifest";
            }
            if (!isValidStorageKey(key)) {
                continue;
            }
            if (prefix.empty() || key.starts_with(prefix)) {
                keys.push_back(std::move(key));
            }
        }
        return {};
    };

    if (auto result = collect(objectsRoot, false); !result) {
        return result.error();
    }
    if (auto result = collect(manifestsRoot, true); !result) {
        return result.error();
    }

    std::sort(keys.begin(), keys.end());
    return keys;
}

Result<void> StorageEngine::verify() const {
    spdlog::debug("Starting storage verification...");

    size_t verifiedCount = 0;
    size_t errorCount = 0;
    const auto objectsRoot = pImpl->config.basePath / "objects";

    try {
        if (!std::filesystem::exists(objectsRoot)) {
            return Result<void>(ErrorCode::ChunkNotFound);
        }

        for (const auto& entry : std::filesystem::recursive_directory_iterator(objectsRoot)) {
            if (!entry.is_regular_file()) {
                continue;
            }

            const auto expectedKey = storageHashFromObjectPath(objectsRoot, entry.path());
            if (!isValidStorageKey(expectedKey)) {
                spdlog::warn("Storage verification found invalid object path: {}",
                             entry.path().string());
                ++errorCount;
                continue;
            }
            if (expectedKey.ends_with(".manifest")) {
                // Manifest integrity is content-store scoped: the manifest payload is not named by
                // its own content hash. ContentStore::verify() deserializes and validates manifests
                // against chunk objects and reference counts.
                ++verifiedCount;
                continue;
            }
            const auto expectedHash = expectedKey;

            std::ifstream file(entry.path(), std::ios::binary | std::ios::ate);
            if (!file) {
                spdlog::warn("Storage verification failed to open object: {}",
                             entry.path().string());
                ++errorCount;
                continue;
            }

            const auto fileSize = file.tellg();
            if (fileSize < 0) {
                spdlog::warn("Storage verification failed to size object: {}",
                             entry.path().string());
                ++errorCount;
                continue;
            }
            file.seekg(0, std::ios::beg);

            std::vector<std::byte> storedBytes(static_cast<size_t>(fileSize));
            file.read(reinterpret_cast<char*>(storedBytes.data()), fileSize);
            if (!file) {
                spdlog::warn("Storage verification failed to read object: {}",
                             entry.path().string());
                ++errorCount;
                continue;
            }

            auto contentBytes = bytesForContentHash(storedBytes);
            if (!contentBytes) {
                spdlog::warn("Storage verification failed to decode object {}: {}", expectedHash,
                             contentBytes.error().message);
                ++errorCount;
                continue;
            }

            const auto actualHash = crypto::SHA256Hasher::hash(std::span<const std::byte>(
                contentBytes.value().data(), contentBytes.value().size()));
            if (actualHash != expectedHash) {
                spdlog::warn("Storage verification hash mismatch for {}: calculated {}",
                             expectedHash, actualHash);
                ++errorCount;
                continue;
            }

            ++verifiedCount;
        }

        spdlog::debug("Storage verification complete: {} objects verified, {} errors",
                      verifiedCount, errorCount);

        return errorCount == 0 ? Result<void>{} : Result<void>(ErrorCode::CorruptedData);

    } catch (const std::exception& e) {
        spdlog::error("Storage verification failed: {}", e.what());
        return Result<void>(ErrorCode::Unknown);
    }
}

Result<void> StorageEngine::compact() {
    spdlog::debug("Starting storage compaction");

    // Serialize with store/remove so empty shard pruning cannot race target parent creation.
    std::unique_lock<std::shared_mutex> storageLock(pImpl->globalMutex);

    auto cleaned = cleanupStaleTempFiles(pImpl->config.basePath / "temp");
    if (!cleaned) {
        spdlog::error("Storage compaction temp-file cleanup failed: {}", cleaned.error().message);
        return cleaned.error();
    }

    size_t prunedDirectories = 0;
    for (const auto& root :
         {pImpl->config.basePath / "objects", pImpl->config.basePath / "manifests"}) {
        auto pruned = pruneEmptyDirectories(root);
        if (!pruned) {
            spdlog::error("Storage compaction directory pruning failed for {}: {}", root.string(),
                          pruned.error().message);
            return pruned.error();
        }
        prunedDirectories += pruned.value();
    }

    spdlog::debug(
        "Storage compaction complete: removed {} stale temp files, pruned {} empty directories",
        cleaned.value(), prunedDirectories);
    return {};
}

Result<void> StorageEngine::cleanupTempFiles() {
    spdlog::debug("Cleaning up temporary files...");

    auto cleaned = cleanupStaleTempFiles(pImpl->config.basePath / "temp");
    if (!cleaned) {
        spdlog::error("Temp file cleanup failed: {}", cleaned.error().message);
        return cleaned.error();
    }

    spdlog::debug("Cleaned up {} temporary files", cleaned.value());
    return {};
}

// AtomicFileWriter implementation
std::filesystem::path
AtomicFileWriter::generateTempName(const std::filesystem::path& target) const {
    auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    std::random_device rngDevice;
    std::mt19937 gen(rngDevice());
    constexpr int RAND_ID_MIN = 1000;
    constexpr int RAND_ID_MAX = 9999;
    std::uniform_int_distribution<> dist(RAND_ID_MIN, RAND_ID_MAX);
    return {yamsfmt::format("{}.tmp.{}.{}", target.string(), timestamp, dist(gen))};
}

auto AtomicFileWriter::writeImpl(const std::filesystem::path& path, std::span<const std::byte> data)
    -> Result<void> {
    auto tempPath = generateTempName(path);

    // RAII cleanup guard
    struct TempFileGuard {
        std::filesystem::path path;
        bool success = false;

        ~TempFileGuard() {
            if (!success) {
                std::error_code removeError;
                std::filesystem::remove(path, removeError);
            }
        }
    } guard{.path = tempPath};

    // Write to temp file
    {
        std::ofstream file(tempPath, std::ios::binary);
        if (!file) {
            return Result<void>(ErrorCode::PermissionDenied);
        }

        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): Required by ofstream::write
        file.write(reinterpret_cast<const char*>(data.data()),
                   static_cast<std::streamsize>(data.size()));
        if (!file) {
            return {ErrorCode::Unknown};
        }
    }

    // Atomic rename
    std::error_code renameError;
    std::filesystem::rename(tempPath, path, renameError);

    if (renameError) {
        return {ErrorCode::Unknown};
    }

    guard.success = true;
    return {};
}

auto AtomicFileWriter::writeBatch(
    std::vector<std::pair<std::filesystem::path, std::vector<std::byte>>> items)
    -> std::future<std::vector<Result<void>>> {
    return std::async(std::launch::async, [this, items = std::move(items)]() {
        std::vector<Result<void>> results;
        results.reserve(items.size());

        for (const auto& [path, data] : items) {
            results.push_back(writeImpl(path, data));
        }

        return results;
    });
}

// Factory function
auto createStorageEngine(StorageConfig config) -> std::unique_ptr<IStorageEngine> {
    return std::make_unique<StorageEngine>(std::move(config));
}

// Utility functions
auto initializeStorage(const std::filesystem::path& basePath) -> Result<void> {
    try {
        // Create directory structure
        if (!yams::common::ensureDirectories(basePath / "objects")) {
            return {ErrorCode::PermissionDenied};
        }

        if (!yams::common::ensureDirectories(basePath / "temp")) {
            return {ErrorCode::PermissionDenied};
        }

        if (!yams::common::ensureDirectories(basePath / "manifests")) {
            return {ErrorCode::PermissionDenied};
        }

        spdlog::debug("Initialized storage at: {}", basePath.string());
        return {};

    } catch (const std::exception& e) {
        spdlog::error("Failed to initialize storage: {}", e.what());
        return {ErrorCode::Unknown};
    }
}

auto validateStorageIntegrity(const std::filesystem::path& basePath) -> Result<bool> {
    try {
        // Check required directories exist
        if (!std::filesystem::exists(basePath / "objects") ||
            !std::filesystem::exists(basePath / "temp")) {
            return false;
        }

        return true;

    } catch (const std::exception& e) {
        spdlog::error("Storage validation failed: {}", e.what());
        return {ErrorCode::Unknown};
    }
}

#ifdef YAMS_TESTING
void StorageEngine::testing_setAtomicWriteFailureAfterBytes(size_t bytes) {
    gAtomicWriteFailureAfterBytes.store(bytes, std::memory_order_relaxed);
}

void StorageEngine::testing_clearAtomicWriteFailure() {
    gAtomicWriteFailureAfterBytes.store(std::numeric_limits<size_t>::max(),
                                        std::memory_order_relaxed);
}

void StorageEngine::testing_setFileOpenFailure(bool v) {
    gFileOpenFailure.store(v, std::memory_order_relaxed);
}

void StorageEngine::testing_setRenameFailure(bool v) {
    gRenameFailure.store(v, std::memory_order_relaxed);
}
#endif

} // namespace yams::storage
