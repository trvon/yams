#define _CRT_SECURE_NO_WARNINGS
#include <yams/api/content_store_builder.h>
#include <yams/api/content_store_error.h>
#include <yams/chunking/streaming_chunker.h>
#include <yams/common/fs_utils.h>
#include <yams/compression/compression_policy.h>
#include <yams/storage/compressed_storage_engine.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <optional>
#include <sstream>

namespace fs = std::filesystem;

// Forward declare the factory function from content_store_impl.cpp
namespace yams::api {
std::unique_ptr<IContentStore> createContentStore(
    std::shared_ptr<storage::IStorageEngine> storage, std::shared_ptr<chunking::IChunker> chunker,
    std::shared_ptr<crypto::IHasher> hasher,
    std::shared_ptr<manifest::IManifestManager> manifestManager,
    std::shared_ptr<storage::IReferenceCounter> refCounter, const ContentStoreConfig& config);
}

namespace yams::api {

namespace {

template <typename T> std::optional<T> parseIntegralConfig(const std::string& raw) {
    T value{};
    auto begin = raw.data();
    auto end = begin + raw.size();
    auto [ptr, ec] = std::from_chars(begin, end, value);
    if (ec != std::errc{} || ptr != end) {
        return std::nullopt;
    }
    return value;
}

int clampCompressionLevel(int level, int minLevel, int maxLevel) {
    return std::clamp(level, minLevel, maxLevel);
}

std::chrono::hours daysToHoursClamped(int days) {
    if (days <= 0) {
        return std::chrono::hours{0};
    }
    using Rep = std::chrono::hours::rep;
    constexpr Rep kHoursPerDay = 24;
    constexpr Rep kMaxDays = std::numeric_limits<Rep>::max() / kHoursPerDay;
    const Rep safeDays = std::min<Rep>(static_cast<Rep>(days), kMaxDays);
    return std::chrono::hours{safeDays * kHoursPerDay};
}

} // namespace

// Builder implementation
struct ContentStoreBuilder::Impl {
    ContentStoreConfig config;

    // Optional injected components
    std::shared_ptr<storage::IStorageEngine> storageEngine;
    std::shared_ptr<chunking::IChunker> chunker;
    std::shared_ptr<crypto::IHasher> hasher;
    std::shared_ptr<manifest::IManifestManager> manifestManager;
    std::shared_ptr<storage::IReferenceCounter> referenceCounter;

    Impl() {
        // Set default configuration
        config.chunkSize = DEFAULT_CHUNK_SIZE;
        config.enableCompression = true;
        config.compressionType = "zstd";
        config.compressionLevel = 3;
        config.enableDeduplication = true;
        config.maxConcurrentOps = 10;
        config.enableIntegrityChecks = true;
        config.gcInterval = std::chrono::seconds(3600);
    }

    Result<void> createDefaultComponents() {
        auto makeCompressionConfig = [&]() {
            // Configure compression policy
            compression::CompressionPolicy::Rules policyRules;

            // Load compression settings from config if available
            bool hasNeverCompressBelow = false;
            bool hasAlwaysCompressAbove = false;
            loadCompressionSettings(policyRules, hasNeverCompressBelow, hasAlwaysCompressAbove);

            // Only apply eager compression defaults if config didn't specify thresholds
            if (!hasNeverCompressBelow) {
                policyRules.neverCompressBelow = 0;
            }
            if (!hasAlwaysCompressAbove) {
                policyRules.alwaysCompressAbove = 1;
            }
            if (policyRules.preferZstdBelow == 0) {
                policyRules.preferZstdBelow = std::numeric_limits<uint64_t>::max();
            }
            if (policyRules.compressAfterAge.count() == 0) {
                policyRules.compressAfterAge = std::chrono::hours(0);
            }
            if (policyRules.archiveAfterAge.count() == 0) {
                policyRules.archiveAfterAge = std::chrono::hours(24 * 30);
            }

            return storage::CompressedStorageEngine::Config{
                .enableCompression = true,
                .compressExisting = false, // Don't compress existing data on startup
                .policyRules = policyRules,
                .compressionThreshold = 0,
                .asyncCompression = false,
                .maxAsyncQueue = 0,
                .metadataCacheTTL = std::chrono::seconds(300)};
        };

        // Create storage engine if not provided
        if (!storageEngine) {
            storage::StorageConfig storageConfig{
                .basePath = config.storagePath,
                .enableCompression = false // We'll wrap with CompressedStorageEngine instead
            };
            auto baseStorage = storage::createStorageEngine(storageConfig);
            storageEngine = std::shared_ptr<storage::IStorageEngine>(baseStorage.release());
        }

        // Wrap either default local storage or an injected storage engine (for example R2/S3) so
        // data is compressed before it is handed to the underlying backend.
        if (config.enableCompression &&
            !std::dynamic_pointer_cast<storage::CompressedStorageEngine>(storageEngine)) {
            storageEngine = std::make_shared<storage::CompressedStorageEngine>(
                storageEngine, makeCompressionConfig());

            spdlog::debug("Storage engine wrapped with compression support (Zstandard for active, "
                          "LZMA for archived)");
        }

        // Create chunker if not provided
        if (!chunker) {
            chunking::ChunkingConfig chunkerConfig{
                .windowSize = 48,
                .minChunkSize = MIN_CHUNK_SIZE,
                .targetChunkSize = config.chunkSize,
                .maxChunkSize = MAX_CHUNK_SIZE,
                .polynomial = 0x3DA3358B4DC173LL,
                .chunkMask = 0x1FFF // For 64KB average chunks
            };
            // Use streaming chunker to avoid loading entire files in memory
            chunker = chunking::createStreamingChunker(chunkerConfig);
            spdlog::debug("Using StreamingChunker for memory-efficient file processing");
        }

        // Create hasher if not provided
        if (!hasher) {
            hasher = crypto::createSHA256Hasher();
        }

        // Create manifest manager if not provided
        if (!manifestManager) {
            manifest::ManifestManager::Config manifestConfig{
                .enableCompression = config.enableCompression,
                .compressionAlgorithm = config.compressionType,
                .enableChecksums = config.enableIntegrityChecks};
            manifestManager = manifest::createManifestManager(manifestConfig);
        }

        // Create reference counter if not provided
        if (!referenceCounter) {
            auto dbPath = config.storagePath / "refs.db";
            storage::ReferenceCounter::Config refConfig{.databasePath = dbPath};
            try {
                referenceCounter = storage::createReferenceCounter(refConfig);
            } catch (const std::exception& e) {
                spdlog::error("Reference counter initialization failed: {}", e.what());
                return Result<void>(ErrorCode::DatabaseError);
            }
            if (!referenceCounter) {
                return Result<void>(ErrorCode::DatabaseError);
            }
        }

        return Result<void>();
    }

    void loadCompressionSettings(compression::CompressionPolicy::Rules& rules,
                                 bool& hasNeverCompressBelow, bool& hasAlwaysCompressAbove) {
        hasNeverCompressBelow = false;
        hasAlwaysCompressAbove = false;

        // Try to read config file
        fs::path configPath = getConfigPath();
        if (!fs::exists(configPath)) {
            return; // Use defaults if no config
        }

        auto configMap = parseSimpleToml(configPath);

        // Load compression levels
        if (auto it = configMap.find("compression.zstd_level"); it != configMap.end()) {
            if (auto level = parseIntegralConfig<int>(it->second)) {
                const int zstdLevel = clampCompressionLevel(*level, 1, 22);
                rules.defaultZstdLevel = static_cast<std::uint8_t>(zstdLevel);
                const int archiveZstdLevel = zstdLevel <= 19 ? zstdLevel + 3 : 22;
                rules.archiveZstdLevel =
                    static_cast<std::uint8_t>(archiveZstdLevel); // Higher for archives
            }
        }

        if (auto it = configMap.find("compression.lzma_level"); it != configMap.end()) {
            if (auto level = parseIntegralConfig<int>(it->second)) {
                rules.defaultLzmaLevel =
                    static_cast<std::uint8_t>(clampCompressionLevel(*level, 0, 9));
            }
        }

        // Load size thresholds
        if (auto it = configMap.find("compression.chunk_threshold"); it != configMap.end()) {
            if (auto threshold = parseIntegralConfig<std::uint64_t>(it->second)) {
                rules.neverCompressBelow = *threshold;
                hasNeverCompressBelow = true;
            }
        }

        if (auto it = configMap.find("compression.always_compress_above"); it != configMap.end()) {
            if (auto threshold = parseIntegralConfig<std::uint64_t>(it->second)) {
                rules.alwaysCompressAbove = *threshold;
                hasAlwaysCompressAbove = true;
            }
        }

        if (auto it = configMap.find("compression.never_compress_below"); it != configMap.end()) {
            if (auto threshold = parseIntegralConfig<std::uint64_t>(it->second)) {
                rules.neverCompressBelow = *threshold;
                hasNeverCompressBelow = true;
            }
        }

        // Load age-based policies
        if (auto it = configMap.find("compression.compress_after_days"); it != configMap.end()) {
            if (auto days = parseIntegralConfig<int>(it->second)) {
                rules.compressAfterAge = daysToHoursClamped(*days);
            }
        }

        if (auto it = configMap.find("compression.archive_after_days"); it != configMap.end()) {
            if (auto days = parseIntegralConfig<int>(it->second)) {
                rules.archiveAfterAge = daysToHoursClamped(*days);
            }
        }

        // Load performance settings
        if (auto it = configMap.find("compression.max_concurrent_compressions");
            it != configMap.end()) {
            if (auto maxConcurrent = parseIntegralConfig<std::uint64_t>(it->second)) {
                rules.maxConcurrentCompressions = *maxConcurrent;
            }
        }

        spdlog::debug("Loaded compression config: zstd_level={}, lzma_level={}, threshold={}",
                      rules.defaultZstdLevel, rules.defaultLzmaLevel, rules.neverCompressBelow);
    }

    bool getConfigBool(const std::string& key, bool defaultValue) {
        fs::path configPath = getConfigPath();
        if (!fs::exists(configPath)) {
            return defaultValue;
        }

        auto configMap = parseSimpleToml(configPath);
        if (configMap.find(key) != configMap.end()) {
            return configMap[key] == "true";
        }
        return defaultValue;
    }

    fs::path getConfigPath() const {
        const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
        const char* homeEnv = std::getenv("HOME");

        fs::path configHome;
        if (xdgConfigHome) {
            configHome = fs::path(xdgConfigHome);
        } else if (homeEnv) {
            configHome = fs::path(homeEnv) / ".config";
        } else {
            return fs::path("~/.config") / "yams" / "config.toml";
        }

        return configHome / "yams" / "config.toml";
    }

    std::map<std::string, std::string> parseSimpleToml(const fs::path& path) const {
        std::map<std::string, std::string> parsedConfig;
        std::ifstream file(path);
        if (!file) {
            return parsedConfig;
        }

        std::string line;
        std::string currentSection;

        while (std::getline(file, line)) {
            // Skip comments and empty lines
            if (line.empty() || line[0] == '#')
                continue;

            // Check for section headers
            if (line[0] == '[') {
                size_t end = line.find(']');
                if (end != std::string::npos) {
                    currentSection = line.substr(1, end - 1);
                    if (!currentSection.empty()) {
                        currentSection += ".";
                    }
                }
                continue;
            }

            // Parse key-value pairs
            size_t eq = line.find('=');
            if (eq != std::string::npos) {
                std::string key = line.substr(0, eq);
                std::string value = line.substr(eq + 1);

                // Trim whitespace
                key.erase(0, key.find_first_not_of(" \t"));
                key.erase(key.find_last_not_of(" \t") + 1);
                value.erase(0, value.find_first_not_of(" \t"));
                value.erase(value.find_last_not_of(" \t") + 1);

                // Remove quotes if present
                if (value.size() >= 2 && value[0] == '"' && value.back() == '"') {
                    value = value.substr(1, value.size() - 2);
                }

                // Remove comments from value
                size_t comment = value.find('#');
                if (comment != std::string::npos) {
                    value = value.substr(0, comment);
                    // Trim again after removing comment
                    value.erase(value.find_last_not_of(" \t") + 1);
                }

                parsedConfig[currentSection + key] = value;
            }
        }

        return parsedConfig;
    }
};

ContentStoreBuilder::ContentStoreBuilder() : pImpl(std::make_unique<Impl>()) {}

ContentStoreBuilder::~ContentStoreBuilder() = default;

ContentStoreBuilder::ContentStoreBuilder(ContentStoreBuilder&&) noexcept = default;
ContentStoreBuilder& ContentStoreBuilder::operator=(ContentStoreBuilder&&) noexcept = default;

// Configuration methods
ContentStoreBuilder& ContentStoreBuilder::withConfig(const ContentStoreConfig& config) {
    pImpl->config = config;
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withStoragePath(const std::filesystem::path& path) {
    pImpl->config.storagePath = path;
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withChunkSize(size_t size) {
    pImpl->config.chunkSize = size;
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withCompression(bool enable) {
    pImpl->config.enableCompression = enable;
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withCompressionType(const std::string& type) {
    pImpl->config.compressionType = type;
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withCompressionLevel(size_t level) {
    pImpl->config.compressionLevel = level;
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withDeduplication(bool enable) {
    pImpl->config.enableDeduplication = enable;
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withMaxConcurrentOperations(size_t max) {
    pImpl->config.maxConcurrentOps = max;
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withIntegrityChecks(bool enable) {
    pImpl->config.enableIntegrityChecks = enable;
    return *this;
}

ContentStoreBuilder&
ContentStoreBuilder::withGarbageCollectionInterval(std::chrono::seconds interval) {
    pImpl->config.gcInterval = interval;
    return *this;
}

// Component injection
ContentStoreBuilder&
ContentStoreBuilder::withStorageEngine(std::shared_ptr<storage::IStorageEngine> engine) {
    pImpl->storageEngine = std::move(engine);
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withChunker(std::shared_ptr<chunking::IChunker> chunker) {
    pImpl->chunker = std::move(chunker);
    return *this;
}

ContentStoreBuilder& ContentStoreBuilder::withHasher(std::shared_ptr<crypto::IHasher> hasher) {
    pImpl->hasher = std::move(hasher);
    return *this;
}

ContentStoreBuilder&
ContentStoreBuilder::withManifestManager(std::shared_ptr<manifest::IManifestManager> manager) {
    pImpl->manifestManager = std::move(manager);
    return *this;
}

ContentStoreBuilder&
ContentStoreBuilder::withReferenceCounter(std::shared_ptr<storage::IReferenceCounter> counter) {
    pImpl->referenceCounter = std::move(counter);
    return *this;
}

// Build methods
Result<std::unique_ptr<IContentStore>> ContentStoreBuilder::build() {
    // Validate configuration
    auto validateResult = pImpl->config.validate();
    if (!validateResult) {
        return Result<std::unique_ptr<IContentStore>>(validateResult.error());
    }

    // Ensure storage directories exist (may have been created previously).
    try {
        yams::common::ensureDirectories(pImpl->config.storagePath);
        yams::common::ensureDirectories(pImpl->config.storagePath / "temp");
    } catch (const std::exception& e) {
        spdlog::error("Failed to create storage directory: {}", e.what());
        return Result<std::unique_ptr<IContentStore>>(ErrorCode::PermissionDenied);
    }

    // Create default components if not provided
    auto createResult = pImpl->createDefaultComponents();
    if (!createResult) {
        return Result<std::unique_ptr<IContentStore>>(createResult.error());
    }

    // Create content store
    auto store = createContentStore(pImpl->storageEngine, pImpl->chunker, pImpl->hasher,
                                    pImpl->manifestManager, pImpl->referenceCounter, pImpl->config);

    spdlog::debug("Content store built successfully with storage path: {}",
                  pImpl->config.storagePath.string());

    return Result<std::unique_ptr<IContentStore>>(std::move(store));
}

Result<std::unique_ptr<IContentStore>> ContentStoreBuilder::buildAndValidate() {
    auto result = build();
    if (!result) {
        return result;
    }

    // Perform additional validation
    auto& store = result.value();
    auto health = store->checkHealth();

    if (!health.isHealthy) {
        spdlog::error("Content store health check failed: {}", health.status);
        return Result<std::unique_ptr<IContentStore>>(ErrorCode::Unknown);
    }

    return result;
}

// Static factory methods
Result<std::unique_ptr<IContentStore>>
ContentStoreBuilder::createDefault(const std::filesystem::path& storagePath) {
    return ContentStoreBuilder().withStoragePath(storagePath).build();
}

Result<std::unique_ptr<IContentStore>>
ContentStoreBuilder::createFromConfig(const ContentStoreConfig& config) {
    return ContentStoreBuilder().withConfig(config).build();
}

Result<std::unique_ptr<IContentStore>> ContentStoreBuilder::createInMemory() {
    // TODO: Implement in-memory storage engine
    // For now, use a temporary directory
    auto tempPath =
        std::filesystem::temp_directory_path() /
        ("yams_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));

    return ContentStoreBuilder().withStoragePath(tempPath).build();
}

// Convenience factory functions
Result<std::unique_ptr<IContentStore>>
createContentStore(const std::filesystem::path& storagePath) {
    return ContentStoreBuilder::createDefault(storagePath);
}

Result<std::unique_ptr<IContentStore>> createContentStore(const ContentStoreConfig& config) {
    return ContentStoreBuilder::createFromConfig(config);
}

} // namespace yams::api
