#pragma once

#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>
#include <yams/core/types.h>

namespace yams::config {

/**
 * @brief Configuration version information
 */
struct ConfigVersion {
    int major = 2;
    int minor = 0;
    int patch = 0;
    std::string toString() const {
        return std::to_string(major) + "." + std::to_string(minor) + "." + std::to_string(patch);
    }
};

/**
 * @brief Configuration migration entry
 */
struct MigrationEntry {
    std::string old_key;       // Key in old config
    std::string new_key;       // Key in new config
    std::string default_value; // Default if not present
    std::string description;   // What this setting does
    bool required = false;     // Whether this is required
};

/**
 * @brief Config migrator for handling version upgrades
 */
class ConfigMigrator {
public:
    ConfigMigrator() = default;
    ~ConfigMigrator() = default;

    /**
     * @brief Check if migration is needed
     */
    Result<bool> needsMigration(const std::filesystem::path& configPath);

    /**
     * @brief Migrate config from v1 to v2
     */
    Result<void> migrateToV2(const std::filesystem::path& configPath, bool createBackup = true);

    /**
     * @brief Get current config version
     */
    Result<ConfigVersion> getConfigVersion(const std::filesystem::path& configPath);

    /**
     * @brief Create default v2 config
     */
    Result<void> createDefaultV2Config(const std::filesystem::path& configPath);

    /**
     * @brief Validate v2 config structure
     */
    Result<void> validateV2Config(const std::filesystem::path& configPath);

    /**
     * @brief Get migration map from v1 to v2
     */
    static std::vector<MigrationEntry> getV1ToV2MigrationMap();

    /**
     * @brief Get all v2 config sections with defaults
     */
    static std::map<std::string, std::map<std::string, std::string>> getV2ConfigDefaults();

    /**
     * @brief Additional v2 keys introduced after initial v2 rollout (additive only)
     * These are merged non-destructively into existing v2 configs during update.
     */
    static std::map<std::string, std::map<std::string, std::string>> getV2AdditiveDefaults();

    /**
     * @brief Update existing v2 config by adding any missing keys from additive defaults.
     * - Does not overwrite existing values
     * - Optionally creates a timestamped backup
     * - When dryRun=true, no file is written; returns the list of keys that would be added
     * @return list of dot-keys (section.key) added or to be added
     */
    Result<std::vector<std::string>> updateV2SchemaAdditive(const std::filesystem::path& configPath,
                                                            bool makeBackup = true,
                                                            bool dryRun = false);

    /**
     * @brief Parse TOML config file
     */
    Result<std::map<std::string, std::map<std::string, std::string>>>
    parseTomlConfig(const std::filesystem::path& path);

private:
    /**
     * @brief Write TOML config file
     */
    Result<void>
    writeTomlConfig(const std::filesystem::path& path,
                    const std::map<std::string, std::map<std::string, std::string>>& config,
                    const ConfigVersion& version);

    /**
     * @brief Create backup of existing config
     */
    Result<std::filesystem::path> createBackup(const std::filesystem::path& configPath);

    /**
     * @brief Merge old config values into new structure
     */
    std::map<std::string, std::map<std::string, std::string>>
    mergeConfigs(const std::map<std::string, std::map<std::string, std::string>>& oldConfig,
                 const std::map<std::string, std::map<std::string, std::string>>& newDefaults);

    /**
     * @brief Log migration actions
     */
    void logMigration(const std::string& action, const std::string& details);
};

/**
 * @brief V2 Configuration structure (complete)
 */
struct ConfigV2 {
    // Version info
    struct Version {
        int config_version = 2;
        int migrated_from = 0;
        std::string migration_date;
    } version;

    // Core settings
    struct Core {
        std::string data_dir = "~/.yams/data";
        std::string storage_engine = "local";
        bool enable_wal = true;
        bool enable_telemetry = false;
        std::string log_level = "info";
    } core;

    // Authentication
    struct Auth {
        std::string private_key_path;
        std::string public_key_path;
        std::vector<std::string> api_keys;
    } auth;

    // Performance settings
    struct Performance {
        int num_worker_threads = 4;
        int io_thread_pool_size = 2;
        int max_concurrent_operations = 100;
        bool enable_memory_mapping = false;
        size_t cache_size_mb = 512;
    } performance;

    // Storage settings
    struct Storage {
        std::string engine = "local";
        std::string base_path;
        bool enable_compression = true;
        bool enable_deduplication = true;
        bool enable_chunking = true;
    } storage;

    // Chunking settings
    struct Chunking {
        size_t window_size = 48;
        size_t min_chunk_size = 1024;
        size_t max_chunk_size = 16384;
        size_t average_chunk_size = 8192;
        std::string hash_algorithm = "sha256";
    } chunking;

    // Compression settings
    struct Compression {
        bool enable = true;
        std::string algorithm = "zstd";
        int zstd_level = 9;
        int lzma_level = 6;
        size_t chunk_threshold = 1024;
        size_t always_compress_above = 10485760;
        size_t never_compress_below = 1024;
        int compress_after_days = 1;
        int archive_after_days = 30;
        int max_concurrent_compressions = 4;
        bool async_compression = true;
        size_t decompression_cache_size = 100;
        size_t decompression_cache_size_bytes = 536870912;
    } compression;

    // WAL settings
    struct WAL {
        bool enable = true;
        std::string wal_directory = "./wal";
        size_t max_log_size = 104857600;
        size_t sync_interval = 1000;
        int sync_timeout_ms = 100;
        bool compress_old_logs = true;
        size_t max_open_files = 10;
        bool enable_group_commit = true;
    } wal;

    // Vector database settings
    struct VectorDatabase {
        bool enable = true;
        std::string database_path = "vectors.db";
        std::string table_name = "document_embeddings";
        size_t embedding_dim = 384;
        std::string index_type = "IVF_PQ";
        size_t num_partitions = 256;
        size_t num_sub_quantizers = 96;
        bool enable_checkpoints = true;
        size_t checkpoint_frequency = 1000;
        size_t max_batch_size = 1000;
        float default_similarity_threshold = 0.7f;
        bool use_in_memory = false;
        bool auto_create = true;
        bool auto_repair = true;
    } vector_database;

    // Embeddings settings
    struct Embeddings {
        bool enable = true;
        bool auto_generate = true;
        std::string preferred_model = "all-MiniLM-L6-v2";
        std::string model_path = "~/.yams/models";
        std::string tokenizer_path = "models/tokenizer.json";
        size_t max_sequence_length = 512;
        size_t embedding_dim = 384;
        size_t batch_size = 32;
        bool normalize_embeddings = true;
        bool enable_gpu = false;
        int num_threads = -1;
        int generation_delay_ms = 1000;
        bool track_content_hash = true;
        std::string model_version = "1.0.0";
        int embedding_schema_version = 1;
    } embeddings;

    // Vector index settings
    struct VectorIndex {
        std::string type = "HNSW";
        size_t dimension = 384;
        std::string distance_metric = "COSINE";
        size_t hnsw_m = 16;
        size_t hnsw_ef_construction = 200;
        size_t hnsw_ef_search = 50;
        bool normalize_vectors = true;
        size_t max_elements = 1000000;
        bool enable_persistence = true;
        std::string index_path = "vector_index.bin";
        size_t batch_size = 1000;
        bool use_simd = true;
        int num_threads = 0;
        bool enable_delta_index = true;
        size_t delta_threshold = 1000;
    } vector_index;

    // Search settings
    struct Search {
        size_t default_limit = 10;
        size_t max_limit = 100;
        bool enable_cache = true;
        size_t cache_size = 1000;
        int cache_ttl_minutes = 60;

        // Hybrid search settings
        struct Hybrid {
            bool enable = true;
            bool enable_kg = true;
            float vector_weight = 0.6f;
            float keyword_weight = 0.35f;
            float kg_entity_weight = 0.03f;
            float structural_weight = 0.02f;
            size_t vector_top_k = 50;
            size_t keyword_top_k = 50;
            size_t final_top_k = 10;
            std::string fusion_strategy = "LINEAR_COMBINATION";
            float rrf_k = 60.0f;
            bool enable_reranking = true;
            size_t rerank_top_k = 20;
            float rerank_threshold = 0.0f;
            bool enable_query_expansion = false;
            size_t expansion_terms = 5;
            float expansion_weight = 0.3f;
            size_t kg_max_neighbors = 32;
            size_t kg_max_hops = 1;
            int kg_budget_ms = 20;
            bool normalize_scores = true;
            bool generate_explanations = true;
            bool include_debug_scores = false;
            bool parallel_search = true;
        } hybrid;
    } search;

    // Knowledge graph settings
    struct KnowledgeGraph {
        std::string db_path;
        bool enable_alias_fts = true;
        bool enable_wal = true;
        bool prefer_exact_alias_first = true;
        size_t default_limit = 1000;
        size_t node_cache_capacity = 10000;
        size_t alias_cache_capacity = 50000;
        size_t embedding_cache_capacity = 10000;
        size_t neighbor_cache_capacity = 10000;
    } knowledge_graph;

    // File detection settings
    struct FileDetection {
        bool use_lib_magic = true;
        bool use_builtin_patterns = true;
        bool use_custom_patterns = true;
        std::string patterns_file;
        size_t max_bytes_to_read = 512;
        bool cache_results = true;
        size_t cache_size = 1000;
    } file_detection;

    // MCP server settings
    struct MCPServer {
        bool enable = false;
        std::string transport = "stdio";
        std::string host = "localhost";
        uint16_t port = 8080;
        std::string path = "/mcp";
        bool use_ssl = false;
        int connect_timeout_ms = 5000;
        int receive_timeout_ms = 30000;
        size_t max_message_size = 1048576;
        bool enable_ping_pong = true;
        int ping_interval_s = 30;
    } mcp_server;

    // Daemon settings for background embedding service
    struct Daemon {
        bool enable = true;     // Enable daemon usage
        bool auto_start = true; // Auto-start daemon if not running
        std::string socket_path = "/tmp/yams-daemon.sock";
        std::string pid_file = "/tmp/yams-daemon.pid";
        size_t worker_threads = 4;
        size_t max_memory_gb = 4;
        std::string log_level = "info";
        int connect_timeout_ms = 1000;
        int request_timeout_ms = 5000;

        // Model management
        struct Models {
            size_t max_loaded_models = 3; // Max models in memory
            size_t hot_pool_size = 1;     // Always loaded models
            std::vector<std::string> preload_models = {"all-MiniLM-L6-v2"};
            bool lazy_loading = false;      // Load only when needed
            int model_idle_timeout_s = 300; // Unload after idle
            bool enable_gpu = false;
            int num_threads = 4;
            std::string eviction_policy = "lru"; // lru, lfu, fifo
        } models;

        // Resource pooling
        struct ResourcePool {
            size_t min_sessions = 1;         // Min ONNX sessions per model
            size_t max_sessions = 3;         // Max ONNX sessions per model
            int idle_timeout_s = 300;        // Session idle timeout
            bool validate_on_acquire = true; // Validate session health
        } resource_pool;

        // Circuit breaker settings
        struct CircuitBreaker {
            bool enable = true;
            size_t failure_threshold = 5;
            size_t success_threshold = 2;
            int open_timeout_s = 30;
            int half_open_timeout_s = 10;
        } circuit_breaker;

        // Embedding client behavior
        struct Client {
            bool use_daemon = true;        // Try daemon first
            bool fallback_to_local = true; // Fallback if daemon fails
            size_t max_retries = 3;
            int batch_size = 32; // Batch embedding requests
        } client;
    } daemon;

    // Experimental features
    struct Experimental {
        bool enable = false;

        struct BERTNER {
            bool enable = false;
            std::string model_path;
            std::string model_name = "bert-base-NER";
            size_t batch_size = 16;
            float confidence_threshold = 0.8f;
            bool auto_tag = false;
            bool update_knowledge_graph = false;
            std::vector<std::string> entity_types = {"PERSON", "ORG", "LOC", "MISC"};
        } bert_ner;

        struct AutoTagging {
            bool enable = false;
            bool use_bert_ner = false;
            bool use_keyword_extraction = true;
            bool use_topic_modeling = false;
            float min_tag_confidence = 0.7f;
            size_t max_tags_per_document = 10;
            int tag_update_frequency_hours = 24;
        } auto_tagging;

        struct SmartChunking {
            bool enable = false;
            bool use_semantic_boundaries = false;
            bool use_sentence_detection = true;
            bool preserve_code_blocks = true;
            bool preserve_markdown_sections = true;
        } smart_chunking;
    } experimental;

    // Migration settings
    struct Migrations {
        bool auto_migrate = true;
        bool backup_before_migrate = true;
        std::string migration_log_path = "migrations.log";
    } migrations;

    // Load from file
    static Result<ConfigV2> load(const std::filesystem::path& path);

    // Save to file
    Result<void> save(const std::filesystem::path& path) const;

    // Validate configuration
    Result<void> validate() const;
};

} // namespace yams::config