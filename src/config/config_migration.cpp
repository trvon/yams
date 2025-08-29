#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>
#include <yams/config/config_migration.h>

namespace yams::config {

namespace fs = std::filesystem;

Result<bool> ConfigMigrator::needsMigration(const fs::path& configPath) {
    if (!fs::exists(configPath)) {
        // No config exists, create v2
        return true;
    }

    auto versionResult = getConfigVersion(configPath);
    if (!versionResult) {
        // Can't determine version, assume v1
        return true;
    }

    // Check if current version is less than v2
    return versionResult.value().major < 2;
}

Result<ConfigVersion> ConfigMigrator::getConfigVersion(const fs::path& configPath) {
    if (!fs::exists(configPath)) {
        return Error{ErrorCode::FileNotFound, "Config file not found"};
    }

    auto configResult = parseTomlConfig(configPath);
    if (!configResult) {
        return Error{configResult.error()};
    }

    const auto& config = configResult.value();

    // Look for version section
    if (config.find("version") != config.end()) {
        const auto& version = config.at("version");
        if (version.find("config_version") != version.end()) {
            ConfigVersion v;
            v.major = std::stoi(version.at("config_version"));
            return v;
        }
    }

    // No version section, assume v1
    ConfigVersion v1;
    v1.major = 1;
    v1.minor = 0;
    v1.patch = 0;
    return v1;
}

Result<void> ConfigMigrator::migrateToV2(const fs::path& configPath, bool createBackup) {
    spdlog::info("Starting config migration to v2");

    // Create backup if requested
    fs::path backupPath;
    if (createBackup && fs::exists(configPath)) {
        auto backupResult = this->createBackup(configPath);
        if (!backupResult) {
            return Error{backupResult.error()};
        }
        backupPath = backupResult.value();
        spdlog::info("Created backup at: {}", backupPath.string());
    }

    // Parse existing config if it exists
    std::map<std::string, std::map<std::string, std::string>> oldConfig;
    if (fs::exists(configPath)) {
        auto parseResult = parseTomlConfig(configPath);
        if (!parseResult) {
            return Error{parseResult.error()};
        }
        oldConfig = parseResult.value();
    }

    // Get v2 defaults
    auto v2Defaults = getV2ConfigDefaults();

    // Merge old config into new structure
    auto mergedConfig = mergeConfigs(oldConfig, v2Defaults);

    // Write new config
    ConfigVersion v2;
    v2.major = 2;
    v2.minor = 0;
    v2.patch = 0;

    auto writeResult = writeTomlConfig(configPath, mergedConfig, v2);
    if (!writeResult) {
        // Restore backup if write failed
        if (!backupPath.empty() && fs::exists(backupPath)) {
            fs::copy(backupPath, configPath, fs::copy_options::overwrite_existing);
            spdlog::error("Migration failed, restored backup");
        }
        return writeResult;
    }

    spdlog::info("Successfully migrated config to v2");
    logMigration("migrate", "Successfully migrated from v1 to v2");

    return Result<void>();
}

Result<void> ConfigMigrator::createDefaultV2Config(const fs::path& configPath) {
    spdlog::info("Creating default v2 config at: {}", configPath.string());

    // Ensure parent directory exists
    fs::create_directories(configPath.parent_path());

    // Get v2 defaults
    auto v2Defaults = getV2ConfigDefaults();

    // Write config
    ConfigVersion v2;
    v2.major = 2;
    v2.minor = 0;
    v2.patch = 0;

    return writeTomlConfig(configPath, v2Defaults, v2);
}

std::map<std::string, std::map<std::string, std::string>> ConfigMigrator::getV2AdditiveDefaults() {
    // Additive keys introduced post-initial v2 rollout. These will be merged non-destructively.
    return {{"cli.streaming",
             {{"enable", "true"},
              {"payload_threshold_bytes", "262144"},
              {"ttfb_threshold_ms", "150"},
              {"chunk_size_bytes", "65536"},
              {"max_inflight_chunks", "4"}}},
            {"cli.pool",
             {{"max_clients", "8"},
              {"min_clients", "1"},
              {"acquire_timeout_ms", "5000"},
              {"max_waiters", "0"},
              {"fair_queue", "true"},
              {"health_check_interval_ms", "30000"},
              {"health_check_timeout_ms", "500"},
              {"reconnect_backoff_base_ms", "100"},
              {"reconnect_backoff_max_ms", "5000"},
              {"reconnect_backoff_jitter_pct", "0.2"},
              {"circuit_breaker_error_threshold", "5"},
              {"circuit_breaker_reset_ms", "10000"}}}};
}

std::vector<MigrationEntry> ConfigMigrator::getV1ToV2MigrationMap() {
    return {
        // Core mappings
        {"core.data_dir", "core.data_dir", "~/.yams/data", "Data storage directory", true},
        {"core.storage_engine", "storage.engine", "local", "Storage engine type", true},

        // Auth mappings (direct copy)
        {"auth.private_key_path", "auth.private_key_path", "", "Private key path", false},
        {"auth.public_key_path", "auth.public_key_path", "", "Public key path", false},
        {"auth.api_keys", "auth.api_keys", "", "API keys", false},

        // Compression mappings
        {"compression.enable", "compression.enable", "true", "Enable compression", false},
        {"compression.algorithm", "compression.algorithm", "zstd", "Compression algorithm", false},
        {"compression.zstd_level", "compression.zstd_level", "9", "ZSTD compression level", false},
        {"compression.lzma_level", "compression.lzma_level", "6", "LZMA compression level", false},
        {"compression.chunk_threshold", "compression.chunk_threshold", "1024",
         "Min size to compress", false},
        {"compression.always_compress_above", "compression.always_compress_above", "10485760",
         "Always compress above", false},
        {"compression.never_compress_below", "compression.never_compress_below", "1024",
         "Never compress below", false},
        {"compression.compress_after_days", "compression.compress_after_days", "1",
         "Days before compression", false},
        {"compression.archive_after_days", "compression.archive_after_days", "30",
         "Days before archiving", false},
        {"compression.max_concurrent_compressions", "compression.max_concurrent_compressions", "4",
         "Max parallel compressions", false},
        {"compression.async_compression", "compression.async_compression", "true",
         "Async compression", false},

        // Search.hybrid mappings
        {"search.hybrid.enable_kg", "search.hybrid.enable_kg", "true", "Enable knowledge graph",
         false},
        {"search.hybrid.vector_weight", "search.hybrid.vector_weight", "0.6",
         "Vector search weight", false},
        {"search.hybrid.keyword_weight", "search.hybrid.keyword_weight", "0.35",
         "Keyword search weight", false},
        {"search.hybrid.kg_entity_weight", "search.hybrid.kg_entity_weight", "0.03",
         "KG entity weight", false},
        {"search.hybrid.structural_weight", "search.hybrid.structural_weight", "0.02",
         "Structural weight", false},
        {"search.hybrid.kg_max_neighbors", "search.hybrid.kg_max_neighbors", "32",
         "Max KG neighbors", false},
        {"search.hybrid.kg_max_hops", "search.hybrid.kg_max_hops", "1", "Max KG hops", false},
        {"search.hybrid.kg_budget_ms", "search.hybrid.kg_budget_ms", "20", "KG time budget", false},
        {"search.hybrid.generate_explanations", "search.hybrid.generate_explanations", "true",
         "Generate explanations", false},

        // Knowledge graph mappings
        {"knowledge_graph.db_path", "knowledge_graph.db_path", "", "KG database path", false},
        {"knowledge_graph.enable_alias_fts", "knowledge_graph.enable_alias_fts", "true",
         "Enable FTS", false},
        {"knowledge_graph.enable_wal", "knowledge_graph.enable_wal", "true", "Enable WAL", false},
        {"knowledge_graph.node_cache_capacity", "knowledge_graph.node_cache_capacity", "10000",
         "Node cache size", false},
        {"knowledge_graph.alias_cache_capacity", "knowledge_graph.alias_cache_capacity", "50000",
         "Alias cache size", false},
        {"knowledge_graph.embedding_cache_capacity", "knowledge_graph.embedding_cache_capacity",
         "10000", "Embedding cache size", false},
        {"knowledge_graph.neighbor_cache_capacity", "knowledge_graph.neighbor_cache_capacity",
         "10000", "Neighbor cache size", false},
    };
}

std::map<std::string, std::map<std::string, std::string>> ConfigMigrator::getV2ConfigDefaults() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d");

    return {{"version",
             {{"config_version", "2"}, {"migrated_from", "1"}, {"migration_date", ss.str()}}},

            {"core",
             {{"data_dir", "~/.yams/data"},
              {"storage_engine", "local"},
              {"enable_wal", "true"},
              {"enable_telemetry", "false"},
              {"log_level", "info"}}},

            {"auth",
             {
                 // Auth settings are copied from existing config
             }},

            {"performance",
             {{"num_worker_threads", "4"},
              {"io_thread_pool_size", "2"},
              {"max_concurrent_operations", "100"},
              {"enable_memory_mapping", "false"},
              {"cache_size_mb", "512"}}},

            {"storage",
             {{"engine", "local"},
              {"base_path", "~/.yams/data"},
              {"enable_compression", "true"},
              {"enable_deduplication", "true"},
              {"enable_chunking", "true"},
              {"objects_dir", "objects"},
              {"staging_dir", "staging"}}},

            {"chunking",
             {{"window_size", "48"},
              {"min_chunk_size", "1024"},
              {"max_chunk_size", "16384"},
              {"average_chunk_size", "8192"},
              {"hash_algorithm", "sha256"}}},

            {"compression",
             {{"enable", "true"},
              {"algorithm", "zstd"},
              {"zstd_level", "9"},
              {"lzma_level", "6"},
              {"chunk_threshold", "1024"},
              {"always_compress_above", "10485760"},
              {"never_compress_below", "1024"},
              {"compress_after_days", "1"},
              {"archive_after_days", "30"},
              {"max_concurrent_compressions", "4"},
              {"async_compression", "true"},
              {"decompression_cache_size", "100"},
              {"decompression_cache_size_bytes", "536870912"}}},

            {"wal",
             {{"enable", "true"},
              {"wal_directory", "./wal"},
              {"max_log_size", "104857600"},
              {"sync_interval", "1000"},
              {"sync_timeout_ms", "100"},
              {"compress_old_logs", "true"},
              {"max_open_files", "10"},
              {"enable_group_commit", "true"}}},

            {"vector_database",
             {{"enable", "true"},
              {"database_path", "vectors.db"},
              {"table_name", "document_embeddings"},
              {"embedding_dim", "384"},
              {"index_type", "IVF_PQ"},
              {"num_partitions", "256"},
              {"num_sub_quantizers", "96"},
              {"enable_checkpoints", "true"},
              {"checkpoint_frequency", "1000"},
              {"max_batch_size", "1000"},
              {"default_similarity_threshold", "0.7"},
              {"use_in_memory", "false"},
              {"auto_create", "true"},
              {"auto_repair", "true"}}},

            {"embeddings",
             {{"enable", "true"},
              {"auto_generate", "false"},
              {"preferred_model", "all-MiniLM-L6-v2"},
              {"model_path", "~/.yams/models"},
              {"tokenizer_path", "models/tokenizer.json"},
              {"max_sequence_length", "512"},
              {"embedding_dim", "384"},
              {"batch_size", "32"},
              {"normalize_embeddings", "true"},
              {"enable_gpu", "false"},
              {"num_threads", "-1"},
              {"generation_delay_ms", "1000"},
              {"track_content_hash", "true"},
              {"model_version", "1.0.0"},
              {"embedding_schema_version", "1"},
              {"keep_model_hot", "true"},
              {"model_idle_timeout", "300"},
              {"preload_on_startup", "false"},
              {"max_model_memory_mb", "1024"}}},

            {"embeddings.cache",
             {{"enable_query_cache", "true"},
              {"cache_size", "1000"},
              {"cache_ttl_seconds", "3600"},
              {"lru_eviction", "true"}}},

            {"vector_index",
             {{"type", "HNSW"},
              {"dimension", "384"},
              {"distance_metric", "COSINE"},
              {"hnsw_m", "16"},
              {"hnsw_ef_construction", "200"},
              {"hnsw_ef_search", "50"},
              {"normalize_vectors", "true"},
              {"max_elements", "1000000"},
              {"enable_persistence", "true"},
              {"index_path", "vector_index.bin"},
              {"batch_size", "1000"},
              {"use_simd", "true"},
              {"num_threads", "0"},
              {"enable_delta_index", "true"},
              {"delta_threshold", "1000"}}},

            {"search",
             {{"default_limit", "10"},
              {"max_limit", "100"},
              {"enable_cache", "true"},
              {"cache_size", "1000"},
              {"cache_ttl_minutes", "60"}}},

            {"search.hybrid",
             {{"enable", "true"},
              {"enable_kg", "true"},
              {"vector_weight", "0.50"},
              {"keyword_weight", "0.30"},
              {"kg_entity_weight", "0.10"},
              {"structural_weight", "0.05"},
              {"classification_weight", "0.05"},
              {"vector_top_k", "50"},
              {"keyword_top_k", "50"},
              {"final_top_k", "10"},
              {"fusion_strategy", "LINEAR_COMBINATION"},
              {"rrf_k", "60.0"},
              {"enable_reranking", "true"},
              {"rerank_top_k", "20"},
              {"rerank_threshold", "0.0"},
              {"enable_query_expansion", "false"},
              {"expansion_terms", "5"},
              {"expansion_weight", "0.3"},
              {"kg_max_neighbors", "32"},
              {"kg_max_hops", "1"},
              {"kg_budget_ms", "20"},
              {"normalize_scores", "true"},
              {"generate_explanations", "true"},
              {"include_debug_scores", "false"},
              {"parallel_search", "true"}}},

            {"knowledge_graph",
             {{"db_path", "~/.yams/data/yams.db"},
              {"enable_alias_fts", "true"},
              {"enable_wal", "true"},
              {"prefer_exact_alias_first", "true"},
              {"default_limit", "1000"},
              {"node_cache_capacity", "10000"},
              {"alias_cache_capacity", "50000"},
              {"embedding_cache_capacity", "10000"},
              {"neighbor_cache_capacity", "10000"}}},

            {"file_detection",
             {{"use_lib_magic", "true"},
              {"use_builtin_patterns", "true"},
              {"use_custom_patterns", "true"},
              {"patterns_file", ""},
              {"max_bytes_to_read", "512"},
              {"cache_results", "true"},
              {"cache_size", "1000"}}},

            {"mcp_server",
             {{"enable", "false"},
              {"transport", "stdio"},
              {"host", "localhost"},
              {"port", "8080"},
              {"path", "/mcp"},
              {"use_ssl", "false"},
              {"connect_timeout_ms", "5000"},
              {"receive_timeout_ms", "30000"},
              {"max_message_size", "1048576"},
              {"enable_ping_pong", "true"},
              {"ping_interval_s", "30"}}},

            {"experimental", {{"enable", "false"}}},

            {"experimental.bert_ner",
             {{"enable", "false"},
              {"model_path", ""},
              {"model_name", "bert-base-NER"},
              {"batch_size", "16"},
              {"confidence_threshold", "0.8"},
              {"auto_tag", "false"},
              {"update_knowledge_graph", "false"},
              {"entity_types", "[\"PERSON\", \"ORG\", \"LOC\", \"MISC\"]"}}},

            {"experimental.auto_tagging",
             {{"enable", "false"},
              {"use_bert_ner", "false"},
              {"use_keyword_extraction", "true"},
              {"use_topic_modeling", "false"},
              {"min_tag_confidence", "0.7"},
              {"max_tags_per_document", "10"},
              {"tag_update_frequency_hours", "24"}}},

            {"experimental.smart_chunking",
             {{"enable", "false"},
              {"use_semantic_boundaries", "false"},
              {"use_sentence_detection", "true"},
              {"preserve_code_blocks", "true"},
              {"preserve_markdown_sections", "true"}}},

            {"daemon",
             {{"enable", "true"},
              {"auto_start", "true"},
              {"socket_path", "/tmp/yams-daemon.sock"},
              {"pid_file", "/tmp/yams-daemon.pid"},
              {"worker_threads", "4"},
              {"max_memory_gb", "4"},
              {"log_level", "info"},
              {"connect_timeout_ms", "1000"},
              {"request_timeout_ms", "5000"}}},

            {"daemon.models",
             {{"max_loaded_models", "4"},
              {"hot_pool_size", "2"},
              {"preload_models", "[\"all-MiniLM-L6-v2\", \"bart-large-mnli\"]"},
              {"lazy_loading", "false"},
              {"model_idle_timeout_s", "300"},
              {"enable_gpu", "false"},
              {"num_threads", "4"},
              {"eviction_policy", "lru"}}},

            {"daemon.resource_pool",
             {{"min_sessions", "1"},
              {"max_sessions", "3"},
              {"idle_timeout_s", "300"},
              {"validate_on_acquire", "true"}}},

            {"daemon.circuit_breaker",
             {{"enable", "true"},
              {"failure_threshold", "5"},
              {"success_threshold", "2"},
              {"open_timeout_s", "30"},
              {"half_open_timeout_s", "10"}}},

            {"daemon.client",
             {{"use_daemon", "true"},
              {"fallback_to_local", "true"},
              {"max_retries", "3"},
              {"batch_size", "32"}}},

            {"migrations",
             {{"auto_migrate", "true"},
              {"backup_before_migrate", "true"},
              {"migration_log_path", "migrations.log"}}},

            {"classification",
             {{"enable", "false"},
              {"model_path", "~/.yams/models/bart-large-mnli"},
              {"confidence_threshold", "0.7"},
              {"max_tags_per_document", "5"},
              {"max_sequence_length", "1024"},
              {"enable_batch_processing", "true"},
              {"batch_size", "16"},
              {"inference_timeout_ms", "5000"},
              {"auto_classify_on_index", "true"},
              {"enable_search_time_classification", "false"}}},

            {"search.hotzones",
             {{"enable", "false"},
              {"decay_factor", "0.95"},
              {"min_frequency", "3"},
              {"decay_interval_hours", "24"},
              {"max_boost_factor", "2.0"},
              {"enable_persistence", "true"},
              {"data_file", "~/.yams/data/hotzones.db"},
              {"decay_processing_interval_minutes", "60"},
              {"enable_shared_hotzones", "false"}}},

            {"search.enhanced",
             {{"enable", "false"},
              {"classification_weight", "0.15"},
              {"kg_expansion_weight", "0.10"},
              {"hotzone_weight", "0.05"},
              {"enable_cross_domain", "true"},
              {"max_expansion_concepts", "10"},
              {"enable_concept_reranking", "true"},
              {"enhanced_search_timeout_ms", "2000"}}},
            {"downloader",
             {{"default_concurrency", "4"},
              {"default_chunk_size_bytes", "8388608"},
              {"default_timeout_ms", "60000"},
              {"follow_redirects", "true"},
              {"resume", "true"},
              {"rate_limit_global_bps", "0"},
              {"rate_limit_per_conn_bps", "0"},
              {"max_retry_attempts", "5"},
              {"retry_backoff_ms", "500"},
              {"retry_backoff_multiplier", "2.0"},
              {"retry_max_backoff_ms", "15000"},
              {"checksum_algo", "sha256"},
              {"temp_extension", ".part"},
              {"store_only", "true"},
              {"max_file_bytes", "0"}}},
            {"downloader.tls", {{"verify", "true"}, {"ca_path", ""}}},
            {"downloader.proxy", {{"url", ""}}}};
}

Result<std::map<std::string, std::map<std::string, std::string>>>
ConfigMigrator::parseTomlConfig(const fs::path& path) {
    std::map<std::string, std::map<std::string, std::string>> config;
    std::ifstream file(path);
    if (!file) {
        return Error{ErrorCode::FileNotFound, "Cannot open config file: " + path.string()};
    }

    std::string line;
    std::string currentSection;

    while (std::getline(file, line)) {
        // Skip comments and empty lines
        if (line.empty() || line[0] == '#')
            continue;

        // Trim whitespace
        line.erase(0, line.find_first_not_of(" \t"));
        line.erase(line.find_last_not_of(" \t") + 1);

        if (line.empty() || line[0] == '#')
            continue;

        // Check for section headers
        if (line[0] == '[') {
            size_t end = line.find(']');
            if (end != std::string::npos) {
                currentSection = line.substr(1, end - 1);
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

            // Handle arrays
            if (value[0] == '[') {
                // Keep as-is for now
            }

            config[currentSection][key] = value;
        }
    }

    return config;
}

Result<void> ConfigMigrator::writeTomlConfig(
    const fs::path& path, const std::map<std::string, std::map<std::string, std::string>>& config,
    const ConfigVersion& version) {
    std::ofstream file(path);
    if (!file) {
        return Error{ErrorCode::WriteError, "Cannot write to config file: " + path.string()};
    }

    // Write header
    file << "# YAMS v" << version.toString() << " Configuration\n";
    file << "# Generated on: ";
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    file << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S") << "\n\n";

    // Write sections in a logical order
    std::vector<std::string> sectionOrder = {"version",
                                             "core",
                                             "auth",
                                             "performance",
                                             "storage",
                                             "chunking",
                                             "compression",
                                             "wal",
                                             "vector_database",
                                             "embeddings",
                                             "embeddings.cache",
                                             "vector_index",
                                             "search",
                                             "search.hybrid",
                                             "knowledge_graph",
                                             "file_detection",
                                             "downloader",
                                             "downloader.tls",
                                             "downloader.proxy",
                                             "mcp_server",
                                             "daemon",
                                             "daemon.models",
                                             "daemon.resource_pool",
                                             "daemon.circuit_breaker",
                                             "daemon.client",
                                             "experimental",
                                             "experimental.bert_ner",
                                             "experimental.auto_tagging",
                                             "experimental.smart_chunking",
                                             "classification",
                                             "search.hotzones",
                                             "search.enhanced",
                                             "migrations"};

    std::set<std::string> written;

    for (const auto& section : sectionOrder) {
        if (config.find(section) != config.end()) {
            file << "[" << section << "]\n";

            for (const auto& [key, value] : config.at(section)) {
                // Handle special formatting for certain values
                if (value.empty()) {
                    file << "# " << key << " = \"\"\n";
                } else if (value[0] == '[') {
                    // Array value
                    file << key << " = " << value << "\n";
                } else if (value == "true" || value == "false" ||
                           std::all_of(value.begin(), value.end(), ::isdigit) ||
                           (value.find('.') != std::string::npos &&
                            std::all_of(value.begin(), value.end(),
                                        [](char c) { return ::isdigit(c) || c == '.'; }))) {
                    // Boolean or numeric value
                    file << key << " = " << value << "\n";
                } else {
                    // String value
                    file << key << " = \"" << value << "\"\n";
                }
            }
            file << "\n";
            written.insert(section);
        }
    }

    // Append any remaining sections not covered by the canonical order (e.g., cli.*)
    for (const auto& [section, kv] : config) {
        if (written.find(section) != written.end())
            continue;
        file << "[" << section << "]\n";
        for (const auto& [key, value] : kv) {
            if (value.empty()) {
                file << "# " << key << " = \"\"\n";
            } else if (!value.empty() && value[0] == '[') {
                file << key << " = " << value << "\n";
            } else if (value == "true" || value == "false" ||
                       std::all_of(value.begin(), value.end(), ::isdigit) ||
                       (value.find('.') != std::string::npos &&
                        std::all_of(value.begin(), value.end(),
                                    [](char c) { return ::isdigit(c) || c == '.'; }))) {
                file << key << " = " << value << "\n";
            } else {
                file << key << " = \"" << value << "\"\n";
            }
        }
        file << "\n";
    }

    return Result<void>();
}

Result<fs::path> ConfigMigrator::createBackup(const fs::path& configPath) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << configPath.stem().string() << ".backup.";
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");
    ss << configPath.extension().string();

    fs::path backupPath = configPath.parent_path() / ss.str();

    try {
        fs::copy(configPath, backupPath);
        return backupPath;
    } catch (const std::exception& e) {
        return Error{ErrorCode::WriteError, "Failed to create backup: " + std::string(e.what())};
    }
}

std::map<std::string, std::map<std::string, std::string>> ConfigMigrator::mergeConfigs(
    const std::map<std::string, std::map<std::string, std::string>>& oldConfig,
    const std::map<std::string, std::map<std::string, std::string>>& newDefaults) {
    auto merged = newDefaults;
    auto migrationMap = getV1ToV2MigrationMap();

    // Apply migration mappings
    for (const auto& entry : migrationMap) {
        // Parse old key into section and key
        size_t dot = entry.old_key.find('.');
        if (dot != std::string::npos) {
            std::string oldSection = entry.old_key.substr(0, dot);
            std::string oldKey = entry.old_key.substr(dot + 1);

            // Check if old config has this value
            if (oldConfig.find(oldSection) != oldConfig.end()) {
                const auto& section = oldConfig.at(oldSection);
                if (section.find(oldKey) != section.end()) {
                    // Parse new key
                    dot = entry.new_key.find('.');
                    if (dot != std::string::npos) {
                        std::string newSection = entry.new_key.substr(0, dot);
                        std::string newKey = entry.new_key.substr(dot + 1);

                        // Copy value to new location
                        merged[newSection][newKey] = section.at(oldKey);
                        logMigration("migrate_key", entry.old_key + " -> " + entry.new_key + " = " +
                                                        section.at(oldKey));
                    }
                }
            }
        }
    }

    // Special handling for data_dir to set storage.base_path
    if (merged["core"].find("data_dir") != merged["core"].end()) {
        merged["storage"]["base_path"] = merged["core"]["data_dir"];
    }

    // Special handling for knowledge_graph.db_path
    if (merged["knowledge_graph"]["db_path"].empty() &&
        merged["core"].find("data_dir") != merged["core"].end()) {
        merged["knowledge_graph"]["db_path"] = merged["core"]["data_dir"] + "/yams.db";
    }

    return merged;
}

void ConfigMigrator::logMigration(const std::string& action, const std::string& details) {
    spdlog::debug("[CONFIG_MIGRATION] {}: {}", action, details);
}

Result<void> ConfigMigrator::validateV2Config(const fs::path& configPath) {
    auto parseResult = parseTomlConfig(configPath);
    if (!parseResult) {
        return Error{parseResult.error()};
    }

    const auto& config = parseResult.value();

    // Check for required sections
    std::vector<std::string> requiredSections = {"version", "core", "storage"};

    for (const auto& section : requiredSections) {
        if (config.find(section) == config.end()) {
            return Error{ErrorCode::InvalidData, "Missing required section: " + section};
        }
    }

    // Check version
    if (config.find("version") != config.end()) {
        const auto& version = config.at("version");
        if (version.find("config_version") == version.end() ||
            version.at("config_version") != "2") {
            return Error{ErrorCode::InvalidData, "Invalid config version"};
        }
    }

    // Validate numeric ranges
    auto validateRange = [](const std::string& value, int min, int max) -> bool {
        try {
            int v = std::stoi(value);
            return v >= min && v <= max;
        } catch (...) {
            return false;
        }
    };

    // Check compression levels
    if (config.find("compression") != config.end()) {
        const auto& comp = config.at("compression");
        if (comp.find("zstd_level") != comp.end()) {
            if (!validateRange(comp.at("zstd_level"), 1, 22)) {
                return Error{ErrorCode::InvalidData, "Invalid zstd_level (must be 1-22)"};
            }
        }
        if (comp.find("lzma_level") != comp.end()) {
            if (!validateRange(comp.at("lzma_level"), 0, 9)) {
                return Error{ErrorCode::InvalidData, "Invalid lzma_level (must be 0-9)"};
            }
        }
    }

    return Result<void>();
}

Result<std::vector<std::string>>
ConfigMigrator::updateV2SchemaAdditive(const fs::path& configPath, bool makeBackup, bool dryRun) {
    if (!fs::exists(configPath)) {
        return Error{ErrorCode::FileNotFound, "Config file not found: " + configPath.string()};
    }

    auto parseResult = parseTomlConfig(configPath);
    if (!parseResult) {
        return Error{parseResult.error()};
    }
    auto config = parseResult.value();

    auto additives = getV2AdditiveDefaults();
    std::vector<std::string> addedKeys;

    // Determine missing keys
    for (const auto& [section, kv] : additives) {
        auto& destSection = config[section]; // creates if missing (used later only if we add)
        bool sectionExists = (parseResult.value().find(section) != parseResult.value().end());
        for (const auto& [k, v] : kv) {
            bool missing = true;
            if (sectionExists) {
                const auto& existingSection = parseResult.value().at(section);
                missing = (existingSection.find(k) == existingSection.end());
            }
            if (missing) {
                addedKeys.emplace_back(section + "." + k);
                // Stage value
                destSection[k] = v;
            }
        }
    }

    if (addedKeys.empty()) {
        return addedKeys; // No changes needed
    }

    if (dryRun) {
        return addedKeys;
    }

    // Backup if requested
    if (makeBackup) {
        auto backup = this->createBackup(configPath);
        if (!backup) {
            return Error{backup.error()};
        }
        spdlog::info("Created backup before update: {}", backup.value().string());
    }

    // Update version metadata
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&t), "%Y-%m-%d");
    config["version"]["config_version"] = "2"; // ensure v2
    config["version"]["migration_date"] = ss.str();
    if (config["version"].find("migrated_from") == config["version"].end()) {
        config["version"]["migrated_from"] = "2";
    }

    // Write updated config
    ConfigVersion v2;
    v2.major = 2;
    v2.minor = 0;
    v2.patch = 0;
    auto writeResult = writeTomlConfig(configPath, config, v2);
    if (!writeResult) {
        return Error{writeResult.error()};
    }

    spdlog::info("Config updated additively with {} keys", addedKeys.size());
    return addedKeys;
}

} // namespace yams::config