// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include <filesystem>
#include <map>
#include <optional>
#include <string>

#include <yams/vector/document_chunker.h>

namespace yams::daemon {

struct DaemonConfig; // Forward declaration

/**
 * @brief Static utility class for configuration resolution and parsing.
 *
 * Extracted from ServiceManager (PBI-088) to centralize config-related helpers.
 * All methods are static and thread-safe.
 *
 * ## Responsibilities
 * - Resolve default config file paths (XDG, HOME, env overrides)
 * - Parse simple TOML files into flat key-value maps
 * - Environment variable helpers (truthy checks)
 * - Vector sentinel file I/O
 * - Embedding dimension detection
 */
class ConfigResolver {
public:
    struct EmbeddingSelectionPolicy {
        enum class Strategy { Ranked, IntroHeadings };
        enum class Mode { Full, Budgeted, Adaptive };
        Strategy strategy{Strategy::Ranked};
        Mode mode{Mode::Budgeted};
        std::size_t maxChunksPerDoc{8};
        std::size_t maxCharsPerDoc{24000};
        double headingBoost{1.25};
        double introBoost{0.75};
    };

    struct EmbeddingChunkingPolicy {
        yams::vector::ChunkingStrategy strategy{yams::vector::ChunkingStrategy::PARAGRAPH_BASED};
        yams::vector::ChunkingConfig config{};
        bool overridden{false};
    };

    ConfigResolver() = delete; // Static-only class

    /**
     * @brief Check if an environment variable value is "truthy".
     *
     * Returns true for any value except: empty, "0", "false", "off", "no" (case-insensitive).
     *
     * @param value Environment variable value (may be nullptr)
     * @return true if value is truthy, false otherwise
     */
    static bool envTruthy(const char* value);

    /**
     * @brief Resolve the default config file path.
     *
     * Search order:
     * 1. YAMS_CONFIG_PATH environment variable
     * 2. $XDG_CONFIG_HOME/yams/config.toml
     * 3. $HOME/.config/yams/config.toml
     *
     * @return Path to config file if found, empty path otherwise
     */
    static std::filesystem::path resolveDefaultConfigPath();

    /**
     * @brief Parse a simple TOML file into a flat key-value map.
     *
     * Supports basic TOML features:
     * - [section] headers (flattened as "section.key")
     * - key = "value" assignments
     * - # comments
     *
     * Does NOT support: nested tables, arrays, multi-line strings.
     *
     * @param path Path to TOML file
     * @return Map of flattened keys to values
     */
    static std::map<std::string, std::string>
    parseSimpleTomlFlat(const std::filesystem::path& path);

    /**
     * @brief Read embedding dimension from vector database.
     *
     * Opens the sqlite-vec database and reads stored embedding dimension.
     *
     * @param dbPath Path to vectors.db
     * @return Dimension if found and > 0, nullopt otherwise
     */
    static std::optional<size_t> readDbEmbeddingDim(const std::filesystem::path& dbPath);

    /**
     * @brief Read embedding dimension from sentinel file.
     *
     * Reads from dataDir/vectors_sentinel.json.
     *
     * @param dataDir Data directory containing sentinel file
     * @return Dimension if found and > 0, nullopt otherwise
     */
    static std::optional<size_t> readVectorSentinelDim(const std::filesystem::path& dataDir);

    /**
     * @brief Write vector sentinel file with dimension and schema info.
     *
     * Creates dataDir/vectors_sentinel.json with embedding metadata.
     *
     * @param dataDir Data directory for sentinel file
     * @param dim Embedding dimension
     * @param tableName Table name (for documentation)
     * @param schemaVersion Schema version
     */
    static void writeVectorSentinel(const std::filesystem::path& dataDir, size_t dim,
                                    const std::string& tableName, int schemaVersion);

    /**
     * @brief Resolve the preferred embedding model name from env/config/data dir.
     *
     * Precedence:
     * 1. Environment variable YAMS_PREFERRED_MODEL (if non-empty)
     * 2. Config file key embeddings.preferred_model
     * 3. First entry in daemon.models.preload_models (if present)
     * 4. First model found under resolvedDataDir/models (if any)
     *
     * @param config Daemon configuration (used for config file path)
     * @param resolvedDataDir Data directory resolved by the daemon (for auto-detect)
     * @return Preferred model name or empty string if none found
     */
    static std::string resolvePreferredModel(const DaemonConfig& config,
                                             const std::filesystem::path& resolvedDataDir);

    /**
     * @brief Resolve the preferred reranker model name from env/config.
     *
     * Precedence:
     * 1. Environment variable YAMS_RERANKER_MODEL (if non-empty)
     * 2. Config file key search.reranker_model
     *
     * @param config Daemon configuration (used for config file path)
     * @return Reranker model name or empty string if none found
     */
    static std::string resolveRerankerModel(const DaemonConfig& config);

    /**
     * @brief Determine if symbol extraction plugins should be enabled.
     *
     * Reads plugins.symbol_extraction.enable from config.toml when present;
     * defaults to true when unset or on parse errors.
     */
    static bool isSymbolExtractionEnabled(const DaemonConfig& config);

    /**
     * @brief Detect if embedding preload on startup is configured.
     *
     * Checks config file (embeddings.preload_on_startup) and
     * environment override (YAMS_EMBED_PRELOAD_ON_STARTUP).
     *
     * @param config Daemon configuration
     * @return true if preload is enabled
     */
    static bool detectEmbeddingPreloadFlag(const DaemonConfig& config);

    /**
     * @brief Resolve embedding chunk-selection policy from config with env overrides.
     *
     * Config keys:
    * - embeddings.selection.strategy = ranked|intro_headings
     * - embeddings.selection.mode = full|budgeted|adaptive
     * - embeddings.selection.max_chunks_per_doc = int
     * - embeddings.selection.max_chars_per_doc = int
     * - embeddings.selection.heading_boost = float
     * - embeddings.selection.intro_boost = float
     *
     * Environment overrides:
    * - YAMS_EMBED_SELECTION_STRATEGY
     * - YAMS_EMBED_SELECTION_MODE
     * - YAMS_EMBED_MAX_CHUNKS_PER_DOC
     * - YAMS_EMBED_MAX_CHARS_PER_DOC
     * - YAMS_EMBED_SELECTION_HEADING_BOOST
     * - YAMS_EMBED_SELECTION_INTRO_BOOST
     */
    static EmbeddingSelectionPolicy resolveEmbeddingSelectionPolicy();

    /**
     * @brief Resolve embedding chunking policy from config with env overrides.
     *
     * Config keys:
     * - embeddings.chunking.strategy = fixed|sentence|paragraph|recursive|sliding_window|markdown
     * - embeddings.chunking.use_tokens = 0|1
     * - embeddings.chunking.target = int
     * - embeddings.chunking.max = int
     * - embeddings.chunking.min = int
     * - embeddings.chunking.overlap = int
     * - embeddings.chunking.overlap_pct = float (0..1)
     * - embeddings.chunking.preserve_sentences = 0|1
     *
     * Environment overrides (backwards-compatible):
     * - YAMS_EMBED_CHUNK_STRATEGY
     * - YAMS_EMBED_CHUNK_USE_TOKENS
     * - YAMS_EMBED_CHUNK_TARGET
     * - YAMS_EMBED_CHUNK_MAX
     * - YAMS_EMBED_CHUNK_MIN
     * - YAMS_EMBED_CHUNK_OVERLAP
     * - YAMS_EMBED_CHUNK_OVERLAP_PCT
     * - YAMS_EMBED_CHUNK_PRESERVE_SENTENCES
     */
    static EmbeddingChunkingPolicy resolveEmbeddingChunkingPolicy();

    /**
     * @brief Read an integer timeout from environment with bounds.
     *
     * @param envName Environment variable name
     * @param defaultMs Default value in milliseconds
     * @param minMs Minimum allowed value
     * @return Timeout in milliseconds
     */
    static int readTimeoutMs(const char* envName, int defaultMs, int minMs);

    /**
     * @brief Read vector index max_elements from config/env.
     *
     * Checks in order:
     * 1. YAMS_VECTOR_MAX_ELEMENTS environment variable
     * 2. vector_database.max_elements from config file
     * 3. Default value (100000)
     *
     * @return Maximum number of vectors for the index
     */
    static size_t readVectorMaxElements();
};

} // namespace yams::daemon
