// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <filesystem>
#include <map>
#include <optional>
#include <string>

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
    static std::map<std::string, std::string> parseSimpleTomlFlat(const std::filesystem::path& path);

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
