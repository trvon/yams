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

    struct TopologyRoutingPolicy {
        std::optional<bool> enableWeakQueryRouting;
        std::optional<std::size_t> maxClusters;
        std::optional<std::size_t> maxDocs;
        std::optional<float> medoidBoost;
        std::optional<float> bridgeBoost;
        std::optional<float> routedBaseMultiplier;
        std::optional<std::string> routingVariant;
        std::optional<std::string> integration;
        std::optional<std::size_t> recallExpandPerCluster;
        std::optional<float> rrfK;
        std::optional<std::string> routeScoring;
    };

    struct TopologyEnginePolicy {
        std::optional<std::string> engine;
        std::optional<std::size_t> hdbscanMinPoints;
        std::optional<std::size_t> hdbscanMinClusterSize;
        std::optional<std::size_t> featureSmoothingHops;
    };

    // Per-corpus adaptive tuner for the topology layer (Phase G). Disabled
    // by default; opt-in via [topology.tuner] in TOML. Reward weights default
    // to the Phase G plan values when not overridden.
    struct TopologyTunerPolicy {
        std::optional<bool> enabled;
        std::optional<std::uint32_t> cooldownMinutes;
        std::optional<std::size_t> docCountDelta;
        std::optional<double> rewardAlphaSingleton;
        std::optional<double> rewardBetaGiantCluster;
        std::optional<double> rewardGammaGiniDeviation;
        std::optional<double> rewardDeltaIntraEdge;
    };

    struct PostIngestCaps {
        std::optional<std::uint32_t> totalConcurrent;
        std::optional<std::uint32_t> embedConcurrent;
        std::optional<std::uint32_t> extractionConcurrent;
        std::optional<std::uint32_t> kgConcurrent;
        std::optional<std::uint32_t> symbolConcurrent;
        std::optional<std::uint32_t> entityConcurrent;
        std::optional<std::uint32_t> titleConcurrent;
    };

    struct SimeonEncoderPolicy {
        std::optional<std::string> ngramMode;
        std::optional<std::uint32_t> ngramMin;
        std::optional<std::uint32_t> ngramMax;
        std::optional<std::uint32_t> sketchDim;
        std::optional<std::uint32_t> outputDim;
        std::optional<std::string> projection;
        std::optional<bool> l2Normalize;
        std::optional<std::uint32_t> pqBytes;
    };

    struct SimeonBm25Policy {
        std::optional<bool> enabled;
        std::optional<std::string> variant;
        std::optional<float> subwordGamma;
        std::optional<std::size_t> maxCorpusDocs;
        // Per-query router over {Atire, SabSmooth}. When enabled, the
        // backend builds both indexes and dispatches via simeon::QueryRouter
        // using `routerPreset`. Recognized presets: "passE_scq0_clar3"
        // (default — best BEIR result from simeon bench), "off" (disable).
        std::optional<bool> routerEnabled;
        std::optional<std::string> routerPreset;
    };

    struct RerankerBackendPolicy {
        std::optional<std::string> backend;
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
     * @brief Resolve embedding backend selection.
     *
     * Priority:
     *   1. YAMS_EMBED_BACKEND environment variable (highest)
     *   2. `embeddings.backend` key in TOML config
     *   3. defaultValue fallback
     *
     * Recognized values: "auto", "daemon", "simeon", "mock".
     * "auto" means: let PluginManager pick ABI plugin first, fall back to any
     * registered in-process provider.
     *
     * @param defaultValue Value returned when neither env nor TOML is set.
     * @return Lower-cased backend name.
     */
    static std::string resolveEmbeddingBackend(const std::string& defaultValue = "simeon");

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
     * @brief Resolve topology-aware routing policy from config file.
     *
     * Reads [search.topology] keys. Callers apply these as defaults, then
     * overlay env vars (YAMS_SEARCH_TOPOLOGY_*) which take final precedence.
     * All fields are optional; unset keys are left nullopt.
     *
     * Config keys:
     * - search.topology.enable_weak_query_routing = true|false
     * - search.topology.max_clusters = int
     * - search.topology.max_docs = int
     * - search.topology.medoid_boost = float
     * - search.topology.bridge_boost = float
     * - search.topology.routed_base_multiplier = float
     * - search.topology.routing_variant = baseline|vector_seed|kg_walk|score_replace|medoid_promote
     * - search.topology.integration = boost|recall_expand|rrf|both
     * - search.topology.recall_expand_per_cluster = int
     * - search.topology.rrf_k = float
     * - search.topology.route_scoring = current|size_weighted|seed_coverage
     */
    static TopologyRoutingPolicy resolveTopologyRoutingPolicy();

    /**
     * @brief Resolve topology cluster-engine selection from config file.
     *
     * Reads [topology] keys. Callers apply `engine` to
     * TuningConfig::topologyAlgorithm (which seeds topology::makeEngine
     * dispatch) and the hdbscan_* knobs to TopologyBuildConfig.
     *
     * Config keys:
     * - topology.engine = connected|hdbscan
     * - topology.hdbscan_min_points = int (0 = auto from corpus size)
     * - topology.hdbscan_min_cluster_size = int (0 = auto from corpus size)
     * - topology.feature_smoothing_hops = int (0 = off; SGC K for embedding
     *   propagation over the semantic-neighbor graph before clustering)
     */
    static TopologyEnginePolicy resolveTopologyEnginePolicy();

    /**
     * @brief Resolve topology corpus-adaptive tuner config (Phase G).
     *
     * Disabled by default. Recognized TOML keys:
     * - topology.tuner.enabled                     = bool
     * - topology.tuner.cooldown_minutes            = int
     * - topology.tuner.doc_count_delta             = int
     * - topology.tuner.reward.alpha_singleton      = float
     * - topology.tuner.reward.beta_giant_cluster   = float
     * - topology.tuner.reward.gamma_gini_deviation = float
     * - topology.tuner.reward.delta_intra_edge     = float
     */
    static TopologyTunerPolicy resolveTopologyTunerPolicy();

    /**
     * @brief Resolve Simeon encoder config from env + config file.
     *
     * Precedence: env var > TOML > default. Env vars map to legacy
     * YAMS_SIMEON_* names kept for test-only overrides. Canonical keys:
     * - embeddings.simeon.ngram_mode     = "char" | "word" | "char_and_word"
     * - embeddings.simeon.ngram_min      = int
     * - embeddings.simeon.ngram_max      = int
     * - embeddings.simeon.sketch_dim     = int (power of two for fwht)
     * - embeddings.simeon.output_dim     = int
     * - embeddings.simeon.projection     = "achlioptas_sparse" | "dense_gaussian"
     *                                    | "very_sparse" | "sparse_jl" | "fwht"
     * - embeddings.simeon.l2_normalize   = bool
     * - embeddings.simeon.pq_bytes       = int (0 = off; reserved for
     *                                          post-encode PQ in storage layer)
     */
    static SimeonEncoderPolicy resolveSimeonEncoderPolicy();

    /**
     * @brief Resolve Simeon BM25 reranker config from env + config file.
     *
     * Precedence: env var > TOML > default. Canonical keys:
     * - embeddings.simeon.bm25.enabled         = bool (default true when
     *                                                 backend=simeon)
     * - embeddings.simeon.bm25.variant         = "atire" | "sab_smooth"
     * - embeddings.simeon.bm25.subword_gamma   = float
     * - embeddings.simeon.bm25.max_corpus_docs = int
     */
    static SimeonBm25Policy resolveSimeonBm25Policy();

    /**
     * @brief Resolve reranker backend selection from config.
     *
     * Canonical key:
     * - search.reranker_backend = "simeon" | "onnx" | "colbert" | "auto"
     *
     * When unset, callers should default to "simeon".
     */
    static RerankerBackendPolicy resolveRerankerBackendPolicy();
    static RerankerBackendPolicy resolveRerankerBackendPolicy(const DaemonConfig& config);

    /**
     * @brief Resolve post-ingest concurrency caps from config file.
     *
     * Reads [tuning.post_ingest] keys. Each entry is optional; callers apply
     * values via TuneAdvisor::setPost*Concurrent() only when the corresponding
     * YAMS_POST_*_CONCURRENT env var is not set (env wins).
     *
     * Config keys:
     * - tuning.post_ingest.total_concurrent    = int (1..256)
     * - tuning.post_ingest.embed_concurrent    = int (1..32)
     * - tuning.post_ingest.extraction_concurrent = int (1..64)
     * - tuning.post_ingest.kg_concurrent       = int (1..64)
     * - tuning.post_ingest.symbol_concurrent   = int (1..32)
     * - tuning.post_ingest.entity_concurrent   = int (1..16)
     * - tuning.post_ingest.title_concurrent    = int (1..16)
     */
    static PostIngestCaps resolvePostIngestCaps();

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
