#pragma once

#include <yams/core/types.h>
#include <yams/search/search_engine.h>

#include <yams/search/search_tuner.h>
#include <yams/search/simeon_lexical_backend.h>

#include <filesystem>
#include <memory>
#include <optional>

// Forward declarations
namespace yams::storage {
struct CorpusStats;
}

namespace yams {
namespace metadata {
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace metadata

namespace vector {
class VectorDatabase;
class EmbeddingGenerator;
} // namespace vector
} // namespace yams

namespace yams::search {

/**
 * SearchEngineBuilder
 *
 * Central builder for composing a SearchEngine in a single place so both CLI
 * and MCP server can share composition logic. Handles:
 * - Wiring VectorDatabase + MetadataRepository
 * - Optional KG scorer injection when a KnowledgeGraphStore is available
 * - Conservative, enabled-by-default settings
 *
 */
class SearchEngineBuilder {
public:
    struct BuildOptions {
        // SearchEngine configuration with conservative defaults
        SearchEngineConfig config{};

        // Auto-tune: when true, use SearchTuner to select optimal parameters
        // based on corpus statistics. When false, use the provided config as-is.
        // Default: true (enabled)
        bool autoTune = true;

        // Override tuning state: when set, forces use of this tuning state instead
        // of auto-detecting from corpus stats. Useful for benchmarks with known
        // corpus types (e.g., SCIENTIFIC for BEIR datasets) where auto-detection
        // may fail due to absolute path depth calculations.
        std::optional<TuningState> tuningStateOverride = std::nullopt;

        // Persist adaptive tuner EWMA state to this path across process restarts.
        // When set, the builder loads prior state at construction time and enables
        // throttled auto-save from SearchTuner::observe(). Empty => disabled.
        std::filesystem::path tunerStatePath{};

        // Optional first-class lexical backend injection. When set, the builder
        // installs Simeon lexical rescoring before returning the engine.
        std::optional<SimeonLexicalBackend::Config> simeonLexicalConfig{};

        // Convenience: default-initialize to tuned conservative config
        static BuildOptions makeDefault() {
            BuildOptions o{};
            // Use the mixed-precision tuned profile as the non-auto-tune fallback.
            // Auto-tune remains enabled and will still specialize per corpus stats.
            const auto params = getTunedParams(TuningState::MIXED_PRECISION);
            params.applyTo(o.config);
            o.config.corpusProfile = SearchEngineConfig::CorpusProfile::CUSTOM;
            o.config.enableParallelExecution = true;
            o.config.includeDebugInfo = true;
            o.config.maxResults = 100;
            o.autoTune = true;
            return o;
        }

        // Create options with auto-tune disabled (use static config)
        static BuildOptions withoutAutoTune() {
            BuildOptions o = makeDefault();
            o.autoTune = false;
            return o;
        }
    };

public:
    SearchEngineBuilder() = default;

    SearchEngineBuilder& withVectorDatabase(std::shared_ptr<yams::vector::VectorDatabase> vdb) {
        vectorDatabase_ = std::move(vdb);
        return *this;
    }

    SearchEngineBuilder&
    withMetadataRepo(std::shared_ptr<yams::metadata::MetadataRepository> repo) {
        metadataRepo_ = std::move(repo);
        return *this;
    }

    SearchEngineBuilder& withKGStore(std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg) {
        kgStore_ = std::move(kg);
        return *this;
    }

    SearchEngineBuilder&
    withEmbeddingGenerator(std::shared_ptr<yams::vector::EmbeddingGenerator> gen) {
        embeddingGenerator_ = std::move(gen);
        return *this;
    }

    // Build embedded engine only (no remote fallback)
    Result<std::shared_ptr<SearchEngine>>
    buildEmbedded(const BuildOptions& options = BuildOptions::makeDefault());

private:
    std::shared_ptr<yams::vector::VectorDatabase> vectorDatabase_;
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<yams::vector::EmbeddingGenerator> embeddingGenerator_;
};

} // namespace yams::search
