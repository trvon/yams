#pragma once

#include <yams/core/types.h>
#include <yams/search/search_engine.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>

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

// Forward declare SearchTuner
class SearchTuner;

/**
 * SearchEngineBuilder
 *
 * Central builder for composing a SearchEngine in a single place so both CLI
 * and MCP server can share composition logic. Handles:
 * - Wiring VectorDatabase + MetadataRepository
 * - Optional KG scorer injection when a KnowledgeGraphStore is available
 * - Conservative, enabled-by-default settings
 *
 * Remote/hybrid failover is scoped via Mode and RemoteConfig (client implementation
 * can be provided separately).
 */
class SearchEngineBuilder {
public:
    enum class Mode {
        Embedded, // Always use in-process SearchEngine
        Remote,   // Always call remote search service
        Hybrid    // Prefer embedded; fail over to remote
    };

    struct RemoteConfig {
        std::string base_url = "http://127.0.0.1:8081";
        std::string health_path = "/health";
        std::chrono::milliseconds timeout{1500};
        bool enable_failover = true;
    };

    struct BuildOptions {
        Mode mode = Mode::Embedded;
        RemoteConfig remote{};

        // SearchEngine configuration with conservative defaults
        SearchEngineConfig config{};

        // Auto-tune: when true, use SearchTuner to select optimal parameters
        // based on corpus statistics. When false, use the provided config as-is.
        // Default: true (enabled)
        bool autoTune = true;

        // Convenience: default-initialize to tuned conservative config
        static BuildOptions makeDefault() {
            BuildOptions o{};
            // Conservative weight distribution preserving existing relevance behavior
            // These weights match a balanced MIXED corpus profile
            o.config.textWeight = 0.45f; // Unified FTS5+symbol text search
            o.config.pathTreeWeight = 0.10f;
            o.config.kgWeight = 0.05f;
            o.config.vectorWeight = 0.30f;
            o.config.tagWeight = 0.03f;
            o.config.metadataWeight = 0.02f;

            o.config.corpusProfile = SearchEngineConfig::CorpusProfile::MIXED;
            o.config.fusionStrategy = SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK;
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

    // Build according to requested mode. For Remote/Hybrid, returns an embedded engine when
    // possible. A remote client/facade can be layered on top by the caller if desired.
    Result<std::shared_ptr<SearchEngine>>
    buildWithMode(const BuildOptions& options = BuildOptions::makeDefault()) {
        // For now, builder returns the embedded engine even in Hybrid/Remote modes.
        // The remote facade/client is composed at a higher layer.
        return buildEmbedded(options);
    }

private:
    std::shared_ptr<yams::vector::VectorDatabase> vectorDatabase_;
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<yams::vector::EmbeddingGenerator> embeddingGenerator_;
};

} // namespace yams::search
