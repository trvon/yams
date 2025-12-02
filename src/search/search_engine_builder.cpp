#include <yams/search/search_engine_builder.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

#include <spdlog/spdlog.h>

namespace yams::search {

// ------------------------------
// SearchEngineBuilder
// ------------------------------

Result<std::shared_ptr<SearchEngine>>
SearchEngineBuilder::buildEmbedded(const BuildOptions& options) {
    spdlog::info("Building embedded SearchEngine");

    // Validate required dependencies
    if (!vectorIndex_) {
        return Error{ErrorCode::InvalidArgument,
                     "SearchEngineBuilder: VectorIndexManager not provided"};
    }
    if (!vectorDatabase_) {
        return Error{ErrorCode::InvalidArgument,
                     "SearchEngineBuilder: VectorDatabase not provided"};
    }
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidArgument,
                     "SearchEngineBuilder: MetadataRepository not provided"};
    }

    // Use the provided config or defaults
    SearchEngineConfig cfg = options.config;

    // Create the SearchEngine using the factory function
    // Factory returns unique_ptr, convert to shared_ptr for builder interface
    auto engine = createSearchEngine(metadataRepo_, vectorDatabase_, vectorIndex_,
                                     embeddingGenerator_, kgStore_, cfg);
    if (!engine) {
        return Error{ErrorCode::InvalidState, "Failed to create SearchEngine"};
    }
    return std::shared_ptr<SearchEngine>(std::move(engine));
}

} // namespace yams::search
