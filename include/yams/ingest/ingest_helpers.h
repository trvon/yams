#pragma once

#include <string>
#include <yams/core/types.h>
#include <yams/vector/document_chunker.h>

namespace yams {
namespace daemon {
class IModelProvider;
}
namespace metadata {
class IMetadataRepository;
}
} // namespace yams
namespace yams {
namespace vector {
class EmbeddingGenerator;
class VectorDatabase;
} // namespace vector
} // namespace yams

namespace yams::ingest {

// Persist extracted content and index into FTS + fuzzy, updating doc flags.
// method: e.g., "post_ingest", "repair", etc.
Result<void> persist_content_and_index(metadata::IMetadataRepository& meta, int64_t docId,
                                       const std::string& title, const std::string& text,
                                       const std::string& mime, const std::string& method);

} // namespace yams::ingest
