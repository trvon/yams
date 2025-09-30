#pragma once

#include <string>
#include <yams/core/types.h>
#include <yams/vector/document_chunker.h>

namespace yams {
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

// Chunk, embed, insert vectors, and mark embedding status for a single document.
// Returns number of inserted records.
Result<size_t>
embed_and_insert_document(vector::EmbeddingGenerator& gen, vector::VectorDatabase& vdb,
                          metadata::IMetadataRepository& meta, const std::string& hash,
                          const std::string& text, const std::string& name, const std::string& path,
                          const std::string& mime, const yams::vector::ChunkingConfig& cfg = {});

} // namespace yams::ingest
