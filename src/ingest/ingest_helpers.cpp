#include <spdlog/spdlog.h>
#include <yams/extraction/text_extractor.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

namespace yams::ingest {

Result<void> persist_content_and_index(metadata::IMetadataRepository& meta, int64_t docId,
                                       const std::string& title, const std::string& text,
                                       const std::string& mime, const std::string& method) {
    try {
        metadata::DocumentContent contentRow;
        contentRow.documentId = docId;
        contentRow.contentText = text;
        contentRow.contentLength = static_cast<int64_t>(text.size());
        contentRow.extractionMethod = method;
        double langConfidence = 0.0;
        contentRow.language =
            yams::extraction::LanguageDetector::detectLanguage(text, &langConfidence);
        if (const auto up = meta.insertContent(contentRow); !up) {
            return Result<void>(Error{up.error()});
        }
        if (auto r = meta.indexDocumentContent(docId, title, text, mime); !r) {
            return Result<void>(Error{r.error()});
        }
        (void)meta.updateFuzzyIndex(docId);
        if (auto d = meta.getDocument(docId); d && d.value().has_value()) {
            auto updated = d.value().value();
            updated.contentExtracted = true;
            updated.extractionStatus = metadata::ExtractionStatus::Success;
            (void)meta.updateDocument(updated);
        }
        return Result<void>();
    } catch (const std::exception& e) {
        return Result<void>(Error{ErrorCode::InternalError, e.what()});
    } catch (...) {
        return Result<void>(Error{ErrorCode::InternalError, "persist_content_and_index: unknown"});
    }
}

Result<size_t>
embed_and_insert_document(vector::EmbeddingGenerator& gen, vector::VectorDatabase& vdb,
                          metadata::IMetadataRepository& meta, const std::string& hash,
                          const std::string& text, const std::string& name, const std::string& path,
                          const std::string& mime, const yams::vector::ChunkingConfig& cfg) {
    try {
        // Chunk
        auto chunker = yams::vector::createChunker(yams::vector::ChunkingStrategy::SENTENCE_BASED,
                                                   cfg, nullptr);
        auto chunks = chunker->chunkDocument(text, hash);
        if (chunks.empty()) {
            yams::vector::DocumentChunk c;
            c.chunk_id = yams::vector::utils::generateChunkId(hash, 0);
            c.document_hash = hash;
            c.content = text;
            c.chunk_index = 0;
            chunks.push_back(std::move(c));
        }

        std::vector<std::string> texts;
        texts.reserve(chunks.size());
        for (auto& c : chunks)
            texts.push_back(c.content);
        auto embeds = gen.generateEmbeddings(texts);
        size_t n = std::min(embeds.size(), chunks.size());
        if (n == 0) {
            return Result<size_t>(Error{ErrorCode::InternalError, "no embeddings generated"});
        }
        std::vector<yams::vector::VectorRecord> recs;
        recs.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            yams::vector::VectorRecord r;
            r.document_hash = hash;
            r.chunk_id = chunks[i].chunk_id.empty() ? yams::vector::utils::generateChunkId(hash, i)
                                                    : chunks[i].chunk_id;
            r.embedding = std::move(embeds[i]);
            r.content = std::move(chunks[i].content);
            r.start_offset = chunks[i].start_offset;
            r.end_offset = chunks[i].end_offset;
            r.metadata["name"] = name;
            r.metadata["mime_type"] = mime;
            r.metadata["path"] = path;
            recs.push_back(std::move(r));
        }
        if (!vdb.insertVectorsBatch(recs)) {
            return Result<size_t>(Error{ErrorCode::DatabaseError, vdb.getLastError()});
        }
        (void)meta.updateDocumentEmbeddingStatusByHash(hash, true, gen.getConfig().model_name);
        return Result<size_t>(recs.size());
    } catch (const std::exception& e) {
        return Result<size_t>(Error{ErrorCode::InternalError, e.what()});
    } catch (...) {
        return Result<size_t>(
            Error{ErrorCode::InternalError, "embed_and_insert_document: unknown"});
    }
}

} // namespace yams::ingest
