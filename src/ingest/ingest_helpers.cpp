#include <spdlog/spdlog.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/extraction/text_extractor.h>
#include <yams/ingest/ingest_helpers.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

namespace yams::ingest {

Result<void> persist_content_and_index(metadata::IMetadataRepository& meta, int64_t docId,
                                       const std::string& title, const std::string& text,
                                       const std::string& mime, const std::string& method) {
    YAMS_ZONE_SCOPED_N("ingest::persist_content_and_index");
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
        if (auto r = meta.indexDocumentContentTrusted(docId, title, text, mime); !r) {
            return Result<void>(Error{r.error()});
        }

        (void)meta.updateFuzzyIndex(docId);
        (void)meta.updateDocumentExtractionStatus(docId, true, metadata::ExtractionStatus::Success);
        return Result<void>();
    } catch (const std::exception& e) {
        return Result<void>(Error{ErrorCode::InternalError, e.what()});
    } catch (...) {
        return Result<void>(Error{ErrorCode::InternalError, "persist_content_and_index: unknown"});
    }
}

Result<size_t> embed_and_insert_document(yams::daemon::IModelProvider& provider,
                                         const std::string& modelName, vector::VectorDatabase& vdb,
                                         metadata::IMetadataRepository& meta,
                                         const std::string& hash, const std::string& text,
                                         const std::string& name, const std::string& path,
                                         const std::string& mime,
                                         const yams::vector::ChunkingConfig& cfg) {
    YAMS_ZONE_SCOPED_N("ingest::embed_and_insert_document");
    try {
        std::vector<yams::vector::DocumentChunk> chunks;
        {
            // Chunk
            YAMS_ZONE_SCOPED_N("ingest::chunking");
            auto chunker = yams::vector::createChunker(
                yams::vector::ChunkingStrategy::SENTENCE_BASED, cfg, nullptr);
            chunks = chunker->chunkDocument(text, hash);
            if (chunks.empty()) {
                yams::vector::DocumentChunk c;
                c.chunk_id = yams::vector::utils::generateChunkId(hash, 0);
                c.document_hash = hash;
                c.content = text;
                c.chunk_index = 0;
                chunks.push_back(std::move(c));
            }
        }

        std::vector<std::string> texts;
        texts.reserve(chunks.size());
        for (auto& c : chunks)
            texts.push_back(c.content);

        std::vector<std::vector<float>> embeds;
        {
            // Generate embeddings using IModelProvider directly
            YAMS_ZONE_SCOPED_N("ingest::embedding_generation");
            auto embedResult = provider.generateBatchEmbeddingsFor(modelName, texts);
            if (!embedResult) {
                return Result<size_t>(
                    Error{ErrorCode::InternalError, std::string("Failed to generate embeddings: ") +
                                                        embedResult.error().message});
            }
            embeds = std::move(embedResult.value());
        }

        size_t n = std::min(embeds.size(), chunks.size());
        if (n == 0) {
            return Result<size_t>(Error{ErrorCode::InternalError, "no embeddings generated"});
        }

        // Compute document-level embedding BEFORE moving chunk embeddings
        std::vector<float> doc_embedding;
        if (n > 0 && !embeds[0].empty()) {
            doc_embedding.resize(embeds[0].size(), 0.0f);
            for (size_t i = 0; i < n; ++i) {
                for (size_t j = 0; j < embeds[i].size(); ++j) {
                    doc_embedding[j] += embeds[i][j];
                }
            }
            float norm = 0.0f;
            for (float v : doc_embedding) {
                norm += v * v;
            }
            if (norm > 0.0f) {
                norm = std::sqrt(norm);
                for (float& v : doc_embedding) {
                    v /= norm;
                }
            }
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
            r.level = yams::vector::EmbeddingLevel::CHUNK;
            r.metadata["name"] = name;
            r.metadata["mime_type"] = mime;
            r.metadata["path"] = path;
            recs.push_back(std::move(r));
        }
        {
            YAMS_ZONE_SCOPED_N("ingest::vector_insertion");
            if (!vdb.insertVectorsBatch(recs)) {
                return Result<size_t>(Error{ErrorCode::DatabaseError, vdb.getLastError()});
            }

            if (!doc_embedding.empty()) {
                yams::vector::VectorRecord doc_rec;
                doc_rec.document_hash = hash;
                doc_rec.chunk_id = yams::vector::utils::generateChunkId(hash, 999999);
                doc_rec.embedding = std::move(doc_embedding);
                doc_rec.content = text.substr(0, 1000);
                doc_rec.level = yams::vector::EmbeddingLevel::DOCUMENT;
                doc_rec.source_chunk_ids.reserve(n);
                for (size_t i = 0; i < n; ++i) {
                    doc_rec.source_chunk_ids.push_back(recs[i].chunk_id);
                }
                doc_rec.metadata["name"] = name;
                doc_rec.metadata["mime_type"] = mime;
                doc_rec.metadata["path"] = path;

                if (!vdb.insertVector(doc_rec)) {
                    spdlog::warn("Failed to insert document-level embedding for hash={}: {}", hash,
                                 vdb.getLastError());
                }
            }
        }

        (void)meta.updateDocumentEmbeddingStatusByHash(hash, true, modelName);
        return Result<size_t>(recs.size());
    } catch (const std::exception& e) {
        return Result<size_t>(Error{ErrorCode::InternalError, e.what()});
    } catch (...) {
        return Result<size_t>(
            Error{ErrorCode::InternalError, "embed_and_insert_document: unknown"});
    }
}

} // namespace yams::ingest
