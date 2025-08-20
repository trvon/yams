#pragma once

#include <yams/core/types.h>
#include <yams/vector/embedding_generator.h>

#include <chrono>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yams::vector {

/**
 * Chunking strategies for document segmentation
 */
enum class ChunkingStrategy {
    FIXED_SIZE,      // Fixed character/token count
    SENTENCE_BASED,  // Sentence boundary detection
    PARAGRAPH_BASED, // Paragraph boundaries
    SEMANTIC,        // Semantic similarity grouping
    SLIDING_WINDOW,  // Overlapping fixed windows
    RECURSIVE,       // Recursive text splitting
    MARKDOWN_AWARE   // Respects markdown structure
};

/**
 * Configuration for document chunking
 */
struct ChunkingConfig {
    ChunkingStrategy strategy = ChunkingStrategy::SENTENCE_BASED;
    size_t target_chunk_size = 512;     // Target size in tokens/characters
    size_t max_chunk_size = 1024;       // Maximum chunk size
    size_t min_chunk_size = 100;        // Minimum chunk size
    size_t overlap_size = 50;           // Overlap between chunks (in tokens/chars)
    bool preserve_sentences = true;     // Don't split sentences
    bool preserve_paragraphs = false;   // Don't split paragraphs
    bool preserve_words = true;         // Don't split words
    double overlap_percentage = 0.1;    // 10% overlap as alternative to fixed size
    std::string chunk_separator = "\n"; // Separator to use when joining
    bool use_token_count = false;       // Use token count instead of character count

    // Semantic chunking specific
    float semantic_threshold = 0.5f; // Similarity threshold for semantic chunking

    // Recursive splitting specific
    std::vector<std::string> separators = {"\n\n", "\n", ". ", " ", ""};
};

/**
 * Represents a single document chunk
 */
struct DocumentChunk {
    std::string chunk_id;      // Unique chunk identifier
    std::string document_hash; // Parent document hash
    std::string content;       // Chunk text content
    size_t chunk_index = 0;    // Position in document (0-based)
    size_t start_offset = 0;   // Character offset in document
    size_t end_offset = 0;     // End character offset
    size_t token_count = 0;    // Estimated token count

    // Metadata
    std::map<std::string, std::string> metadata;
    std::string language;           // Detected language
    ChunkingStrategy strategy_used; // How it was chunked

    // Relationships
    std::optional<std::string> previous_chunk_id;
    std::optional<std::string> next_chunk_id;
    std::vector<std::string> overlapping_chunks; // IDs of overlapping chunks

    // Statistics
    double semantic_coherence = 0.0; // Measure of semantic unity
    size_t sentence_count = 0;
    size_t word_count = 0;
    size_t line_count = 0;

    // Constructors
    DocumentChunk() : strategy_used(ChunkingStrategy::FIXED_SIZE) {}
    DocumentChunk(std::string id, std::string doc_hash, std::string text)
        : chunk_id(std::move(id)), document_hash(std::move(doc_hash)), content(std::move(text)),
          strategy_used(ChunkingStrategy::FIXED_SIZE) {}

    // Utility methods
    bool hasOverlapWith(const DocumentChunk& other) const {
        return (start_offset < other.end_offset) && (end_offset > other.start_offset);
    }

    size_t getSize() const { return content.size(); }
};

/**
 * Statistics for chunking operations
 */
struct ChunkingStats {
    size_t total_documents = 0;
    size_t total_chunks = 0;
    double avg_chunk_size = 0.0;
    double avg_overlap = 0.0;
    size_t min_chunk_size = 0;
    size_t max_chunk_size = 0;
    std::chrono::milliseconds total_time{0};
    std::chrono::milliseconds avg_time_per_doc{0};

    void update(size_t chunk_count, size_t total_size, std::chrono::milliseconds time) {
        total_documents++;
        total_chunks += chunk_count;
        total_time += time;

        if (total_documents > 0) {
            avg_time_per_doc = total_time / total_documents;
        }

        if (chunk_count > 0) {
            avg_chunk_size =
                (avg_chunk_size * (total_chunks - chunk_count) + total_size) / total_chunks;
        }
    }
};

/**
 * Base class for document chunking strategies
 */
class DocumentChunker {
public:
    explicit DocumentChunker(const ChunkingConfig& config = {});
    virtual ~DocumentChunker() = default;

    // Non-copyable but movable
    DocumentChunker(const DocumentChunker&) = delete;
    DocumentChunker& operator=(const DocumentChunker&) = delete;
    DocumentChunker(DocumentChunker&&) = default;
    DocumentChunker& operator=(DocumentChunker&&) = default;

    // Main chunking interface
    virtual std::vector<DocumentChunk> chunkDocument(const std::string& content,
                                                     const std::string& document_hash);

    // Async chunking
    std::future<std::vector<DocumentChunk>> chunkDocumentAsync(const std::string& content,
                                                               const std::string& document_hash);

    // Batch chunking
    std::vector<std::vector<DocumentChunk>>
    chunkDocuments(const std::vector<std::string>& contents,
                   const std::vector<std::string>& document_hashes);

    // Configuration
    void setConfig(const ChunkingConfig& config);
    const ChunkingConfig& getConfig() const;

    // Chunk operations
    virtual std::vector<DocumentChunk> mergeSmallChunks(const std::vector<DocumentChunk>& chunks);
    virtual std::vector<DocumentChunk> splitLargeChunks(const std::vector<DocumentChunk>& chunks);
    std::vector<DocumentChunk> addOverlap(const std::vector<DocumentChunk>& chunks);

    // Validation
    bool validateChunks(const std::vector<DocumentChunk>& chunks) const;
    bool validateChunkSize(const DocumentChunk& chunk) const;

    // Statistics
    ChunkingStats getStats() const;
    void resetStats();

    // Utility functions
    size_t estimateTokenCount(const std::string& text) const;
    std::vector<size_t> findSentenceBoundaries(const std::string& text) const;
    std::vector<size_t> findParagraphBoundaries(const std::string& text) const;
    std::vector<size_t> findWordBoundaries(const std::string& text) const;

    // Error handling
    std::string getLastError() const;
    bool hasError() const;

protected:
    // Override this for custom chunking strategies
    virtual std::vector<DocumentChunk> doChunking(const std::string& content,
                                                  const std::string& document_hash) = 0;

    // Helper methods for derived classes
    std::string generateChunkId(const std::string& document_hash, size_t index) const;
    void updateChunkMetadata(DocumentChunk& chunk, const std::string& content) const;
    void linkChunks(std::vector<DocumentChunk>& chunks) const;

    ChunkingConfig config_;
    mutable ChunkingStats stats_;
    mutable std::string last_error_;
    mutable bool has_error_ = false;
};

/**
 * Fixed-size chunking strategy
 */
class FixedSizeChunker : public DocumentChunker {
public:
    explicit FixedSizeChunker(const ChunkingConfig& config = {});

protected:
    std::vector<DocumentChunk> doChunking(const std::string& content,
                                          const std::string& document_hash) override;
};

/**
 * Sentence-based chunking strategy
 */
class SentenceBasedChunker : public DocumentChunker {
public:
    explicit SentenceBasedChunker(const ChunkingConfig& config = {});

protected:
    std::vector<DocumentChunk> doChunking(const std::string& content,
                                          const std::string& document_hash) override;

private:
    std::vector<std::string> splitIntoSentences(const std::string& text) const;
    bool isSentenceEnd(const std::string& text, size_t pos) const;
};

/**
 * Paragraph-based chunking strategy
 */
class ParagraphBasedChunker : public DocumentChunker {
public:
    explicit ParagraphBasedChunker(const ChunkingConfig& config = {});

protected:
    std::vector<DocumentChunk> doChunking(const std::string& content,
                                          const std::string& document_hash) override;

private:
    std::vector<std::string> splitIntoParagraphs(const std::string& text) const;
};

/**
 * Semantic chunking using embeddings
 */
class SemanticChunker : public DocumentChunker {
public:
    SemanticChunker(const ChunkingConfig& config, std::shared_ptr<EmbeddingGenerator> embedder);

protected:
    std::vector<DocumentChunk> doChunking(const std::string& content,
                                          const std::string& document_hash) override;

private:
    double computeSimilarity(const std::string& text1, const std::string& text2);
    std::vector<size_t> findSemanticBoundaries(const std::string& text);

    std::shared_ptr<EmbeddingGenerator> embedder_;
};

/**
 * Recursive text splitting strategy
 */
class RecursiveTextSplitter : public DocumentChunker {
public:
    explicit RecursiveTextSplitter(const ChunkingConfig& config = {});

protected:
    std::vector<DocumentChunk> doChunking(const std::string& content,
                                          const std::string& document_hash) override;

private:
    std::vector<std::string> recursiveSplit(const std::string& text,
                                            const std::vector<std::string>& separators,
                                            size_t target_size);

    void splitOnSeparator(const std::string& text, const std::string& separator,
                          std::vector<std::string>& results, size_t target_size);
};

/**
 * Markdown-aware chunking strategy
 */
class MarkdownChunker : public DocumentChunker {
public:
    explicit MarkdownChunker(const ChunkingConfig& config = {});

protected:
    std::vector<DocumentChunk> doChunking(const std::string& content,
                                          const std::string& document_hash) override;

private:
    struct MarkdownSection {
        std::string content;
        int header_level = 0; // 0 for non-header content
        size_t start_pos = 0;
        size_t end_pos = 0;
    };

    std::vector<MarkdownSection> parseMarkdownStructure(const std::string& text) const;
    std::vector<DocumentChunk> chunksFromSections(const std::vector<MarkdownSection>& sections,
                                                  const std::string& document_hash) const;
};

/**
 * Sliding window chunking strategy
 */
class SlidingWindowChunker : public DocumentChunker {
public:
    explicit SlidingWindowChunker(const ChunkingConfig& config = {});

protected:
    std::vector<DocumentChunk> doChunking(const std::string& content,
                                          const std::string& document_hash) override;

private:
    size_t calculateStride() const;
};

/**
 * Factory function for creating chunkers based on strategy
 */
std::unique_ptr<DocumentChunker>
createChunker(ChunkingStrategy strategy, const ChunkingConfig& config = {},
              std::shared_ptr<EmbeddingGenerator> embedder = nullptr);

/**
 * Utility functions for chunking operations
 */
namespace chunking_utils {
/**
 * Calculate optimal chunk size for a given model
 */
size_t calculateOptimalChunkSize(const std::string& model_name);

/**
 * Estimate token count for text
 */
size_t estimateTokens(const std::string& text);

/**
 * Clean and normalize text before chunking
 */
std::string preprocessText(const std::string& text);

/**
 * Detect language of text
 */
std::string detectLanguage(const std::string& text);

/**
 * Calculate semantic coherence of a chunk
 */
double calculateSemanticCoherence(const std::string& text);

/**
 * Find natural breaking points in text
 */
std::vector<size_t> findNaturalBreaks(const std::string& text);

/**
 * Merge overlapping chunks
 */
std::vector<DocumentChunk> mergeOverlappingChunks(const std::vector<DocumentChunk>& chunks,
                                                  double overlap_threshold = 0.5);

/**
 * Deduplicate chunks based on content similarity
 */
std::vector<DocumentChunk> deduplicateChunks(const std::vector<DocumentChunk>& chunks,
                                             double similarity_threshold = 0.95);

/**
 * Create a summary of chunking results
 */
std::string createChunkingSummary(const std::vector<DocumentChunk>& chunks);
} // namespace chunking_utils

} // namespace yams::vector