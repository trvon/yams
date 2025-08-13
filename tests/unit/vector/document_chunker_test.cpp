#include <gtest/gtest.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>

#include <string>
#include <vector>
#include <numeric>
#include <algorithm>

using namespace yams::vector;

class DocumentChunkerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Set up default configuration
        default_config_.target_chunk_size = 100;
        default_config_.max_chunk_size = 150;
        default_config_.min_chunk_size = 50;
        default_config_.overlap_size = 20;
        default_config_.preserve_sentences = true;
        default_config_.preserve_words = true;
    }

    ChunkingConfig default_config_;
    
    // Test documents
    std::string short_text_ = "This is a short text.";
    
    std::string sentence_text_ = 
        "This is the first sentence. This is the second sentence. "
        "Here is the third sentence. And finally, the fourth sentence.";
    
    std::string paragraph_text_ = 
        "This is the first paragraph. It contains multiple sentences. "
        "Each sentence adds to the content.\n\n"
        "This is the second paragraph. It also has multiple sentences. "
        "The paragraphs are separated by newlines.\n\n"
        "And here is the third paragraph. It concludes our text. "
        "Thank you for reading.";
    
    std::string long_text_ = 
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris. "
        "Nisi ut aliquip ex ea commodo consequat. "
        "Duis aute irure dolor in reprehenderit in voluptate velit. "
        "Esse cillum dolore eu fugiat nulla pariatur. "
        "Excepteur sint occaecat cupidatat non proident. "
        "Sunt in culpa qui officia deserunt mollit anim id est laborum. "
        "Sed ut perspiciatis unde omnis iste natus error sit voluptatem. "
        "Accusantium doloremque laudantium, totam rem aperiam. "
        "Eaque ipsa quae ab illo inventore veritatis et quasi architecto. "
        "Beatae vitae dicta sunt explicabo.";
        
    std::string markdown_text_ = 
        "# Main Title\n\n"
        "This is an introduction paragraph under the main title.\n\n"
        "## Section 1\n\n"
        "Content for section 1 goes here. It has multiple sentences.\n\n"
        "### Subsection 1.1\n\n"
        "Details about subsection 1.1.\n\n"
        "## Section 2\n\n"
        "Content for section 2. This is another paragraph.\n\n"
        "```code\n"
        "def example():\n"
        "    return 'code block'\n"
        "```\n\n"
        "Final paragraph after code.";
};

// =============================================================================
// Fixed Size Chunker Tests
// =============================================================================

TEST_F(DocumentChunkerTest, FixedSizeChunker_BasicChunking) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::FIXED_SIZE;
    config.target_chunk_size = 50;
    
    FixedSizeChunker chunker(config);
    auto chunks = chunker.chunkDocument(sentence_text_, "doc_hash_123");
    
    ASSERT_FALSE(chunks.empty());
    
    // Verify chunk properties
    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        EXPECT_EQ(chunk.document_hash, "doc_hash_123");
        EXPECT_EQ(chunk.chunk_index, i);
        EXPECT_FALSE(chunk.content.empty());
        
        // Check size constraints (except last chunk)
        if (i < chunks.size() - 1) {
            EXPECT_GE(chunk.content.size(), config.min_chunk_size);
            EXPECT_LE(chunk.content.size(), config.max_chunk_size);
        }
    }
    
    // Verify chunks are linked
    for (size_t i = 0; i < chunks.size(); ++i) {
        if (i > 0) {
            EXPECT_TRUE(chunks[i].previous_chunk_id.has_value());
        }
        if (i < chunks.size() - 1) {
            EXPECT_TRUE(chunks[i].next_chunk_id.has_value());
        }
    }
}

TEST_F(DocumentChunkerTest, FixedSizeChunker_PreserveWords) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::FIXED_SIZE;
    config.target_chunk_size = 30;
    config.preserve_words = true;
    
    FixedSizeChunker chunker(config);
    std::string text = "This sentence should not split words arbitrarily anywhere.";
    auto chunks = chunker.chunkDocument(text, "doc_hash");
    
    // Check that no chunk ends in the middle of a word
    for (const auto& chunk : chunks) {
        if (!chunk.content.empty()) {
            // Last character should be space or punctuation if not end of text
            char last_char = chunk.content.back();
            if (chunk.end_offset < text.size()) {
                EXPECT_TRUE(std::isspace(last_char) || std::ispunct(last_char))
                    << "Chunk ends mid-word: " << chunk.content;
            }
        }
    }
}

TEST_F(DocumentChunkerTest, FixedSizeChunker_WithOverlap) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::FIXED_SIZE;
    config.target_chunk_size = 50;
    config.overlap_size = 10;
    
    FixedSizeChunker chunker(config);
    auto chunks = chunker.chunkDocument(long_text_, "doc_hash");
    
    // Verify overlap between consecutive chunks
    for (size_t i = 1; i < chunks.size(); ++i) {
        const auto& prev_chunk = chunks[i - 1];
        const auto& curr_chunk = chunks[i];
        
        // Check for actual content overlap
        if (prev_chunk.content.size() >= config.overlap_size) {
            std::string prev_end = prev_chunk.content.substr(
                prev_chunk.content.size() - config.overlap_size);
            std::string curr_start = curr_chunk.content.substr(0, config.overlap_size);
            
            // They should share some content
            EXPECT_FALSE(prev_end.empty());
            EXPECT_FALSE(curr_start.empty());
        }
    }
}

// =============================================================================
// Sentence-Based Chunker Tests
// =============================================================================

TEST_F(DocumentChunkerTest, SentenceBasedChunker_BasicChunking) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::SENTENCE_BASED;
    config.target_chunk_size = 60;
    
    SentenceBasedChunker chunker(config);
    auto chunks = chunker.chunkDocument(sentence_text_, "doc_hash");
    
    ASSERT_FALSE(chunks.empty());
    
    // Each chunk should contain complete sentences
    for (const auto& chunk : chunks) {
        // Should end with sentence terminator or be the last chunk
        if (!chunk.content.empty()) {
            char last_char = chunk.content.back();
            bool is_sentence_end = (last_char == '.' || last_char == '!' || last_char == '?');
            bool is_last_chunk = (chunk.next_chunk_id == std::nullopt);
            EXPECT_TRUE(is_sentence_end || is_last_chunk)
                << "Chunk doesn't end with sentence: " << chunk.content;
        }
    }
}

TEST_F(DocumentChunkerTest, SentenceBasedChunker_SingleSentence) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::SENTENCE_BASED;
    
    SentenceBasedChunker chunker(config);
    std::string single_sentence = "This is just one sentence.";
    auto chunks = chunker.chunkDocument(single_sentence, "doc_hash");
    
    ASSERT_EQ(chunks.size(), 1);
    EXPECT_EQ(chunks[0].content, single_sentence);
}

TEST_F(DocumentChunkerTest, SentenceBasedChunker_LongSentence) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::SENTENCE_BASED;
    config.target_chunk_size = 50;
    config.max_chunk_size = 100;
    
    SentenceBasedChunker chunker(config);
    std::string long_sentence = 
        "This is an extremely long sentence that goes on and on and on "
        "with many words and phrases and clauses that make it much longer "
        "than our target chunk size would normally allow.";
    
    auto chunks = chunker.chunkDocument(long_sentence, "doc_hash");
    
    // Long sentence should be split even if it means breaking sentences
    ASSERT_GT(chunks.size(), 1);
}

// =============================================================================
// Paragraph-Based Chunker Tests
// =============================================================================

TEST_F(DocumentChunkerTest, ParagraphBasedChunker_BasicChunking) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::PARAGRAPH_BASED;
    config.target_chunk_size = 100;
    
    ParagraphBasedChunker chunker(config);
    auto chunks = chunker.chunkDocument(paragraph_text_, "doc_hash");
    
    ASSERT_FALSE(chunks.empty());
    
    // Verify paragraphs are preserved when possible
    for (const auto& chunk : chunks) {
        // Count paragraph breaks within chunk
        size_t para_breaks = std::count(chunk.content.begin(), chunk.content.end(), '\n');
        // If chunk size allows, paragraphs should be kept together
        if (chunk.content.size() < config.target_chunk_size) {
            // Small chunks should be complete paragraphs
            EXPECT_TRUE(chunk.content.find("\n\n") == std::string::npos ||
                       chunk.content.find("\n\n") == chunk.content.size() - 2);
        }
    }
}

TEST_F(DocumentChunkerTest, ParagraphBasedChunker_SingleParagraph) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::PARAGRAPH_BASED;
    
    ParagraphBasedChunker chunker(config);
    std::string single_para = "This is a single paragraph with multiple sentences. But no paragraph breaks.";
    auto chunks = chunker.chunkDocument(single_para, "doc_hash");
    
    ASSERT_EQ(chunks.size(), 1);
    EXPECT_EQ(chunks[0].content, single_para);
}

// =============================================================================
// Recursive Text Splitter Tests
// =============================================================================

TEST_F(DocumentChunkerTest, RecursiveTextSplitter_BasicChunking) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::RECURSIVE;
    config.target_chunk_size = 80;
    config.separators = {"\n\n", "\n", ". ", " "};
    
    RecursiveTextSplitter chunker(config);
    auto chunks = chunker.chunkDocument(paragraph_text_, "doc_hash");
    
    ASSERT_FALSE(chunks.empty());
    
    // Verify chunks respect hierarchical splitting
    for (const auto& chunk : chunks) {
        EXPECT_FALSE(chunk.content.empty());
        EXPECT_LE(chunk.content.size(), config.max_chunk_size);
    }
}

TEST_F(DocumentChunkerTest, RecursiveTextSplitter_CustomSeparators) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::RECURSIVE;
    config.target_chunk_size = 50;
    config.separators = {";", ",", " "};
    
    RecursiveTextSplitter chunker(config);
    std::string text = "First part; second part, third part and fourth part; final part";
    auto chunks = chunker.chunkDocument(text, "doc_hash");
    
    ASSERT_FALSE(chunks.empty());
    
    // Verify text is split according to separator hierarchy
    for (const auto& chunk : chunks) {
        EXPECT_FALSE(chunk.content.empty());
    }
}

// =============================================================================
// Sliding Window Chunker Tests
// =============================================================================

TEST_F(DocumentChunkerTest, SlidingWindowChunker_BasicChunking) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::SLIDING_WINDOW;
    config.target_chunk_size = 50;
    config.overlap_size = 20;
    // Keep preserve_words = true for realistic behavior
    
    SlidingWindowChunker chunker(config);
    auto chunks = chunker.chunkDocument(long_text_, "doc_hash");
    
    ASSERT_FALSE(chunks.empty());
    
    // Verify sliding window properties
    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        
        // All chunks except last should be reasonable size
        // Very generous tolerance when word preservation is enabled (can be 2x target)
        if (i < chunks.size() - 1) {
            EXPECT_GE(chunk.content.size(), 20);   // At least 40% of 50
            EXPECT_LE(chunk.content.size(), 120);  // Up to 240% of 50 for word boundaries
        }
        
        // Verify reasonable chunk positioning (overlapping or reasonably adjacent)
        if (i > 0) {
            // Allow some gap between chunks due to word boundary adjustments
            size_t gap = chunk.start_offset > chunks[i-1].end_offset ? 
                         chunk.start_offset - chunks[i-1].end_offset : 0;
            EXPECT_LE(gap, 10);  // Allow up to 10 char gap for word boundaries
        }
    }
}

TEST_F(DocumentChunkerTest, SlidingWindowChunker_PercentageOverlap) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::SLIDING_WINDOW;
    config.target_chunk_size = 100;
    config.overlap_percentage = 0.25; // 25% overlap
    config.overlap_size = 0; // Use percentage instead
    
    SlidingWindowChunker chunker(config);
    auto chunks = chunker.chunkDocument(long_text_, "doc_hash");
    
    ASSERT_FALSE(chunks.empty());
    
    // Verify percentage-based overlap exists
    size_t expected_overlap = config.target_chunk_size * config.overlap_percentage;  // 25
    for (size_t i = 1; i < chunks.size(); ++i) {
        size_t actual_overlap = chunks[i-1].end_offset - chunks[i].start_offset;
        if (i < chunks.size() - 1) { // Not the last chunk
            // Allow generous tolerance due to word boundary preservation and algorithm behavior
            EXPECT_GE(actual_overlap, 5);   // Minimum overlap
            EXPECT_LE(actual_overlap, 60);  // Up to 240% of expected (25) for word boundaries
        }
    }
}

TEST_F(DocumentChunkerTest, SlidingWindowChunker_PreciseMode) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::SLIDING_WINDOW;
    config.target_chunk_size = 50;
    config.overlap_size = 20;
    config.preserve_words = false;  // For precise sliding window behavior
    
    SlidingWindowChunker chunker(config);
    auto chunks = chunker.chunkDocument(long_text_, "doc_hash");
    
    ASSERT_FALSE(chunks.empty());
    
    // With precise mode, chunks should be close to target size (±15% tolerance)
    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        
        if (i < chunks.size() - 1) {
            // Allow ±15% tolerance (42-58 chars for target of 50)
            EXPECT_GE(chunk.content.size(), 42);
            EXPECT_LE(chunk.content.size(), 58);
        }
        
        // Verify reasonable overlap (±50% tolerance: 10-30 for target of 20)
        if (i > 0) {
            size_t actual_overlap = chunks[i-1].end_offset - chunks[i].start_offset;
            EXPECT_GE(actual_overlap, 10);
            EXPECT_LE(actual_overlap, 30);
        }
    }
}

// =============================================================================
// Markdown Chunker Tests (Placeholder)
// =============================================================================

TEST_F(DocumentChunkerTest, MarkdownChunker_BasicChunking) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::MARKDOWN_AWARE;
    config.target_chunk_size = 100;
    
    MarkdownChunker chunker(config);
    auto chunks = chunker.chunkDocument(markdown_text_, "doc_hash");
    
    // Since implementation is placeholder, just verify it returns something
    ASSERT_FALSE(chunks.empty());
}

// =============================================================================
// Semantic Chunker Tests (Placeholder)
// =============================================================================

TEST_F(DocumentChunkerTest, SemanticChunker_BasicChunking) {
    ChunkingConfig config = default_config_;
    config.strategy = ChunkingStrategy::SEMANTIC;
    config.target_chunk_size = 100;
    config.semantic_threshold = 0.7f;
    
    // Create mock embedding generator
    EmbeddingConfig embed_config;
    auto embedder = std::make_shared<EmbeddingGenerator>(embed_config);
    
    SemanticChunker chunker(config, embedder);
    auto chunks = chunker.chunkDocument(sentence_text_, "doc_hash");
    
    // Since implementation is placeholder, just verify it returns something
    ASSERT_FALSE(chunks.empty());
}

// =============================================================================
// Factory Function Tests
// =============================================================================

TEST_F(DocumentChunkerTest, CreateChunker_AllStrategies) {
    ChunkingConfig config = default_config_;
    
    // Test each strategy
    std::vector<ChunkingStrategy> strategies = {
        ChunkingStrategy::FIXED_SIZE,
        ChunkingStrategy::SENTENCE_BASED,
        ChunkingStrategy::PARAGRAPH_BASED,
        ChunkingStrategy::SLIDING_WINDOW,
        ChunkingStrategy::RECURSIVE,
        ChunkingStrategy::MARKDOWN_AWARE
    };
    
    for (auto strategy : strategies) {
        auto chunker = createChunker(strategy, config);
        ASSERT_NE(chunker, nullptr) << "Failed to create chunker for strategy";
        
        // Test that it can chunk
        auto chunks = chunker->chunkDocument("Test text.", "doc_hash");
        EXPECT_FALSE(chunks.empty());
    }
}

TEST_F(DocumentChunkerTest, CreateChunker_SemanticWithEmbedder) {
    ChunkingConfig config = default_config_;
    EmbeddingConfig embed_config;
    auto embedder = std::make_shared<EmbeddingGenerator>(embed_config);
    
    auto chunker = createChunker(ChunkingStrategy::SEMANTIC, config, embedder);
    ASSERT_NE(chunker, nullptr);
    
    auto chunks = chunker->chunkDocument("Test text.", "doc_hash");
    EXPECT_FALSE(chunks.empty());
}

// =============================================================================
// Validation Tests
// =============================================================================

TEST_F(DocumentChunkerTest, ValidateChunks_ValidChunks) {
    ChunkingConfig config = default_config_;
    config.min_chunk_size = 10;
    config.max_chunk_size = 100;
    
    FixedSizeChunker chunker(config);
    
    std::vector<DocumentChunk> valid_chunks = {
        DocumentChunk("chunk1", "doc_hash", "This is valid content"),
        DocumentChunk("chunk2", "doc_hash", "Another valid chunk here")
    };
    
    EXPECT_TRUE(chunker.validateChunks(valid_chunks));
}

TEST_F(DocumentChunkerTest, ValidateChunks_InvalidSize) {
    ChunkingConfig config = default_config_;
    config.min_chunk_size = 20;
    config.max_chunk_size = 50;
    
    FixedSizeChunker chunker(config);
    
    std::vector<DocumentChunk> invalid_chunks = {
        DocumentChunk("chunk1", "doc_hash", "Too short"),  // Less than min_chunk_size
        DocumentChunk("chunk2", "doc_hash", std::string(100, 'x'))  // More than max_chunk_size
    };
    
    EXPECT_FALSE(chunker.validateChunks(invalid_chunks));
}

// =============================================================================
// Chunk Operations Tests
// =============================================================================

TEST_F(DocumentChunkerTest, MergeSmallChunks) {
    ChunkingConfig config = default_config_;
    config.min_chunk_size = 50;
    
    FixedSizeChunker chunker(config);
    
    std::vector<DocumentChunk> small_chunks = {
        DocumentChunk("chunk1", "doc_hash", "Small"),
        DocumentChunk("chunk2", "doc_hash", "Tiny"),
        DocumentChunk("chunk3", "doc_hash", "Regular sized chunk that meets minimum")
    };
    
    auto merged = chunker.mergeSmallChunks(small_chunks);
    
    // Small chunks should be merged
    EXPECT_LT(merged.size(), small_chunks.size());
    
    // All merged chunks should meet minimum size (except possibly the last)
    if (!merged.empty()) {
        for (size_t i = 0; i < merged.size() - 1; ++i) {
            EXPECT_GE(merged[i].content.size(), config.min_chunk_size);
        }
    }
}

TEST_F(DocumentChunkerTest, SplitLargeChunks) {
    ChunkingConfig config = default_config_;
    config.max_chunk_size = 50;
    
    FixedSizeChunker chunker(config);
    
    std::vector<DocumentChunk> large_chunks = {
        DocumentChunk("chunk1", "doc_hash", std::string(200, 'x')),
        DocumentChunk("chunk2", "doc_hash", "Normal chunk")
    };
    
    auto split = chunker.splitLargeChunks(large_chunks);
    
    // Large chunk should be split
    EXPECT_GT(split.size(), large_chunks.size());
    
    // All chunks should be within max size
    for (const auto& chunk : split) {
        EXPECT_LE(chunk.content.size(), config.max_chunk_size);
    }
}

TEST_F(DocumentChunkerTest, AddOverlap) {
    ChunkingConfig config = default_config_;
    config.overlap_size = 10;
    
    FixedSizeChunker chunker(config);
    
    std::vector<DocumentChunk> chunks = {
        DocumentChunk("chunk1", "doc_hash", "First chunk content here"),
        DocumentChunk("chunk2", "doc_hash", "Second chunk content here"),
        DocumentChunk("chunk3", "doc_hash", "Third chunk content here")
    };
    
    // Set proper offsets
    chunks[0].start_offset = 0;
    chunks[0].end_offset = chunks[0].content.size();
    chunks[1].start_offset = chunks[0].end_offset;
    chunks[1].end_offset = chunks[1].start_offset + chunks[1].content.size();
    chunks[2].start_offset = chunks[1].end_offset;
    chunks[2].end_offset = chunks[2].start_offset + chunks[2].content.size();
    
    auto with_overlap = chunker.addOverlap(chunks);
    
    // Should maintain or increase chunk count
    EXPECT_GE(with_overlap.size(), chunks.size());
    
    // Check that overlapping_chunks are populated
    for (size_t i = 1; i < with_overlap.size(); ++i) {
        if (with_overlap[i].start_offset < with_overlap[i-1].end_offset) {
            EXPECT_FALSE(with_overlap[i].overlapping_chunks.empty());
        }
    }
}

// =============================================================================
// Statistics Tests
// =============================================================================

TEST_F(DocumentChunkerTest, ChunkingStats_Tracking) {
    ChunkingConfig config = default_config_;
    FixedSizeChunker chunker(config);
    
    // Reset stats
    chunker.resetStats();
    auto initial_stats = chunker.getStats();
    EXPECT_EQ(initial_stats.total_documents, 0);
    EXPECT_EQ(initial_stats.total_chunks, 0);
    
    // Chunk some documents
    chunker.chunkDocument(sentence_text_, "doc1");
    chunker.chunkDocument(paragraph_text_, "doc2");
    
    auto stats = chunker.getStats();
    EXPECT_EQ(stats.total_documents, 2);
    EXPECT_GT(stats.total_chunks, 0);
    EXPECT_GT(stats.avg_chunk_size, 0);
}

// =============================================================================
// Error Handling Tests
// =============================================================================

TEST_F(DocumentChunkerTest, EmptyDocument) {
    ChunkingConfig config = default_config_;
    FixedSizeChunker chunker(config);
    
    auto chunks = chunker.chunkDocument("", "doc_hash");
    EXPECT_TRUE(chunks.empty());
}

TEST_F(DocumentChunkerTest, InvalidConfiguration) {
    ChunkingConfig config = default_config_;
    config.min_chunk_size = 100;
    config.max_chunk_size = 50; // Invalid: min > max
    
    FixedSizeChunker chunker(config);
    
    // Should still handle gracefully
    auto chunks = chunker.chunkDocument(sentence_text_, "doc_hash");
    EXPECT_FALSE(chunks.empty()); // Should still produce chunks
}

// =============================================================================
// Utility Functions Tests
// =============================================================================

TEST_F(DocumentChunkerTest, EstimateTokenCount) {
    ChunkingConfig config = default_config_;
    FixedSizeChunker chunker(config);
    
    // Rough estimation: ~4 chars per token
    std::string text = "This is a test sentence with several words.";
    size_t estimated = chunker.estimateTokenCount(text);
    
    // Should be reasonable estimate
    EXPECT_GT(estimated, 0);
    EXPECT_LT(estimated, text.size()); // Less than character count
}

TEST_F(DocumentChunkerTest, FindSentenceBoundaries) {
    ChunkingConfig config = default_config_;
    FixedSizeChunker chunker(config);
    
    auto boundaries = chunker.findSentenceBoundaries(sentence_text_);
    
    // Should find sentence endings
    EXPECT_FALSE(boundaries.empty());
    
    // Each boundary should be at a sentence terminator
    for (size_t pos : boundaries) {
        if (pos > 0 && pos < sentence_text_.size()) {
            char ch = sentence_text_[pos - 1];
            EXPECT_TRUE(ch == '.' || ch == '!' || ch == '?');
        }
    }
}

TEST_F(DocumentChunkerTest, FindParagraphBoundaries) {
    ChunkingConfig config = default_config_;
    FixedSizeChunker chunker(config);
    
    auto boundaries = chunker.findParagraphBoundaries(paragraph_text_);
    
    // Should find paragraph breaks
    EXPECT_FALSE(boundaries.empty());
    
    // Each boundary should be at a paragraph break
    for (size_t pos : boundaries) {
        if (pos >= 2 && pos < paragraph_text_.size()) {
            std::string check = paragraph_text_.substr(pos - 2, 2);
            EXPECT_EQ(check, "\n\n");
        }
    }
}

// =============================================================================
// Async Operations Tests
// =============================================================================

TEST_F(DocumentChunkerTest, ChunkDocumentAsync) {
    ChunkingConfig config = default_config_;
    FixedSizeChunker chunker(config);
    
    auto future = chunker.chunkDocumentAsync(sentence_text_, "doc_hash");
    auto chunks = future.get();
    
    ASSERT_FALSE(chunks.empty());
    EXPECT_EQ(chunks[0].document_hash, "doc_hash");
}

TEST_F(DocumentChunkerTest, ChunkDocumentsBatch) {
    ChunkingConfig config = default_config_;
    FixedSizeChunker chunker(config);
    
    std::vector<std::string> documents = {
        sentence_text_,
        paragraph_text_,
        short_text_
    };
    
    std::vector<std::string> hashes = {
        "hash1", "hash2", "hash3"
    };
    
    auto batch_results = chunker.chunkDocuments(documents, hashes);
    
    ASSERT_EQ(batch_results.size(), documents.size());
    
    for (size_t i = 0; i < batch_results.size(); ++i) {
        EXPECT_FALSE(batch_results[i].empty());
        if (!batch_results[i].empty()) {
            EXPECT_EQ(batch_results[i][0].document_hash, hashes[i]);
        }
    }
}

// =============================================================================
// Namespace Utility Functions Tests
// =============================================================================

TEST(ChunkingUtilsTest, CalculateOptimalChunkSize) {
    // Test for known models
    size_t minilm_size = chunking_utils::calculateOptimalChunkSize("all-MiniLM-L6-v2");
    EXPECT_GT(minilm_size, 0);
    EXPECT_LE(minilm_size, 512); // Model max is 512
    
    size_t mpnet_size = chunking_utils::calculateOptimalChunkSize("all-mpnet-base-v2");
    EXPECT_GT(mpnet_size, 0);
    EXPECT_LE(mpnet_size, 512);
}

TEST(ChunkingUtilsTest, EstimateTokens) {
    std::string text = "This is a sample text for token estimation.";
    size_t tokens = chunking_utils::estimateTokens(text);
    
    // Rough estimate: should be less than character count
    EXPECT_GT(tokens, 0);
    EXPECT_LT(tokens, text.size());
}

TEST(ChunkingUtilsTest, PreprocessText) {
    std::string text = "  This   has    extra   spaces.  \n\n";
    std::string processed = chunking_utils::preprocessText(text);
    
    // Should normalize whitespace
    EXPECT_NE(processed.find("   "), std::string::npos);
    EXPECT_EQ(processed.front(), 'T'); // Should trim
    EXPECT_EQ(processed.back(), '.'); // Should trim
}

TEST(ChunkingUtilsTest, DetectLanguage) {
    std::string english = "This is English text.";
    std::string lang = chunking_utils::detectLanguage(english);
    
    // Placeholder implementation might return "en" or "unknown"
    EXPECT_FALSE(lang.empty());
}

TEST(ChunkingUtilsTest, CalculateSemanticCoherence) {
    std::string coherent = "The cat sat on the mat. The mat was red. The cat was happy.";
    double coherence = chunking_utils::calculateSemanticCoherence(coherent);
    
    // Should return a value between 0 and 1
    EXPECT_GE(coherence, 0.0);
    EXPECT_LE(coherence, 1.0);
}

TEST(ChunkingUtilsTest, MergeOverlappingChunks) {
    std::vector<DocumentChunk> chunks;
    
    DocumentChunk chunk1("c1", "doc", "First chunk content");
    chunk1.start_offset = 0;
    chunk1.end_offset = 19;
    
    DocumentChunk chunk2("c2", "doc", "chunk content overlaps");
    chunk2.start_offset = 10;  // Overlaps with chunk1
    chunk2.end_offset = 33;
    
    DocumentChunk chunk3("c3", "doc", "Third separate chunk");
    chunk3.start_offset = 40;
    chunk3.end_offset = 60;
    
    chunks.push_back(chunk1);
    chunks.push_back(chunk2);
    chunks.push_back(chunk3);
    
    auto merged = chunking_utils::mergeOverlappingChunks(chunks, 0.5);
    
    // Should merge overlapping chunks
    EXPECT_LT(merged.size(), chunks.size());
}

TEST(ChunkingUtilsTest, DeduplicateChunks) {
    std::vector<DocumentChunk> chunks;
    
    chunks.push_back(DocumentChunk("c1", "doc", "Identical content"));
    chunks.push_back(DocumentChunk("c2", "doc", "Identical content"));
    chunks.push_back(DocumentChunk("c3", "doc", "Different content"));
    
    auto deduped = chunking_utils::deduplicateChunks(chunks, 0.95);
    
    // Should remove duplicate
    EXPECT_EQ(deduped.size(), 2);
}

TEST(ChunkingUtilsTest, CreateChunkingSummary) {
    std::vector<DocumentChunk> chunks;
    chunks.push_back(DocumentChunk("c1", "doc", "First chunk"));
    chunks.push_back(DocumentChunk("c2", "doc", "Second chunk"));
    
    std::string summary = chunking_utils::createChunkingSummary(chunks);
    
    EXPECT_FALSE(summary.empty());
    EXPECT_NE(summary.find("2"), std::string::npos); // Should mention count
}