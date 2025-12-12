// Catch2 tests for document chunker
// Migrated from GTest: document_chunker_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <numeric>
#include <string>
#include <vector>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>

using namespace yams::vector;

namespace {

struct DocumentChunkerFixture {
    DocumentChunkerFixture() {
        // Set up default configuration
        defaultConfig.target_chunk_size = 100;
        defaultConfig.max_chunk_size = 150;
        defaultConfig.min_chunk_size = 50;
        defaultConfig.overlap_size = 20;
        defaultConfig.preserve_sentences = true;
        defaultConfig.preserve_words = true;
    }

    ChunkingConfig defaultConfig;

    // Test documents
    std::string shortText = "This is a short text.";

    std::string sentenceText = "This is the first sentence. This is the second sentence. "
                               "Here is the third sentence. And finally, the fourth sentence.";

    std::string paragraphText = "This is the first paragraph. It contains multiple sentences. "
                                "Each sentence adds to the content.\n\n"
                                "This is the second paragraph. It also has multiple sentences. "
                                "The paragraphs are separated by newlines.\n\n"
                                "And here is the third paragraph. It concludes our text. "
                                "Thank you for reading.";

    std::string longText = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
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

    std::string markdownText = "# Main Title\n\n"
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

} // namespace

// =============================================================================
// Fixed Size Chunker Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "FixedSizeChunker basic chunking",
                 "[vector][chunker][fixed-size][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::FIXED_SIZE;
    config.target_chunk_size = 50;

    FixedSizeChunker chunker(config);
    auto chunks = chunker.chunkDocument(sentenceText, "doc_hash_123");

    REQUIRE_FALSE(chunks.empty());

    // Verify chunk properties
    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];
        CHECK(chunk.document_hash == "doc_hash_123");
        CHECK(chunk.chunk_index == i);
        CHECK_FALSE(chunk.content.empty());

        // Check size constraints (except last chunk)
        if (i < chunks.size() - 1) {
            CHECK(chunk.content.size() >= config.min_chunk_size);
            CHECK(chunk.content.size() <= config.max_chunk_size);
        }
    }

    // Verify chunks are linked
    for (size_t i = 0; i < chunks.size(); ++i) {
        if (i > 0) {
            CHECK(chunks[i].previous_chunk_id.has_value());
        }
        if (i < chunks.size() - 1) {
            CHECK(chunks[i].next_chunk_id.has_value());
        }
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "FixedSizeChunker preserve words",
                 "[vector][chunker][fixed-size][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::FIXED_SIZE;
    config.target_chunk_size = 30;
    config.preserve_words = true;

    FixedSizeChunker chunker(config);
    std::string text = "This sentence should not split words arbitrarily anywhere.";
    auto chunks = chunker.chunkDocument(text, "doc_hash");

    // Check that no chunk ends in the middle of a word
    for (const auto& chunk : chunks) {
        if (!chunk.content.empty()) {
            char lastChar = chunk.content.back();
            if (chunk.end_offset < text.size()) {
                CHECK((std::isspace(lastChar) || std::ispunct(lastChar)));
            }
        }
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "FixedSizeChunker with overlap",
                 "[vector][chunker][fixed-size][overlap][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::FIXED_SIZE;
    config.target_chunk_size = 50;
    config.overlap_size = 10;

    FixedSizeChunker chunker(config);
    auto chunks = chunker.chunkDocument(longText, "doc_hash");

    // Verify overlap between consecutive chunks
    for (size_t i = 1; i < chunks.size(); ++i) {
        const auto& prevChunk = chunks[i - 1];
        const auto& currChunk = chunks[i];

        // Check for actual content overlap
        if (prevChunk.content.size() >= config.overlap_size) {
            std::string prevEnd =
                prevChunk.content.substr(prevChunk.content.size() - config.overlap_size);
            std::string currStart = currChunk.content.substr(0, config.overlap_size);

            CHECK_FALSE(prevEnd.empty());
            CHECK_FALSE(currStart.empty());
        }
    }
}

// =============================================================================
// Sentence-Based Chunker Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "SentenceBasedChunker basic chunking",
                 "[vector][chunker][sentence][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::SENTENCE_BASED;
    config.target_chunk_size = 60;

    SentenceBasedChunker chunker(config);
    auto chunks = chunker.chunkDocument(sentenceText, "doc_hash");

    REQUIRE_FALSE(chunks.empty());

    // Each chunk should contain complete sentences
    for (const auto& chunk : chunks) {
        if (!chunk.content.empty()) {
            char lastChar = chunk.content.back();
            bool isSentenceEnd = (lastChar == '.' || lastChar == '!' || lastChar == '?');
            bool isLastChunk = (chunk.next_chunk_id == std::nullopt);
            CHECK((isSentenceEnd || isLastChunk));
        }
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "SentenceBasedChunker single sentence",
                 "[vector][chunker][sentence][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::SENTENCE_BASED;
    config.min_chunk_size = 0;

    SentenceBasedChunker chunker(config);
    std::string singleSentence = "This is just one sentence.";
    auto chunks = chunker.chunkDocument(singleSentence, "doc_hash");

    REQUIRE(chunks.size() == 1);
    CHECK(chunks[0].content == singleSentence);
}

TEST_CASE_METHOD(DocumentChunkerFixture, "SentenceBasedChunker long sentence",
                 "[vector][chunker][sentence][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::SENTENCE_BASED;
    config.target_chunk_size = 50;
    config.max_chunk_size = 100;

    SentenceBasedChunker chunker(config);
    std::string longSentence = "This is an extremely long sentence that goes on and on and on "
                               "with many words and phrases and clauses that make it much longer "
                               "than our target chunk size would normally allow.";

    auto chunks = chunker.chunkDocument(longSentence, "doc_hash");
    REQUIRE(chunks.size() > 1);
}

// =============================================================================
// Paragraph-Based Chunker Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "ParagraphBasedChunker basic chunking",
                 "[vector][chunker][paragraph][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::PARAGRAPH_BASED;
    config.target_chunk_size = 100;

    ParagraphBasedChunker chunker(config);
    auto chunks = chunker.chunkDocument(paragraphText, "doc_hash");

    REQUIRE_FALSE(chunks.empty());

    for (const auto& chunk : chunks) {
        CHECK_FALSE(chunk.content.starts_with("\n\n"));
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "ParagraphBasedChunker single paragraph",
                 "[vector][chunker][paragraph][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::PARAGRAPH_BASED;

    ParagraphBasedChunker chunker(config);
    std::string singlePara =
        "This is a single paragraph with multiple sentences. But no paragraph breaks.";
    auto chunks = chunker.chunkDocument(singlePara, "doc_hash");

    REQUIRE(chunks.size() == 1);
    CHECK(chunks[0].content == singlePara);
}

// =============================================================================
// Recursive Text Splitter Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "RecursiveTextSplitter basic chunking",
                 "[vector][chunker][recursive][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::RECURSIVE;
    config.target_chunk_size = 80;
    config.separators = {"\n\n", "\n", ". ", " "};

    RecursiveTextSplitter chunker(config);
    auto chunks = chunker.chunkDocument(paragraphText, "doc_hash");

    REQUIRE_FALSE(chunks.empty());

    for (const auto& chunk : chunks) {
        CHECK_FALSE(chunk.content.empty());
        CHECK(chunk.content.size() <= config.max_chunk_size);
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "RecursiveTextSplitter custom separators",
                 "[vector][chunker][recursive][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::RECURSIVE;
    config.target_chunk_size = 50;
    config.separators = {";", ",", " "};

    RecursiveTextSplitter chunker(config);
    std::string text = "First part; second part, third part and fourth part; final part";
    auto chunks = chunker.chunkDocument(text, "doc_hash");

    REQUIRE_FALSE(chunks.empty());

    for (const auto& chunk : chunks) {
        CHECK_FALSE(chunk.content.empty());
    }
}

// =============================================================================
// Sliding Window Chunker Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "SlidingWindowChunker basic chunking",
                 "[vector][chunker][sliding-window][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::SLIDING_WINDOW;
    config.target_chunk_size = 50;
    config.overlap_size = 20;

    SlidingWindowChunker chunker(config);
    auto chunks = chunker.chunkDocument(longText, "doc_hash");

    REQUIRE_FALSE(chunks.empty());

    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];

        if (i < chunks.size() - 1) {
            CHECK(chunk.content.size() >= 20);
            CHECK(chunk.content.size() <= 120);
        }

        if (i > 0) {
            size_t gap = chunk.start_offset > chunks[i - 1].end_offset
                             ? chunk.start_offset - chunks[i - 1].end_offset
                             : 0;
            CHECK(gap <= 10);
        }
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "SlidingWindowChunker percentage overlap",
                 "[vector][chunker][sliding-window][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::SLIDING_WINDOW;
    config.target_chunk_size = 100;
    config.overlap_percentage = 0.25f;
    config.overlap_size = 0;

    SlidingWindowChunker chunker(config);
    auto chunks = chunker.chunkDocument(longText, "doc_hash");

    REQUIRE_FALSE(chunks.empty());

    for (size_t i = 1; i < chunks.size(); ++i) {
        size_t actualOverlap = chunks[i - 1].end_offset - chunks[i].start_offset;
        if (i < chunks.size() - 1) {
            CHECK(actualOverlap >= 5);
            CHECK(actualOverlap <= 60);
        }
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "SlidingWindowChunker precise mode",
                 "[vector][chunker][sliding-window][precise][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::SLIDING_WINDOW;
    config.target_chunk_size = 50;
    config.overlap_size = 20;
    config.preserve_words = false;

    SlidingWindowChunker chunker(config);
    auto chunks = chunker.chunkDocument(longText, "doc_hash");

    REQUIRE_FALSE(chunks.empty());

    for (size_t i = 0; i < chunks.size(); ++i) {
        const auto& chunk = chunks[i];

        if (i < chunks.size() - 1) {
            CHECK(chunk.content.size() >= 42);
            CHECK(chunk.content.size() <= 58);
        }

        if (i > 0) {
            size_t actualOverlap = chunks[i - 1].end_offset - chunks[i].start_offset;
            CHECK(actualOverlap >= 10);
            CHECK(actualOverlap <= 30);
        }
    }
}

// =============================================================================
// Markdown Chunker Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "MarkdownChunker basic chunking",
                 "[vector][chunker][markdown][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::MARKDOWN_AWARE;
    config.target_chunk_size = 100;
    config.min_chunk_size = 10;

    MarkdownChunker chunker(config);
    auto chunks = chunker.chunkDocument(markdownText, "doc_hash");

    REQUIRE_FALSE(chunks.empty());
}

// =============================================================================
// Semantic Chunker Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "SemanticChunker basic chunking",
                 "[vector][chunker][semantic][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.strategy = ChunkingStrategy::SEMANTIC;
    config.target_chunk_size = 100;
    config.semantic_threshold = 0.7f;

    EmbeddingConfig embedConfig;
    auto embedder = std::make_shared<EmbeddingGenerator>(embedConfig);

    SemanticChunker chunker(config, embedder);
    auto chunks = chunker.chunkDocument(sentenceText, "doc_hash");

    REQUIRE_FALSE(chunks.empty());
}

// =============================================================================
// Factory Function Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "createChunker all strategies",
                 "[vector][chunker][factory][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.min_chunk_size = 5;

    std::vector<ChunkingStrategy> strategies = {
        ChunkingStrategy::FIXED_SIZE,      ChunkingStrategy::SENTENCE_BASED,
        ChunkingStrategy::PARAGRAPH_BASED, ChunkingStrategy::SLIDING_WINDOW,
        ChunkingStrategy::RECURSIVE,       ChunkingStrategy::MARKDOWN_AWARE};

    for (auto strategy : strategies) {
        INFO("Testing strategy: " << static_cast<int>(strategy));
        auto chunker = createChunker(strategy, config);
        REQUIRE(chunker != nullptr);

        auto chunks = chunker->chunkDocument(
            "Test text that is longer than the minimum chunk size requirement.", "doc_hash");
        CHECK_FALSE(chunks.empty());
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "createChunker semantic with embedder",
                 "[vector][chunker][factory][semantic][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.min_chunk_size = 5;
    EmbeddingConfig embedConfig;
    auto embedder = std::make_shared<EmbeddingGenerator>(embedConfig);

    auto chunker = createChunker(ChunkingStrategy::SEMANTIC, config, embedder);
    REQUIRE(chunker != nullptr);

    auto chunks = chunker->chunkDocument(
        "Test text that is longer than the minimum chunk size requirement.", "doc_hash");
    CHECK_FALSE(chunks.empty());
}

// =============================================================================
// Validation Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "validateChunks valid chunks",
                 "[vector][chunker][validation][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.min_chunk_size = 10;
    config.max_chunk_size = 100;

    FixedSizeChunker chunker(config);

    std::vector<DocumentChunk> validChunks = {
        DocumentChunk("chunk1", "doc_hash", "This is valid content"),
        DocumentChunk("chunk2", "doc_hash", "Another valid chunk here")};

    CHECK(chunker.validateChunks(validChunks));
}

TEST_CASE_METHOD(DocumentChunkerFixture, "validateChunks invalid size",
                 "[vector][chunker][validation][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.min_chunk_size = 20;
    config.max_chunk_size = 50;

    FixedSizeChunker chunker(config);

    std::vector<DocumentChunk> invalidChunks = {
        DocumentChunk("chunk1", "doc_hash", "Too short"),
        DocumentChunk("chunk2", "doc_hash", std::string(100, 'x'))};

    CHECK_FALSE(chunker.validateChunks(invalidChunks));
}

// =============================================================================
// Chunk Operations Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "mergeSmallChunks",
                 "[vector][chunker][operations][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.min_chunk_size = 50;

    FixedSizeChunker chunker(config);

    std::vector<DocumentChunk> smallChunks = {
        DocumentChunk("chunk1", "doc_hash", "Small"), DocumentChunk("chunk2", "doc_hash", "Tiny"),
        DocumentChunk("chunk3", "doc_hash", "Regular sized chunk that meets minimum")};

    auto merged = chunker.mergeSmallChunks(smallChunks);

    CHECK(merged.size() < smallChunks.size());

    if (!merged.empty()) {
        for (size_t i = 0; i < merged.size() - 1; ++i) {
            CHECK(merged[i].content.size() >= config.min_chunk_size);
        }
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "splitLargeChunks",
                 "[vector][chunker][operations][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.max_chunk_size = 50;

    FixedSizeChunker chunker(config);

    std::vector<DocumentChunk> largeChunks = {
        DocumentChunk("chunk1", "doc_hash", std::string(200, 'x')),
        DocumentChunk("chunk2", "doc_hash", "Normal chunk")};

    auto split = chunker.splitLargeChunks(largeChunks);

    CHECK(split.size() > largeChunks.size());

    for (const auto& chunk : split) {
        CHECK(chunk.content.size() <= config.max_chunk_size);
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "addOverlap",
                 "[vector][chunker][operations][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.overlap_size = 10;

    FixedSizeChunker chunker(config);

    std::vector<DocumentChunk> chunks = {
        DocumentChunk("chunk1", "doc_hash", "First chunk content here"),
        DocumentChunk("chunk2", "doc_hash", "Second chunk content here"),
        DocumentChunk("chunk3", "doc_hash", "Third chunk content here")};

    chunks[0].start_offset = 0;
    chunks[0].end_offset = chunks[0].content.size();
    chunks[1].start_offset = chunks[0].end_offset;
    chunks[1].end_offset = chunks[1].start_offset + chunks[1].content.size();
    chunks[2].start_offset = chunks[1].end_offset;
    chunks[2].end_offset = chunks[2].start_offset + chunks[2].content.size();

    auto withOverlap = chunker.addOverlap(chunks);

    CHECK(withOverlap.size() >= chunks.size());

    for (size_t i = 1; i < withOverlap.size(); ++i) {
        if (withOverlap[i].start_offset < withOverlap[i - 1].end_offset) {
            CHECK_FALSE(withOverlap[i].overlapping_chunks.empty());
        }
    }
}

// =============================================================================
// Statistics Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "ChunkingStats tracking",
                 "[vector][chunker][stats][catch2]") {
    ChunkingConfig config = defaultConfig;
    FixedSizeChunker chunker(config);

    chunker.resetStats();
    auto initialStats = chunker.getStats();
    CHECK(initialStats.total_documents == 0);
    CHECK(initialStats.total_chunks == 0);

    chunker.chunkDocument(sentenceText, "doc1");
    chunker.chunkDocument(paragraphText, "doc2");

    auto stats = chunker.getStats();
    CHECK(stats.total_documents == 2);
    CHECK(stats.total_chunks > 0);
    CHECK(stats.avg_chunk_size > 0);
}

// =============================================================================
// Error Handling Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "empty document",
                 "[vector][chunker][error][catch2]") {
    ChunkingConfig config = defaultConfig;
    FixedSizeChunker chunker(config);

    auto chunks = chunker.chunkDocument("", "doc_hash");
    CHECK(chunks.empty());
}

TEST_CASE_METHOD(DocumentChunkerFixture, "invalid configuration",
                 "[vector][chunker][error][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.min_chunk_size = 100;
    config.max_chunk_size = 50;
    config.target_chunk_size = 75;

    FixedSizeChunker chunker(config);

    auto chunks = chunker.chunkDocument(longText, "doc_hash");

    if (!chunks.empty()) {
        for (const auto& chunk : chunks) {
            CHECK(chunk.content.size() <= 100);
        }
    }
}

// =============================================================================
// Utility Functions Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "estimateTokenCount",
                 "[vector][chunker][utility][catch2]") {
    ChunkingConfig config = defaultConfig;
    FixedSizeChunker chunker(config);

    std::string text = "This is a test sentence with several words.";
    size_t estimated = chunker.estimateTokenCount(text);

    CHECK(estimated > 0);
    CHECK(estimated < text.size());
}

TEST_CASE_METHOD(DocumentChunkerFixture, "findSentenceBoundaries",
                 "[vector][chunker][utility][catch2]") {
    ChunkingConfig config = defaultConfig;
    FixedSizeChunker chunker(config);

    auto boundaries = chunker.findSentenceBoundaries(sentenceText);

    CHECK_FALSE(boundaries.empty());

    for (size_t pos : boundaries) {
        if (pos > 0 && pos <= sentenceText.size()) {
            size_t checkPos = pos - 1;
            while (checkPos > 0 && std::isspace(sentenceText[checkPos])) {
                checkPos--;
            }
            if (checkPos < sentenceText.size()) {
                char ch = sentenceText[checkPos];
                CHECK((ch == '.' || ch == '!' || ch == '?' || pos == sentenceText.size()));
            }
        }
    }
}

TEST_CASE_METHOD(DocumentChunkerFixture, "findParagraphBoundaries",
                 "[vector][chunker][utility][catch2]") {
    ChunkingConfig config = defaultConfig;
    FixedSizeChunker chunker(config);

    auto boundaries = chunker.findParagraphBoundaries(paragraphText);

    CHECK_FALSE(boundaries.empty());

    for (size_t pos : boundaries) {
        if (pos >= 2 && pos < paragraphText.size()) {
            std::string check = paragraphText.substr(pos - 2, 2);
            CHECK(check == "\n\n");
        }
    }
}

// =============================================================================
// Async Operations Tests
// =============================================================================

TEST_CASE_METHOD(DocumentChunkerFixture, "chunkDocumentAsync",
                 "[vector][chunker][async][catch2]") {
    ChunkingConfig config = defaultConfig;
    FixedSizeChunker chunker(config);

    auto future = chunker.chunkDocumentAsync(sentenceText, "doc_hash");
    auto chunks = future.get();

    REQUIRE_FALSE(chunks.empty());
    CHECK(chunks[0].document_hash == "doc_hash");
}

TEST_CASE_METHOD(DocumentChunkerFixture, "chunkDocumentsBatch",
                 "[vector][chunker][batch][catch2]") {
    ChunkingConfig config = defaultConfig;
    config.min_chunk_size = 10;
    FixedSizeChunker chunker(config);

    std::vector<std::string> documents = {sentenceText, paragraphText, shortText};
    std::vector<std::string> hashes = {"hash1", "hash2", "hash3"};

    auto batchResults = chunker.chunkDocuments(documents, hashes);

    REQUIRE(batchResults.size() == documents.size());

    for (size_t i = 0; i < batchResults.size(); ++i) {
        if (i < 2) {
            CHECK_FALSE(batchResults[i].empty());
        }
        if (!batchResults[i].empty()) {
            CHECK(batchResults[i][0].document_hash == hashes[i]);
        }
    }
}

// =============================================================================
// Namespace Utility Functions Tests
// =============================================================================

TEST_CASE("chunking_utils calculateOptimalChunkSize", "[vector][chunker][utils][catch2]") {
    size_t minilmSize = chunking_utils::calculateOptimalChunkSize("all-MiniLM-L6-v2");
    CHECK(minilmSize > 0);
    CHECK(minilmSize <= 512);

    size_t mpnetSize = chunking_utils::calculateOptimalChunkSize("all-mpnet-base-v2");
    CHECK(mpnetSize > 0);
    CHECK(mpnetSize <= 512);
}

TEST_CASE("chunking_utils estimateTokens", "[vector][chunker][utils][catch2]") {
    std::string text = "This is a sample text for token estimation.";
    size_t tokens = chunking_utils::estimateTokens(text);

    CHECK(tokens > 0);
    CHECK(tokens < text.size());
}

TEST_CASE("chunking_utils preprocessText", "[vector][chunker][utils][catch2]") {
    std::string text = "  This   has    extra   spaces.  \n\n";
    std::string processed = chunking_utils::preprocessText(text);

    CHECK(processed.find("   ") == std::string::npos);
    CHECK(processed.front() == 'T');
    CHECK(processed.back() == '.');
}

TEST_CASE("chunking_utils detectLanguage", "[vector][chunker][utils][catch2]") {
    std::string english = "This is English text.";
    std::string lang = chunking_utils::detectLanguage(english);

    CHECK_FALSE(lang.empty());
}

TEST_CASE("chunking_utils calculateSemanticCoherence", "[vector][chunker][utils][catch2]") {
    std::string coherent = "The cat sat on the mat. The mat was red. The cat was happy.";
    double coherence = chunking_utils::calculateSemanticCoherence(coherent);

    CHECK(coherence >= 0.0);
    CHECK(coherence <= 1.0);
}

TEST_CASE("chunking_utils mergeOverlappingChunks", "[vector][chunker][utils][catch2]") {
    std::vector<DocumentChunk> chunks;

    DocumentChunk chunk1("c1", "doc", "First chunk content");
    chunk1.start_offset = 0;
    chunk1.end_offset = 19;

    DocumentChunk chunk2("c2", "doc", "chunk content overlaps");
    chunk2.start_offset = 5;
    chunk2.end_offset = 28;

    DocumentChunk chunk3("c3", "doc", "Third separate chunk");
    chunk3.start_offset = 40;
    chunk3.end_offset = 60;

    chunks.push_back(chunk1);
    chunks.push_back(chunk2);
    chunks.push_back(chunk3);

    auto merged = chunking_utils::mergeOverlappingChunks(chunks, 0.5);

    CHECK(merged.size() == 2);
}

TEST_CASE("chunking_utils deduplicateChunks", "[vector][chunker][utils][catch2]") {
    std::vector<DocumentChunk> chunks;

    chunks.push_back(DocumentChunk("c1", "doc", "Identical content"));
    chunks.push_back(DocumentChunk("c2", "doc", "Identical content"));
    chunks.push_back(DocumentChunk("c3", "doc", "Different content"));

    auto deduped = chunking_utils::deduplicateChunks(chunks, 0.95);

    CHECK(deduped.size() == 2);
}

TEST_CASE("chunking_utils createChunkingSummary", "[vector][chunker][utils][catch2]") {
    std::vector<DocumentChunk> chunks;
    chunks.push_back(DocumentChunk("c1", "doc", "First chunk"));
    chunks.push_back(DocumentChunk("c2", "doc", "Second chunk"));

    std::string summary = chunking_utils::createChunkingSummary(chunks);

    CHECK_FALSE(summary.empty());
    CHECK(summary.find("2") != std::string::npos);
}
