#include <gtest/gtest.h>
#include <yams/indexing/document_indexer.h>
#include <yams/indexing/indexing_pipeline.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/database.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/connection_pool.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <iostream>

using namespace yams;
using namespace yams::indexing;
using namespace yams::metadata;

class IndexingTest : public ::testing::Test {
protected:
    void SetUp() override {
        // For tests that don't need database, we'll skip setup
    }
    
    void TearDown() override {
        // Clean up if needed
    }
    
    void SetUpWithDatabase() {
        // Create test directory
        testDir_ = std::filesystem::temp_directory_path() / "yams_indexing_test";
        std::filesystem::create_directories(testDir_);
        
        // Initialize database
        dbPath_ = testDir_ / "test.db";
        database_ = std::make_unique<Database>();
        auto openResult = database_->open(dbPath_.string());
        if (!openResult) {
            throw std::runtime_error("Failed to open database: " + openResult.error().message);
        }
        
        // Initialize connection pool
        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolConfig);
        
        // Initialize metadata repository  
        metadataRepo_ = std::make_shared<MetadataRepository>(*pool_);
        
        // Run migrations
        Migration migration;
        auto result = migration.migrate(*database_);
        if (!result) {
            throw std::runtime_error("Failed to initialize database: " + result.error().message);
        }
        
        // Create some test files
        createTestFile("test1.txt", "This is a test document with some content.");
        createTestFile("test2.md", "# Markdown Test\n\nThis is a **markdown** document.");
        createTestFile("test3.cpp", "#include <iostream>\nint main() { return 0; }");
    }
    
    void createTestFile(const std::string& filename, const std::string& content) {
        std::ofstream file(testDir_ / filename);
        file << content;
        file.close();
    }
    
protected:
    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;
    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
};

TEST_F(IndexingTest, ContentChunking) {
    // Test the content processor's chunking functionality
    class TestContentProcessor : public IContentProcessor {
    public:
        std::vector<ContentChunk> chunkContent(
            const std::string& content,
            const std::string& documentId,
            const IndexingConfig& config) override {
            
            std::vector<ContentChunk> chunks;
            
            if (content.size() <= config.chunkSize) {
                ContentChunk chunk;
                chunk.documentId = documentId;
                chunk.chunkIndex = 0;
                chunk.startOffset = 0;
                chunk.endOffset = content.size();
                chunk.content = content;
                chunks.push_back(chunk);
            } else {
                // Split into chunks
                size_t offset = 0;
                size_t index = 0;
                while (offset < content.size()) {
                    ContentChunk chunk;
                    chunk.documentId = documentId;
                    chunk.chunkIndex = index++;
                    chunk.startOffset = offset;
                    chunk.endOffset = std::min(offset + config.chunkSize, content.size());
                    chunk.content = content.substr(offset, config.chunkSize);
                    chunks.push_back(chunk);
                    
                    offset += config.chunkSize - config.overlapSize;
                    if (offset + config.overlapSize >= content.size()) break;
                }
            }
            return chunks;
        }
        
        std::string preprocessText(const std::string& text) override {
            return text;
        }
        
        std::vector<std::pair<std::string, double>> extractKeyTerms(
            const std::string& content, size_t maxTerms) override {
            return {};
        }
    };
    
    TestContentProcessor processor;
    IndexingConfig config;
    config.chunkSize = 20;
    config.overlapSize = 5;
    
    // Test small content (single chunk)
    auto chunks = processor.chunkContent("Short content", "doc1", config);
    EXPECT_EQ(chunks.size(), 1);
    EXPECT_EQ(chunks[0].content, "Short content");
    
    // Test large content (multiple chunks with overlap)
    std::string largeContent = "This is a very long content that should be split into multiple chunks";
    chunks = processor.chunkContent(largeContent, "doc2", config);
    EXPECT_GT(chunks.size(), 1);
    
    // Verify overlap
    for (size_t i = 1; i < chunks.size(); i++) {
        EXPECT_LT(chunks[i].startOffset, chunks[i-1].endOffset);
    }
}

TEST_F(IndexingTest, SingleDocumentIndexing) {
    SetUpWithDatabase();
    
    auto indexer = createDocumentIndexer(metadataRepo_);
    ASSERT_NE(indexer, nullptr);
    
    // Index a single document
    IndexingConfig config;
    auto result = indexer->indexDocument(testDir_ / "test1.txt", config);
    
    ASSERT_TRUE(result.has_value());
    auto& indexResult = result.value();
    
    EXPECT_EQ(indexResult.status, IndexingStatus::Completed);
    EXPECT_FALSE(indexResult.documentId.empty());
    EXPECT_GT(indexResult.chunksCreated, 0);
    EXPECT_FALSE(indexResult.error.has_value());
    
    // Verify document was added to database
    int64_t docId = std::stoll(indexResult.documentId);
    auto docResult = metadataRepo_->getDocument(docId);
    ASSERT_TRUE(docResult.has_value());
    ASSERT_TRUE(docResult.value().has_value());
    
    auto& doc = docResult.value().value();
    EXPECT_EQ(doc.fileName, "test1.txt");
    EXPECT_EQ(doc.fileExtension, ".txt");
}

TEST_F(IndexingTest, BatchDocumentIndexing) {
    SetUpWithDatabase();
    
    auto indexer = createDocumentIndexer(metadataRepo_);
    
    // Prepare batch of documents
    std::vector<std::filesystem::path> paths = {
        testDir_ / "test1.txt",
        testDir_ / "test2.md",
        testDir_ / "test3.cpp"
    };
    
    // Index batch with progress callback
    size_t progressCalls = 0;
    auto progressCallback = [&progressCalls](size_t current, size_t total, const IndexingResult& result) {
        progressCalls++;
        EXPECT_LE(current, total);
        EXPECT_EQ(total, 3);
    };
    
    IndexingConfig config;
    auto results = indexer->indexDocuments(paths, config, progressCallback);
    
    ASSERT_TRUE(results.has_value());
    auto& batchResults = results.value();
    
    EXPECT_EQ(batchResults.size(), 3);
    EXPECT_EQ(progressCalls, 3);
    
    // Verify all documents were indexed successfully
    for (const auto& result : batchResults) {
        EXPECT_EQ(result.status, IndexingStatus::Completed);
        EXPECT_FALSE(result.documentId.empty());
    }
}

TEST_F(IndexingTest, IncrementalIndexing) {
    SetUpWithDatabase();
    
    auto indexer = createDocumentIndexer(metadataRepo_);
    
    // Index a document
    auto path = testDir_ / "test1.txt";
    IndexingConfig config;
    auto result1 = indexer->indexDocument(path, config);
    ASSERT_TRUE(result1.has_value());
    
    // Check if needs re-indexing (should be false)
    auto needsIndexing = indexer->needsIndexing(path);
    ASSERT_TRUE(needsIndexing.has_value());
    EXPECT_FALSE(needsIndexing.value());
    
    // Modify the file
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    createTestFile("test1.txt", "Modified content for the test document.");
    
    // Check if needs re-indexing (should be true now)
    needsIndexing = indexer->needsIndexing(path);
    ASSERT_TRUE(needsIndexing.has_value());
    EXPECT_TRUE(needsIndexing.value());
    
    // Re-index the document
    auto result2 = indexer->updateDocument(path, config);
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(result2.value().status, IndexingStatus::Completed);
}

TEST_F(IndexingTest, DocumentRemoval) {
    SetUpWithDatabase();
    
    auto indexer = createDocumentIndexer(metadataRepo_);
    
    // Index a document
    IndexingConfig config;
    auto result = indexer->indexDocument(testDir_ / "test1.txt", config);
    ASSERT_TRUE(result.has_value());
    
    std::string documentId = result.value().documentId;
    
    // Verify document exists
    int64_t docId = std::stoll(documentId);
    auto docResult = metadataRepo_->getDocument(docId);
    ASSERT_TRUE(docResult.has_value());
    ASSERT_TRUE(docResult.value().has_value());
    
    // Remove document
    auto removeResult = indexer->removeDocument(documentId);
    ASSERT_TRUE(removeResult.has_value());
    
    // Verify document was removed
    docResult = metadataRepo_->getDocument(docId);
    ASSERT_TRUE(docResult.has_value());
    EXPECT_FALSE(docResult.value().has_value());
}

TEST_F(IndexingTest, IndexingPipeline) {
    SetUpWithDatabase();
    
    // Create indexing pipeline
    IndexingPipeline pipeline(metadataRepo_, 2);
    
    // Start pipeline
    pipeline.start();
    EXPECT_TRUE(pipeline.isRunning());
    
    // Queue documents
    auto taskIds = pipeline.queueDocuments({
        testDir_ / "test1.txt",
        testDir_ / "test2.md",
        testDir_ / "test3.cpp"
    });
    
    EXPECT_EQ(taskIds.size(), 3);
    
    // Wait for processing
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // Check statistics
    auto stats = pipeline.getStatistics();
    EXPECT_GT(stats["documents_processed"], 0);
    EXPECT_EQ(stats["documents_failed"], 0);
    
    // Stop pipeline
    pipeline.stop();
    EXPECT_FALSE(pipeline.isRunning());
}

TEST_F(IndexingTest, ErrorHandling) {
    SetUpWithDatabase();
    
    auto indexer = createDocumentIndexer(metadataRepo_);
    
    // Try to index non-existent file
    IndexingConfig config;
    auto result = indexer->indexDocument(testDir_ / "nonexistent.txt", config);
    
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().status, IndexingStatus::Failed);
    EXPECT_TRUE(result.value().error.has_value());
    
    // Try to index a very large file (simulate)
    createTestFile("large.txt", std::string(200 * 1024 * 1024, 'x')); // 200MB
    config.maxDocumentSize = 100 * 1024 * 1024; // 100MB limit
    
    result = indexer->indexDocument(testDir_ / "large.txt", config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().status, IndexingStatus::Skipped);
    EXPECT_TRUE(result.value().error.has_value());
}

TEST_F(IndexingTest, TextPreprocessing) {
    class TestProcessor : public IContentProcessor {
    public:
        std::vector<ContentChunk> chunkContent(
            const std::string& content,
            const std::string& documentId,
            const IndexingConfig& config) override {
            return {};
        }
        
        std::string preprocessText(const std::string& text) override {
            // Basic preprocessing: normalize whitespace
            std::string result;
            bool inSpace = false;
            for (char c : text) {
                if (std::isspace(c)) {
                    if (!inSpace) {
                        result += ' ';
                        inSpace = true;
                    }
                } else {
                    result += c;
                    inSpace = false;
                }
            }
            
            // Trim
            size_t start = result.find_first_not_of(" \t\n\r");
            size_t end = result.find_last_not_of(" \t\n\r");
            if (start == std::string::npos) return "";
            return result.substr(start, end - start + 1);
        }
        
        std::vector<std::pair<std::string, double>> extractKeyTerms(
            const std::string& content, size_t maxTerms) override {
            return {};
        }
    };
    
    TestProcessor processor;
    
    // Test whitespace normalization
    EXPECT_EQ(processor.preprocessText("  hello   world  "), "hello world");
    EXPECT_EQ(processor.preprocessText("line1\n\nline2"), "line1 line2");
    EXPECT_EQ(processor.preprocessText("\t\ttab\t\ttext\t"), "tab text");
    
    // Test empty and whitespace-only
    EXPECT_EQ(processor.preprocessText(""), "");
    EXPECT_EQ(processor.preprocessText("   \n\t  "), "");
}

TEST_F(IndexingTest, KeyTermExtraction) {
    class TestProcessor : public IContentProcessor {
    public:
        std::vector<ContentChunk> chunkContent(
            const std::string& content,
            const std::string& documentId,
            const IndexingConfig& config) override {
            return {};
        }
        
        std::string preprocessText(const std::string& text) override {
            return text;
        }
        
        std::vector<std::pair<std::string, double>> extractKeyTerms(
            const std::string& content, size_t maxTerms) override {
            
            std::unordered_map<std::string, int> termFreq;
            std::istringstream stream(content);
            std::string word;
            
            while (stream >> word) {
                // Simple lowercase conversion
                std::transform(word.begin(), word.end(), word.begin(), ::tolower);
                
                // Remove punctuation
                word.erase(std::remove_if(word.begin(), word.end(), 
                          [](char c) { return !std::isalnum(c); }), word.end());
                
                if (word.length() > 2) {
                    termFreq[word]++;
                }
            }
            
            // Convert to vector and sort
            std::vector<std::pair<std::string, double>> terms;
            for (const auto& [term, freq] : termFreq) {
                terms.emplace_back(term, static_cast<double>(freq));
            }
            
            std::sort(terms.begin(), terms.end(),
                     [](const auto& a, const auto& b) { return a.second > b.second; });
            
            if (terms.size() > maxTerms) {
                terms.resize(maxTerms);
            }
            
            return terms;
        }
    };
    
    TestProcessor processor;
    
    std::string content = "The quick brown fox jumps over the lazy dog. "
                         "The fox is quick and the dog is lazy.";
    
    auto terms = processor.extractKeyTerms(content, 5);
    
    EXPECT_FALSE(terms.empty());
    EXPECT_LE(terms.size(), 5);
    
    // "the" should be most frequent (appears 4 times)
    // But since we filter out words <= 2 chars, check for other terms
    bool foundQuick = false;
    bool foundLazy = false;
    for (const auto& [term, freq] : terms) {
        if (term == "quick") foundQuick = true;
        if (term == "lazy") foundLazy = true;
    }
    EXPECT_TRUE(foundQuick);
    EXPECT_TRUE(foundLazy);
}