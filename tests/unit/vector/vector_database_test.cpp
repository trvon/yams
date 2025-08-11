#include <yams/vector/vector_database.h>

#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <random>
#include <algorithm>

using namespace yams::vector;

class VectorDatabaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_.db_path = "test_vectors.lancedb";
        config_.table_name = "test_embeddings";
        config_.embedding_dim = 384;
        
        db_ = std::make_unique<VectorDatabase>(config_);
        ASSERT_TRUE(db_->initialize());
    }

    void TearDown() override {
        if (db_) {
            db_->dropTable();
            db_->close();
        }
    }

    std::vector<float> generateRandomEmbedding(size_t dim = 384) {
        std::vector<float> embedding(dim);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::normal_distribution<float> dist(0.0f, 1.0f);
        
        for (size_t i = 0; i < dim; ++i) {
            embedding[i] = dist(gen);
        }
        
        return utils::normalizeVector(embedding);
    }

    VectorRecord createTestRecord(const std::string& chunk_id, 
                                  const std::string& document_hash,
                                  const std::string& content) {
        VectorRecord record;
        record.chunk_id = chunk_id;
        record.document_hash = document_hash;
        record.embedding = generateRandomEmbedding();
        record.content = content;
        record.start_offset = 0;
        record.end_offset = content.length();
        record.metadata["source"] = "test";
        return record;
    }

    VectorDatabaseConfig config_;
    std::unique_ptr<VectorDatabase> db_;
};

TEST_F(VectorDatabaseTest, InitializationAndBasicOperations) {
    EXPECT_TRUE(db_->isInitialized());
    EXPECT_TRUE(db_->tableExists());
    EXPECT_EQ(db_->getVectorCount(), 0);
    EXPECT_FALSE(db_->hasError());
}

TEST_F(VectorDatabaseTest, InsertSingleVector) {
    auto record = createTestRecord("chunk_1", "doc_hash_1", "This is a test document.");
    
    EXPECT_TRUE(db_->insertVector(record));
    EXPECT_EQ(db_->getVectorCount(), 1);
    
    auto retrieved = db_->getVector("chunk_1");
    ASSERT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->chunk_id, "chunk_1");
    EXPECT_EQ(retrieved->document_hash, "doc_hash_1");
    EXPECT_EQ(retrieved->content, "This is a test document.");
    EXPECT_EQ(retrieved->embedding.size(), config_.embedding_dim);
}

TEST_F(VectorDatabaseTest, InsertBatchVectors) {
    std::vector<VectorRecord> records;
    
    for (int i = 0; i < 10; ++i) {
        records.push_back(createTestRecord(
            "chunk_" + std::to_string(i),
            "doc_hash_1",
            "Test content " + std::to_string(i)
        ));
    }
    
    EXPECT_TRUE(db_->insertVectorsBatch(records));
    EXPECT_EQ(db_->getVectorCount(), 10);
    
    auto doc_vectors = db_->getVectorsByDocument("doc_hash_1");
    EXPECT_EQ(doc_vectors.size(), 10);
}

TEST_F(VectorDatabaseTest, UpdateVector) {
    auto record = createTestRecord("chunk_1", "doc_hash_1", "Original content");
    EXPECT_TRUE(db_->insertVector(record));
    
    // Update the record
    record.content = "Updated content";
    record.metadata["updated"] = "true";
    
    EXPECT_TRUE(db_->updateVector("chunk_1", record));
    
    auto retrieved = db_->getVector("chunk_1");
    ASSERT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->content, "Updated content");
    EXPECT_EQ(retrieved->metadata.at("updated"), "true");
}

TEST_F(VectorDatabaseTest, DeleteVector) {
    auto record = createTestRecord("chunk_1", "doc_hash_1", "Test content");
    EXPECT_TRUE(db_->insertVector(record));
    EXPECT_EQ(db_->getVectorCount(), 1);
    
    EXPECT_TRUE(db_->deleteVector("chunk_1"));
    EXPECT_EQ(db_->getVectorCount(), 0);
    
    auto retrieved = db_->getVector("chunk_1");
    EXPECT_FALSE(retrieved.has_value());
}

TEST_F(VectorDatabaseTest, DeleteVectorsByDocument) {
    // Insert vectors for multiple documents
    std::vector<VectorRecord> records;
    
    for (int i = 0; i < 5; ++i) {
        records.push_back(createTestRecord(
            "chunk_doc1_" + std::to_string(i),
            "doc_hash_1",
            "Document 1 content " + std::to_string(i)
        ));
        records.push_back(createTestRecord(
            "chunk_doc2_" + std::to_string(i),
            "doc_hash_2",
            "Document 2 content " + std::to_string(i)
        ));
    }
    
    EXPECT_TRUE(db_->insertVectorsBatch(records));
    EXPECT_EQ(db_->getVectorCount(), 10);
    
    // Delete all vectors for document 1
    EXPECT_TRUE(db_->deleteVectorsByDocument("doc_hash_1"));
    EXPECT_EQ(db_->getVectorCount(), 5);
    
    auto doc1_vectors = db_->getVectorsByDocument("doc_hash_1");
    EXPECT_TRUE(doc1_vectors.empty());
    
    auto doc2_vectors = db_->getVectorsByDocument("doc_hash_2");
    EXPECT_EQ(doc2_vectors.size(), 5);
}

TEST_F(VectorDatabaseTest, SimilaritySearch) {
    // Create vectors with known relationships
    auto query_embedding = generateRandomEmbedding();
    
    // Create a similar vector (same + small noise)
    auto similar_embedding = query_embedding;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<float> noise(0.0f, 0.1f);
    
    for (float& val : similar_embedding) {
        val += noise(gen);
    }
    similar_embedding = utils::normalizeVector(similar_embedding);
    
    // Insert test vectors
    VectorRecord similar_record;
    similar_record.chunk_id = "similar_chunk";
    similar_record.document_hash = "doc1";
    similar_record.embedding = similar_embedding;
    similar_record.content = "Similar content";
    
    VectorRecord different_record = createTestRecord("different_chunk", "doc2", "Different content");
    
    EXPECT_TRUE(db_->insertVector(similar_record));
    EXPECT_TRUE(db_->insertVector(different_record));
    
    // Search for similar vectors
    VectorSearchParams params;
    params.k = 10;
    params.similarity_threshold = 0.1f; // Low threshold to include both
    
    auto results = db_->searchSimilar(query_embedding, params);
    
    EXPECT_GE(results.size(), 1);
    
    // The similar vector should have a higher relevance score
    if (results.size() >= 2) {
        EXPECT_GT(results[0].relevance_score, results[1].relevance_score);
    }
    
    // First result should be the similar one
    EXPECT_EQ(results[0].chunk_id, "similar_chunk");
}

TEST_F(VectorDatabaseTest, SimilaritySearchWithFilters) {
    // Insert vectors with different metadata
    auto record1 = createTestRecord("chunk_1", "doc1", "Content 1");
    record1.metadata["category"] = "science";
    record1.metadata["author"] = "alice";
    
    auto record2 = createTestRecord("chunk_2", "doc2", "Content 2");
    record2.metadata["category"] = "technology";
    record2.metadata["author"] = "bob";
    
    auto record3 = createTestRecord("chunk_3", "doc1", "Content 3");
    record3.metadata["category"] = "science";
    record3.metadata["author"] = "alice";
    
    EXPECT_TRUE(db_->insertVector(record1));
    EXPECT_TRUE(db_->insertVector(record2));
    EXPECT_TRUE(db_->insertVector(record3));
    
    // Search with document filter
    VectorSearchParams params;
    params.k = 10;
    params.similarity_threshold = 0.0f;
    params.document_hash = "doc1";
    
    auto results = db_->searchSimilar(generateRandomEmbedding(), params);
    EXPECT_EQ(results.size(), 2);
    
    // Search with metadata filter
    params.document_hash.reset();
    params.metadata_filters["category"] = "science";
    
    results = db_->searchSimilar(generateRandomEmbedding(), params);
    EXPECT_EQ(results.size(), 2);
    
    // Search with multiple metadata filters
    params.metadata_filters["author"] = "alice";
    
    results = db_->searchSimilar(generateRandomEmbedding(), params);
    EXPECT_EQ(results.size(), 2);
}

TEST_F(VectorDatabaseTest, SimilarityThreshold) {
    auto record = createTestRecord("chunk_1", "doc1", "Test content");
    EXPECT_TRUE(db_->insertVector(record));
    
    // Search with high similarity threshold - should return no results
    VectorSearchParams params;
    params.k = 10;
    params.similarity_threshold = 0.99f; // Very high threshold
    
    auto results = db_->searchSimilar(generateRandomEmbedding(), params);
    EXPECT_TRUE(results.empty());
    
    // Search with low similarity threshold - should return the record
    params.similarity_threshold = 0.0f; // Very low threshold
    
    results = db_->searchSimilar(generateRandomEmbedding(), params);
    EXPECT_EQ(results.size(), 1);
}

TEST_F(VectorDatabaseTest, SearchSimilarToDocument) {
    auto record1 = createTestRecord("chunk_1", "doc1", "Source document content");
    auto record2 = createTestRecord("chunk_2", "doc2", "Different document");
    
    EXPECT_TRUE(db_->insertVector(record1));
    EXPECT_TRUE(db_->insertVector(record2));
    
    VectorSearchParams params;
    params.k = 10;
    params.similarity_threshold = 0.0f;
    
    auto results = db_->searchSimilarToDocument("doc1", params);
    EXPECT_GE(results.size(), 1);
}

TEST_F(VectorDatabaseTest, InvalidOperations) {
    // Try to insert invalid vector
    VectorRecord invalid_record;
    invalid_record.chunk_id = "invalid";
    invalid_record.document_hash = "doc1";
    invalid_record.embedding = std::vector<float>(100); // Wrong dimension
    invalid_record.content = "Invalid content";
    
    EXPECT_FALSE(db_->insertVector(invalid_record));
    EXPECT_TRUE(db_->hasError());
    
    // Try to update non-existent vector
    auto valid_record = createTestRecord("chunk_1", "doc1", "Valid content");
    EXPECT_FALSE(db_->updateVector("non_existent", valid_record));
    
    // Try to delete non-existent vector
    EXPECT_FALSE(db_->deleteVector("non_existent"));
}

TEST_F(VectorDatabaseTest, DatabaseStats) {
    // Insert some test data
    std::vector<VectorRecord> records;
    for (int i = 0; i < 5; ++i) {
        records.push_back(createTestRecord(
            "chunk_" + std::to_string(i),
            "doc_" + std::to_string(i / 2), // 2-3 vectors per document
            "Content " + std::to_string(i)
        ));
    }
    
    EXPECT_TRUE(db_->insertVectorsBatch(records));
    
    auto stats = db_->getStats();
    EXPECT_EQ(stats.total_vectors, 5);
    EXPECT_EQ(stats.total_documents, 3); // doc_0, doc_1, doc_2
    EXPECT_GT(stats.avg_embedding_magnitude, 0.0);
    EXPECT_GT(stats.index_size_bytes, 0);
}

TEST_F(VectorDatabaseTest, UtilityFunctions) {
    // Test vector normalization
    std::vector<float> vec = {3.0f, 4.0f, 0.0f};
    auto normalized = utils::normalizeVector(vec);
    
    double magnitude = 0.0;
    for (float val : normalized) {
        magnitude += val * val;
    }
    magnitude = std::sqrt(magnitude);
    
    EXPECT_NEAR(magnitude, 1.0, 1e-6); // Should be unit vector
    
    // Test chunk ID generation
    auto chunk_id1 = utils::generateChunkId("doc_hash", 0);
    auto chunk_id2 = utils::generateChunkId("doc_hash", 1);
    
    EXPECT_NE(chunk_id1, chunk_id2);
    EXPECT_TRUE(chunk_id1.find("doc_hash") != std::string::npos);
    EXPECT_TRUE(chunk_id1.find("_chunk_") != std::string::npos);
    
    // Test record validation
    auto valid_record = createTestRecord("chunk_1", "doc1", "Valid content");
    EXPECT_TRUE(utils::validateVectorRecord(valid_record, config_.embedding_dim));
    
    valid_record.embedding.clear(); // Invalid dimension
    EXPECT_FALSE(utils::validateVectorRecord(valid_record, config_.embedding_dim));
    
    // Test similarity/distance conversion
    double similarity = 0.8;
    double distance = utils::similarityToDistance(similarity);
    double back_to_similarity = utils::distanceToSimilarity(distance);
    
    EXPECT_NEAR(similarity, back_to_similarity, 1e-9);
}

TEST_F(VectorDatabaseTest, CosineSimilarity) {
    std::vector<float> vec1 = {1.0f, 0.0f, 0.0f};
    std::vector<float> vec2 = {0.0f, 1.0f, 0.0f};
    std::vector<float> vec3 = {1.0f, 0.0f, 0.0f}; // Same as vec1
    
    // Orthogonal vectors should have similarity 0
    double sim1 = VectorDatabase::computeCosineSimilarity(vec1, vec2);
    EXPECT_NEAR(sim1, 0.0, 1e-9);
    
    // Identical vectors should have similarity 1
    double sim2 = VectorDatabase::computeCosineSimilarity(vec1, vec3);
    EXPECT_NEAR(sim2, 1.0, 1e-9);
    
    // Test with normalized vectors
    vec1 = {0.6f, 0.8f};
    vec2 = {0.8f, 0.6f};
    
    double sim3 = VectorDatabase::computeCosineSimilarity(vec1, vec2);
    EXPECT_GT(sim3, 0.0);
    EXPECT_LT(sim3, 1.0);
}

// Performance test (disabled by default)
TEST_F(VectorDatabaseTest, DISABLED_PerformanceTest) {
    const size_t num_vectors = 10000;
    const size_t batch_size = 1000;
    
    std::cout << "Inserting " << num_vectors << " vectors..." << std::endl;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (size_t batch = 0; batch < num_vectors; batch += batch_size) {
        std::vector<VectorRecord> records;
        
        for (size_t i = batch; i < std::min(batch + batch_size, num_vectors); ++i) {
            records.push_back(createTestRecord(
                "chunk_" + std::to_string(i),
                "doc_" + std::to_string(i / 100), // ~100 chunks per document
                "Performance test content " + std::to_string(i)
            ));
        }
        
        EXPECT_TRUE(db_->insertVectorsBatch(records));
    }
    
    auto insert_end = std::chrono::high_resolution_clock::now();
    auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        insert_end - start);
    
    std::cout << "Insert completed in " << insert_duration.count() << "ms" << std::endl;
    std::cout << "Throughput: " << (num_vectors * 1000 / insert_duration.count()) 
              << " vectors/sec" << std::endl;
    
    // Test search performance
    auto query_embedding = generateRandomEmbedding();
    VectorSearchParams params;
    params.k = 10;
    params.similarity_threshold = 0.0f;
    
    const size_t num_queries = 100;
    auto search_start = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < num_queries; ++i) {
        auto results = db_->searchSimilar(query_embedding, params);
        EXPECT_LE(results.size(), params.k);
    }
    
    auto search_end = std::chrono::high_resolution_clock::now();
    auto search_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        search_end - search_start);
    
    std::cout << "Search completed " << num_queries << " queries in " 
              << search_duration.count() << "ms" << std::endl;
    std::cout << "Average query time: " 
              << (search_duration.count() / static_cast<double>(num_queries)) 
              << "ms" << std::endl;
    
    auto stats = db_->getStats();
    std::cout << "Final stats:" << std::endl;
    std::cout << "  Total vectors: " << stats.total_vectors << std::endl;
    std::cout << "  Total documents: " << stats.total_documents << std::endl;
    std::cout << "  Index size: " << (stats.index_size_bytes / 1024 / 1024) << "MB" << std::endl;
}