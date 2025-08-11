#include <gtest/gtest.h>
#include <yams/search/bk_tree.h>
#include <vector>
#include <string>
#include <algorithm>
#include <random>

using namespace yams::search;

class BKTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test data
        testWords_ = {
            "hello", "world", "help", "held", "helm", "hell",
            "word", "work", "worm", "worn", "wolf", "wool",
            "test", "text", "next", "best", "rest", "west",
            "search", "reach", "beach", "peach", "teach", "each"
        };
    }
    
    std::vector<std::string> testWords_;
};

// Test Distance Metrics
TEST_F(BKTreeTest, LevenshteinDistance) {
    LevenshteinDistance metric;
    
    // Identity
    EXPECT_EQ(metric.distance("hello", "hello"), 0);
    
    // Single character operations
    EXPECT_EQ(metric.distance("hello", "hallo"), 1); // substitution
    EXPECT_EQ(metric.distance("hello", "hell"), 1);  // deletion
    EXPECT_EQ(metric.distance("hello", "helloo"), 1); // insertion
    
    // Multiple operations
    EXPECT_EQ(metric.distance("kitten", "sitting"), 3);
    EXPECT_EQ(metric.distance("saturday", "sunday"), 3);
    
    // Empty strings
    EXPECT_EQ(metric.distance("", ""), 0);
    EXPECT_EQ(metric.distance("hello", ""), 5);
    EXPECT_EQ(metric.distance("", "world"), 5);
    
    // Case sensitivity
    EXPECT_EQ(metric.distance("Hello", "hello"), 1);
}

TEST_F(BKTreeTest, DamerauLevenshteinDistance) {
    DamerauLevenshteinDistance metric;
    
    // Basic operations (same as Levenshtein)
    EXPECT_EQ(metric.distance("hello", "hello"), 0);
    EXPECT_EQ(metric.distance("hello", "hallo"), 1);
    EXPECT_EQ(metric.distance("hello", "hell"), 1);
    EXPECT_EQ(metric.distance("hello", "helloo"), 1);
    
    // Transposition (key difference from Levenshtein)
    EXPECT_EQ(metric.distance("hello", "hlelo"), 1); // Single transposition
    EXPECT_EQ(metric.distance("abcd", "acbd"), 1);   // Transpose bc
    
    // Multiple operations
    EXPECT_EQ(metric.distance("ca", "abc"), 2);
    
    // Empty strings
    EXPECT_EQ(metric.distance("", ""), 0);
    EXPECT_EQ(metric.distance("test", ""), 4);
}

// Test BK-Tree Construction and Search
TEST_F(BKTreeTest, BasicConstruction) {
    BKTree tree(std::make_unique<LevenshteinDistance>());
    
    // Add single word
    tree.add("hello");
    auto results = tree.search("hello", 0);
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].first, "hello");
    EXPECT_EQ(results[0].second, 0);
}

TEST_F(BKTreeTest, SearchWithDistance) {
    BKTree tree(std::make_unique<LevenshteinDistance>());
    
    // Add test words
    for (const auto& word : testWords_) {
        tree.add(word);
    }
    
    // Exact match
    auto results = tree.search("hello", 0);
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].first, "hello");
    
    // Distance 1 search
    results = tree.search("hello", 1);
    ASSERT_GT(results.size(), 1);
    
    // Should include "hello" (distance 0) and words like "hell" (distance 1)
    bool foundExact = false;
    bool foundClose = false;
    for (const auto& [word, dist] : results) {
        if (word == "hello" && dist == 0) foundExact = true;
        if (word == "hell" && dist == 1) foundClose = true;
    }
    EXPECT_TRUE(foundExact);
    EXPECT_TRUE(foundClose);
    
    // Distance 2 search
    results = tree.search("help", 2);
    ASSERT_GT(results.size(), 2);
    
    // Verify all results are within distance
    for (const auto& [word, dist] : results) {
        EXPECT_LE(dist, 2);
        LevenshteinDistance metric;
        EXPECT_EQ(metric.distance("help", word), dist);
    }
}

TEST_F(BKTreeTest, EmptyTree) {
    BKTree tree(std::make_unique<LevenshteinDistance>());
    
    auto results = tree.search("hello", 1);
    EXPECT_TRUE(results.empty());
}

TEST_F(BKTreeTest, DuplicateWords) {
    BKTree tree(std::make_unique<LevenshteinDistance>());
    
    tree.add("hello");
    tree.add("hello");
    tree.add("hello");
    
    auto results = tree.search("hello", 0);
    ASSERT_EQ(results.size(), 1);
    EXPECT_EQ(results[0].first, "hello");
}

TEST_F(BKTreeTest, LargeDistanceSearch) {
    BKTree tree(std::make_unique<LevenshteinDistance>());
    
    for (const auto& word : testWords_) {
        tree.add(word);
    }
    
    // Search with very large distance
    auto results = tree.search("xyz", 10);
    
    // Should find many words
    EXPECT_GT(results.size(), 10);
    
    // Verify distances
    LevenshteinDistance metric;
    for (const auto& [word, dist] : results) {
        EXPECT_EQ(metric.distance("xyz", word), dist);
    }
}

// Test Trigram Index
TEST_F(BKTreeTest, TrigramIndex) {
    TrigramIndex index;
    
    // Add some test words
    index.add("hello", "hello");
    index.add("help", "help");
    index.add("world", "world");
    index.add("word", "word");
    
    // Search for similar words
    auto results = index.search("hello", 0.3f);
    
    // Should find at least "hello" itself
    EXPECT_FALSE(results.empty());
    
    // Find the exact match
    bool foundExact = false;
    for (const auto& [id, score] : results) {
        if (id == "hello" && score > 0.9f) {
            foundExact = true;
            break;
        }
    }
    EXPECT_TRUE(foundExact);
}

TEST_F(BKTreeTest, TrigramIndexSearch) {
    TrigramIndex index;
    
    // Add words with IDs
    for (size_t i = 0; i < testWords_.size(); ++i) {
        index.add(std::to_string(i), testWords_[i]);
    }
    
    // Search for similar words
    auto results = index.search("help", 0.3f);
    
    // Should find some results
    EXPECT_FALSE(results.empty());
    
    // Check that we get reasonable similarity scores
    for (const auto& [id, score] : results) {
        EXPECT_GE(score, 0.3f);
        EXPECT_LE(score, 1.0f);
    }
}

TEST_F(BKTreeTest, TrigramIndexSize) {
    TrigramIndex index;
    
    // Initially empty
    EXPECT_EQ(index.size(), 0);
    
    // Add some items
    index.add("1", "hello");
    index.add("2", "world");
    
    EXPECT_EQ(index.size(), 2);
    
    // Clear the index
    index.clear();
    EXPECT_EQ(index.size(), 0);
}

// Test Hybrid Fuzzy Search
TEST_F(BKTreeTest, HybridSearchBasic) {
    HybridFuzzySearch search;
    
    // Add test documents
    for (size_t i = 0; i < testWords_.size(); ++i) {
        search.addDocument(testWords_[i], testWords_[i], std::to_string(i));
    }
    
    // Build index
    search.buildIndex();
    
    // Search for exact match
    auto results = search.search("hello", 10);
    
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0].text, "hello");
    EXPECT_FLOAT_EQ(results[0].score, 1.0);
    
    // Search for fuzzy match
    results = search.search("helo", 10);
    
    ASSERT_FALSE(results.empty());
    // Should find "hello" and "help" among top results
    bool foundHello = false;
    bool foundHelp = false;
    for (const auto& result : results) {
        if (result.text == "hello") foundHello = true;
        if (result.text == "help") foundHelp = true;
    }
    EXPECT_TRUE(foundHello);
    EXPECT_TRUE(foundHelp);
}

TEST_F(BKTreeTest, HybridSearchWithOptions) {
    HybridFuzzySearch search;
    
    // Add documents with content
    search.addDocument("Machine Learning", "ML is about teaching computers", "doc1");
    search.addDocument("Deep Learning", "DL is a subset of ML using neural networks", "doc2");
    search.addDocument("Natural Language", "NLP processes human language", "doc3");
    search.addDocument("Computer Vision", "CV enables computers to see", "doc4");
    
    search.buildIndex();
    
    // Search with different strategies
    SearchOptions options;
    
    // BK-tree only
    options.strategy = SearchStrategy::BKTreeOnly;
    auto results = search.search("Machin Learning", 5, options);
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0].text, "Machine Learning");
    
    // Trigram only
    options.strategy = SearchStrategy::TrigramOnly;
    results = search.search("Lerning", 5, options);
    ASSERT_FALSE(results.empty());
    
    // Hybrid (default)
    options.strategy = SearchStrategy::Hybrid;
    options.maxDistance = 2;
    results = search.search("Deap Learning", 5, options);
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0].text, "Deep Learning");
}

TEST_F(BKTreeTest, HybridSearchContentSearch) {
    HybridFuzzySearch search;
    
    // Add documents with different content
    search.addDocument("Doc1", "The quick brown fox jumps over the lazy dog", "1");
    search.addDocument("Doc2", "Machine learning is revolutionizing technology", "2");
    search.addDocument("Doc3", "The lazy dog sleeps under the tree", "3");
    
    search.buildIndex();
    
    SearchOptions options;
    options.searchContent = true;
    
    // Search for content that appears in multiple docs
    auto results = search.search("lazy", 10, options);
    
    // Should find documents containing "lazy"
    ASSERT_GE(results.size(), 2);
    
    // Search for fuzzy content match
    results = search.search("machin lerning", 10, options);
    ASSERT_FALSE(results.empty());
    
    // Should find Doc2 with machine learning content
    bool foundDoc2 = false;
    for (const auto& result : results) {
        if (result.id == "2") {
            foundDoc2 = true;
            break;
        }
    }
    EXPECT_TRUE(foundDoc2);
}

TEST_F(BKTreeTest, HybridSearchRanking) {
    HybridFuzzySearch search;
    
    // Add words with varying similarity to "hello"
    search.addDocument("hello", "exact match", "1");
    search.addDocument("hallo", "one substitution", "2");
    search.addDocument("hell", "one deletion", "3");
    search.addDocument("help", "one substitution", "4");
    search.addDocument("world", "very different", "5");
    
    search.buildIndex();
    
    auto results = search.search("hello", 5);
    
    // Exact match should be first
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0].text, "hello");
    EXPECT_FLOAT_EQ(results[0].score, 1.0);
    
    // Close matches should have high scores
    for (size_t i = 1; i < std::min(size_t(4), results.size()); ++i) {
        EXPECT_GT(results[i].score, 0.5);
        EXPECT_LT(results[i].score, 1.0);
    }
    
    // Scores should be in descending order
    for (size_t i = 1; i < results.size(); ++i) {
        EXPECT_LE(results[i].score, results[i-1].score);
    }
}

TEST_F(BKTreeTest, HybridSearchEmpty) {
    HybridFuzzySearch search;
    
    // Empty index
    auto results = search.search("hello", 10);
    EXPECT_TRUE(results.empty());
    
    // After building empty index
    search.buildIndex();
    results = search.search("hello", 10);
    EXPECT_TRUE(results.empty());
}

TEST_F(BKTreeTest, HybridSearchSpecialCharacters) {
    HybridFuzzySearch search;
    
    // Add documents with special characters
    search.addDocument("hello@world.com", "email address", "1");
    search.addDocument("test_file.txt", "filename", "2");
    search.addDocument("C++ Programming", "programming language", "3");
    search.addDocument("key=value", "key-value pair", "4");
    
    search.buildIndex();
    
    // Search with special characters
    auto results = search.search("hello@world", 5);
    ASSERT_FALSE(results.empty());
    EXPECT_TRUE(results[0].text.find("hello@world") != std::string::npos);
    
    // Fuzzy search with special characters
    results = search.search("test_fil.txt", 5);
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0].text, "test_file.txt");
}

// Performance Tests
TEST_F(BKTreeTest, PerformanceLargeDataset) {
    BKTree tree(std::make_unique<LevenshteinDistance>());
    
    // Generate a large dataset
    std::vector<std::string> words;
    for (int i = 0; i < 1000; ++i) {
        words.push_back("word" + std::to_string(i));
    }
    
    // Add all words
    for (const auto& word : words) {
        tree.add(word);
    }
    
    // Search should complete in reasonable time
    auto start = std::chrono::high_resolution_clock::now();
    auto results = tree.search("word500", 2);
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Should complete within 100ms for 1000 words
    EXPECT_LT(duration.count(), 100);
    
    // Should find the exact match and nearby words
    ASSERT_FALSE(results.empty());
    EXPECT_EQ(results[0].first, "word500");
}

TEST_F(BKTreeTest, StressTestConcurrentSearch) {
    HybridFuzzySearch search;
    
    // Add a reasonable dataset
    for (int i = 0; i < 100; ++i) {
        search.addDocument(
            "Document " + std::to_string(i),
            "Content for document number " + std::to_string(i),
            std::to_string(i)
        );
    }
    search.buildIndex();
    
    // Perform multiple concurrent searches
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&search, &successCount, i]() {
            auto results = search.search("Document " + std::to_string(i * 10), 5);
            if (!results.empty()) {
                successCount++;
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // All searches should succeed
    EXPECT_EQ(successCount.load(), 10);
}