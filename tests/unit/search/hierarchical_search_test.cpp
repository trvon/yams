/**
 * @file hierarchical_search_test.cpp
 * @brief Tests for hierarchical embedding and two-stage search (PBI-080)
 *
 * Tests cover:
 * - Configuration options for two-stage search
 * - HybridSearchConfig validation for hierarchical fields
 * - Edge cases and defaults
 */

#include <gtest/gtest.h>

#include <yams/search/hybrid_search_engine.h>

using namespace yams::search;

// =============================================================================
// Two-Stage Search Configuration Tests
// =============================================================================

TEST(HierarchicalSearchConfigTest, TwoStageSearchDefaultsCorrect) {
    HybridSearchConfig config;

    // Verify default configuration for PBI-080 features
    EXPECT_TRUE(config.enable_two_stage) << "Two-stage search should be enabled by default";
    EXPECT_EQ(config.doc_stage_limit, 20u) << "Default doc_stage_limit should be 20";
    EXPECT_EQ(config.chunk_stage_limit, 10u) << "Default chunk_stage_limit should be 10";
    EXPECT_FLOAT_EQ(config.hierarchy_boost, 0.10f) << "Default hierarchy_boost should be 0.10";
}

TEST(HierarchicalSearchConfigTest, TwoStageSearchCanBeDisabled) {
    HybridSearchConfig config;
    config.enable_two_stage = false;

    EXPECT_FALSE(config.enable_two_stage)
        << "Two-stage search should be disabled when set to false";

    // Disabling should not affect other config fields
    EXPECT_EQ(config.doc_stage_limit, 20u);
    EXPECT_EQ(config.chunk_stage_limit, 10u);
}

TEST(HierarchicalSearchConfigTest, HierarchyBoostConfigurable) {
    HybridSearchConfig config;

    // Test various boost values
    config.hierarchy_boost = 0.05f;
    EXPECT_FLOAT_EQ(config.hierarchy_boost, 0.05f) << "hierarchy_boost should be settable to 0.05";

    config.hierarchy_boost = 0.15f;
    EXPECT_FLOAT_EQ(config.hierarchy_boost, 0.15f) << "hierarchy_boost should be settable to 0.15";

    config.hierarchy_boost = 0.0f; // No boosting
    EXPECT_FLOAT_EQ(config.hierarchy_boost, 0.0f)
        << "hierarchy_boost should be settable to 0.0 (no boost)";

    config.hierarchy_boost = 0.25f; // Higher boost
    EXPECT_FLOAT_EQ(config.hierarchy_boost, 0.25f)
        << "hierarchy_boost should support higher values";
}

TEST(HierarchicalSearchConfigTest, DocStageLimitConfigurable) {
    HybridSearchConfig config;

    // Test various doc_stage_limit values
    config.doc_stage_limit = 10;
    EXPECT_EQ(config.doc_stage_limit, 10u) << "doc_stage_limit should be settable to 10";

    config.doc_stage_limit = 50;
    EXPECT_EQ(config.doc_stage_limit, 50u) << "doc_stage_limit should be settable to 50";

    config.doc_stage_limit = 100;
    EXPECT_EQ(config.doc_stage_limit, 100u) << "doc_stage_limit should support larger values";
}

TEST(HierarchicalSearchConfigTest, ChunkStageLimitConfigurable) {
    HybridSearchConfig config;

    // Test various chunk_stage_limit values
    config.chunk_stage_limit = 5;
    EXPECT_EQ(config.chunk_stage_limit, 5u) << "chunk_stage_limit should be settable to 5";

    config.chunk_stage_limit = 20;
    EXPECT_EQ(config.chunk_stage_limit, 20u) << "chunk_stage_limit should be settable to 20";

    config.chunk_stage_limit = 50;
    EXPECT_EQ(config.chunk_stage_limit, 50u) << "chunk_stage_limit should support larger values";
}

TEST(HierarchicalSearchConfigTest, ConfigValidWithHierarchicalFields) {
    HybridSearchConfig config;

    // Default config should be valid
    EXPECT_TRUE(config.isValid()) << "Default config with hierarchical fields should be valid";

    // Config with two-stage disabled should still be valid
    config.enable_two_stage = false;
    EXPECT_TRUE(config.isValid()) << "Config with two-stage disabled should be valid";

    // Config with modified hierarchical params should be valid
    config.enable_two_stage = true;
    config.doc_stage_limit = 30;
    config.chunk_stage_limit = 15;
    config.hierarchy_boost = 0.20f;
    EXPECT_TRUE(config.isValid()) << "Config with modified hierarchical params should be valid";
}

TEST(HierarchicalSearchConfigTest, EdgeCaseZeroBoost) {
    HybridSearchConfig config;
    config.hierarchy_boost = 0.0f;

    // Zero boost means no hierarchical boosting applied
    // This should be valid and results in: score *= (1.0 + 0.0 * doc_best) = score
    EXPECT_TRUE(config.isValid()) << "Config with zero boost should be valid";
    EXPECT_FLOAT_EQ(config.hierarchy_boost, 0.0f);
}

TEST(HierarchicalSearchConfigTest, EdgeCaseHighBoost) {
    HybridSearchConfig config;
    config.hierarchy_boost = 1.0f; // Very high boost (doubles score at best doc_score=1.0)

    // High boost values should be allowed (though may not be practical)
    EXPECT_FLOAT_EQ(config.hierarchy_boost, 1.0f);
    EXPECT_TRUE(config.isValid()) << "Config with high boost should be valid";
}

TEST(HierarchicalSearchConfigTest, EdgeCaseSmallStageLimits) {
    HybridSearchConfig config;
    config.doc_stage_limit = 1;
    config.chunk_stage_limit = 1;

    // Very small limits should be allowed (though not practical)
    EXPECT_EQ(config.doc_stage_limit, 1u);
    EXPECT_EQ(config.chunk_stage_limit, 1u);
    EXPECT_TRUE(config.isValid()) << "Config with minimal stage limits should be valid";
}

TEST(HierarchicalSearchConfigTest, InteractionWithExistingFields) {
    HybridSearchConfig config;

    // Verify hierarchical fields don't interfere with existing config
    config.enable_two_stage = true;
    config.doc_stage_limit = 25;
    config.hierarchy_boost = 0.12f;

    // Check existing fields still work
    config.vector_weight = 0.6f;
    config.keyword_weight = 0.4f;
    config.final_top_k = 15;
    config.fusion_strategy = HybridSearchConfig::FusionStrategy::LINEAR_COMBINATION;

    EXPECT_TRUE(config.isValid()) << "Hierarchical fields should coexist with existing config";
    EXPECT_FLOAT_EQ(config.vector_weight, 0.6f);
    EXPECT_FLOAT_EQ(config.keyword_weight, 0.4f);
    EXPECT_EQ(config.final_top_k, 15u);
}

TEST(HierarchicalSearchConfigTest, WeightNormalizationPreservesHierarchicalFields) {
    HybridSearchConfig config;
    config.enable_two_stage = true;
    config.doc_stage_limit = 30;
    config.hierarchy_boost = 0.15f;

    // Set weights that don't sum to 1.0
    config.vector_weight = 0.5f;
    config.keyword_weight = 0.5f; // Sum = 1.0, but test normalization doesn't break hierarchy

    config.normalizeWeights();

    // Hierarchical fields should be unchanged
    EXPECT_TRUE(config.enable_two_stage);
    EXPECT_EQ(config.doc_stage_limit, 30u);
    EXPECT_FLOAT_EQ(config.hierarchy_boost, 0.15f);
}
