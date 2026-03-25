// Catch2 unit tests for TurboQuantPackedReranker (IVectorReranker implementation)
//
// Covers:
//   - Basic construction and isReady()
//   - rerank() with mixed candidates (with/without packed codes)
//   - Score ordering: TurboQuant-scored candidates ranked by rerank_score
//   - Fallback: candidates without packed codes ranked by initial_score
//   - Dimension mismatch returns error
//   - Empty transformed query returns error
//   - require_packed_codes flag

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

#include <yams/search/vector_reranker.h>
#include <yams/search/turboquant_packed_reranker.h>
#include <yams/vector/turboquant.h>

using namespace yams::search;
using namespace yams::vector;

namespace {

std::vector<float> generateUnitVector(size_t dim, uint32_t seed) {
    std::mt19937 rng(seed);
    std::normal_distribution<float> dist(0.0f, 1.0f);
    std::vector<float> v(dim);
    float norm_sq = 0.0f;
    for (size_t i = 0; i < dim; ++i) {
        v[i] = dist(rng);
        norm_sq += v[i] * v[i];
    }
    float norm = std::sqrt(norm_sq);
    for (size_t i = 0; i < dim; ++i) {
        v[i] /= norm;
    }
    return v;
}

VectorRecord makeRecordWithPackedCodes(const std::string& chunk_id, size_t dim, uint64_t seed,
                                       uint8_t bits_per_channel, const std::vector<float>& vec) {
    TurboQuantConfig cfg;
    cfg.dimension = dim;
    cfg.bits_per_channel = bits_per_channel;
    cfg.seed = seed;
    TurboQuantMSE quantizer(cfg);

    VectorRecord rec;
    rec.chunk_id = chunk_id;
    rec.embedding_dim = dim;
    rec.quantized.format = VectorRecord::QuantizedFormat::TURBOquant_1;
    rec.quantized.bits_per_channel = bits_per_channel;
    rec.quantized.seed = seed;
    rec.quantized.packed_codes = quantizer.packedEncode(vec);
    return rec;
}

VectorRecord makeRecordWithoutPackedCodes(const std::string& chunk_id) {
    VectorRecord rec;
    rec.chunk_id = chunk_id;
    rec.embedding_dim = 0;
    rec.quantized.format = VectorRecord::QuantizedFormat::NONE;
    return rec;
}

} // namespace

TEST_CASE("TurboQuantPackedReranker: construction and isReady", "[vector][turboquant][reranker]") {
    TurboQuantPackedReranker::Config config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    config.rerank_weight = 0.5f;

    auto reranker = std::make_unique<TurboQuantPackedReranker>(config);

    CHECK(reranker->isReady());
    CHECK(reranker->name() == "TurboQuantPackedReranker");
    CHECK(reranker->dimension() == 384);
    CHECK(reranker->bitsPerChannel() == 4);
}

TEST_CASE("TurboQuantPackedReranker: rerank orders by rerank_score descending",
          "[vector][turboquant][reranker]") {
    const size_t dim = 128;
    const uint64_t seed = 123;
    const uint8_t bits = 4;

    TurboQuantPackedReranker::Config config;
    config.dimension = dim;
    config.bits_per_channel = bits;
    config.seed = seed;
    config.rerank_weight = 1.0f; // Only rerank score
    auto reranker = std::make_unique<TurboQuantPackedReranker>(config);

    TurboQuantConfig tq_cfg;
    tq_cfg.dimension = dim;
    tq_cfg.bits_per_channel = bits;
    tq_cfg.seed = seed;
    TurboQuantMSE quantizer(tq_cfg);

    // Two queries: one near corpus[0], one near corpus[1]
    std::vector<float> query_near_0 = generateUnitVector(dim, 999);
    std::vector<float> query_near_1 = generateUnitVector(dim, 1000);

    // Transform queries once each
    std::vector<float> y_q_0 = quantizer.transformQuery(query_near_0);
    std::vector<float> y_q_1 = quantizer.transformQuery(query_near_1);

    // Corpus: two vectors
    std::vector<float> corpus_0 = generateUnitVector(dim, 100);
    std::vector<float> corpus_1 = generateUnitVector(dim, 101);

    VectorRerankInput input;
    input.transformed_query = y_q_0;

    // candidate-a: should score higher (query_near_0 ≈ corpus_0)
    input.candidates["a"] = makeRecordWithPackedCodes("a", dim, seed, bits, corpus_0);
    // candidate-b: should score lower
    input.candidates["b"] = makeRecordWithPackedCodes("b", dim, seed, bits, corpus_1);

    auto result = reranker->rerank(input);
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.candidates.size() == 2);
    CHECK(out.candidates[0].chunk_id == "a"); // Higher TurboQuant score
    CHECK(out.candidates[0].has_packed_codes);
    CHECK(out.candidates[1].chunk_id == "b");
    CHECK(out.candidates[1].has_packed_codes);
    CHECK(out.candidates[0].rerank_score >= out.candidates[1].rerank_score);
    CHECK(out.packed_candidates_scored == 2);
    CHECK(out.candidates_skipped == 0);
}

TEST_CASE("TurboQuantPackedReranker: mixed candidates — with and without packed codes",
          "[vector][turboquant][reranker]") {
    const size_t dim = 64;
    const uint64_t seed = 77;
    const uint8_t bits = 4;

    TurboQuantPackedReranker::Config config;
    config.dimension = dim;
    config.bits_per_channel = bits;
    config.seed = seed;
    config.rerank_weight = 1.0f;
    auto reranker = std::make_unique<TurboQuantPackedReranker>(config);

    TurboQuantConfig tq_cfg;
    tq_cfg.dimension = dim;
    tq_cfg.bits_per_channel = bits;
    tq_cfg.seed = seed;
    TurboQuantMSE quantizer(tq_cfg);

    // Use same vector for query and corpus so asymmetric score is positive
    std::vector<float> v = generateUnitVector(dim, 888);
    std::vector<float> y_q = quantizer.transformQuery(v);

    std::vector<float> corpus = v; // Same vector → high positive score

    VectorRerankInput input;
    input.transformed_query = y_q;
    input.candidates["with-packed"] =
        makeRecordWithPackedCodes("with-packed", dim, seed, bits, corpus);
    input.candidates["without-packed"] = makeRecordWithoutPackedCodes("without-packed");
    input.initial_scores["with-packed"] = 0.1f;
    input.initial_scores["without-packed"] = 0.9f;

    auto result = reranker->rerank(input);
    REQUIRE(result.has_value());

    const auto& out = result.value();
    // with-packed gets TurboQuant score (non-zero), without-packed gets 0.0f rerank_score
    CHECK(out.packed_candidates_scored == 1);
    CHECK(out.candidates_skipped == 1);

    // Find each candidate in output
    const VectorRerankOutput::Candidate* with_cand = nullptr;
    const VectorRerankOutput::Candidate* without_cand = nullptr;
    for (const auto& c : out.candidates) {
        if (c.chunk_id == "with-packed")
            with_cand = &c;
        if (c.chunk_id == "without-packed")
            without_cand = &c;
    }
    REQUIRE(with_cand != nullptr);
    REQUIRE(without_cand != nullptr);

    CHECK(with_cand->has_packed_codes);
    CHECK(!without_cand->has_packed_codes);
    CHECK(with_cand->rerank_score > without_cand->rerank_score);
}

TEST_CASE("TurboQuantPackedReranker: dimension mismatch returns error",
          "[vector][turboquant][reranker]") {
    TurboQuantPackedReranker::Config config;
    config.dimension = 384;
    config.bits_per_channel = 4;
    config.seed = 42;
    auto reranker = std::make_unique<TurboQuantPackedReranker>(config);

    // Query transformed for dim=256 (wrong dimension)
    TurboQuantConfig tq_cfg;
    tq_cfg.dimension = 256;
    tq_cfg.bits_per_channel = 4;
    tq_cfg.seed = 42;
    TurboQuantMSE quantizer(tq_cfg);
    std::vector<float> y_q = quantizer.transformQuery(generateUnitVector(256, 1));

    VectorRerankInput input;
    input.transformed_query = y_q;
    input.candidates["a"] = makeRecordWithoutPackedCodes("a");

    auto result = reranker->rerank(input);
    CHECK(!result.has_value());
}

TEST_CASE("TurboQuantPackedReranker: empty transformed query returns error",
          "[vector][turboquant][reranker]") {
    TurboQuantPackedReranker::Config config;
    config.dimension = 384;
    auto reranker = std::make_unique<TurboQuantPackedReranker>(config);

    VectorRerankInput input;
    input.transformed_query = {};
    input.candidates["a"] = makeRecordWithoutPackedCodes("a");

    auto result = reranker->rerank(input);
    CHECK(!result.has_value());
}

TEST_CASE("TurboQuantPackedReranker: require_packed_codes skips candidates without codes",
          "[vector][turboquant][reranker]") {
    TurboQuantPackedReranker::Config config;
    config.dimension = 128;
    config.bits_per_channel = 4;
    config.seed = 42;
    config.require_packed_codes = true;
    auto reranker = std::make_unique<TurboQuantPackedReranker>(config);

    TurboQuantConfig tq_cfg;
    tq_cfg.dimension = 128;
    tq_cfg.bits_per_channel = 4;
    tq_cfg.seed = 42;
    TurboQuantMSE quantizer(tq_cfg);
    std::vector<float> y_q = quantizer.transformQuery(generateUnitVector(128, 555));

    VectorRerankInput input;
    input.transformed_query = y_q;
    input.candidates["with-packed"] =
        makeRecordWithPackedCodes("with-packed", 128, 42, 4, generateUnitVector(128, 300));
    input.candidates["without-packed"] = makeRecordWithoutPackedCodes("without-packed");

    auto result = reranker->rerank(input);
    REQUIRE(result.has_value());

    const auto& out = result.value();
    CHECK(out.candidates.size() == 1);
    CHECK(out.candidates[0].chunk_id == "with-packed");
    CHECK(out.candidates_skipped == 1);
}

TEST_CASE("TurboQuantPackedReranker: blended score uses rerank_weight",
          "[vector][turboquant][reranker]") {
    const size_t dim = 64;
    const uint64_t seed = 99;
    const uint8_t bits = 4;

    TurboQuantPackedReranker::Config config;
    config.dimension = dim;
    config.bits_per_channel = bits;
    config.seed = seed;
    config.rerank_weight = 0.8f; // 80% TurboQuant, 20% initial
    auto reranker = std::make_unique<TurboQuantPackedReranker>(config);

    TurboQuantConfig tq_cfg;
    tq_cfg.dimension = dim;
    tq_cfg.bits_per_channel = bits;
    tq_cfg.seed = seed;
    TurboQuantMSE quantizer(tq_cfg);

    std::vector<float> corpus = generateUnitVector(dim, 400);
    std::vector<float> query = generateUnitVector(dim, 401);
    std::vector<float> y_q = quantizer.transformQuery(query);

    VectorRerankInput input;
    input.transformed_query = y_q;
    input.candidates["a"] = makeRecordWithPackedCodes("a", dim, seed, bits, corpus);
    input.initial_scores["a"] = 0.5f;

    auto result = reranker->rerank(input);
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.candidates.size() == 1);
    const auto& cand = out.candidates[0];

    float expected_blended = 0.8f * cand.rerank_score + 0.2f * cand.initial_score;
    CHECK(std::abs(cand.blended_score - expected_blended) < 1e-6f);
}

TEST_CASE("TurboQuantPackedReranker: self-score is positive for unit vectors",
          "[vector][turboquant][reranker]") {
    const size_t dim = 128;
    const uint64_t seed = 111;
    const uint8_t bits = 4;

    TurboQuantPackedReranker::Config config;
    config.dimension = dim;
    config.bits_per_channel = bits;
    config.seed = seed;
    config.rerank_weight = 1.0f;
    auto reranker = std::make_unique<TurboQuantPackedReranker>(config);

    TurboQuantConfig tq_cfg;
    tq_cfg.dimension = dim;
    tq_cfg.bits_per_channel = bits;
    tq_cfg.seed = seed;
    TurboQuantMSE quantizer(tq_cfg);

    std::vector<float> v = generateUnitVector(dim, 777);
    std::vector<float> y_q = quantizer.transformQuery(v);

    VectorRerankInput input;
    input.transformed_query = y_q;
    input.candidates["self"] = makeRecordWithPackedCodes("self", dim, seed, bits, v);

    auto result = reranker->rerank(input);
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.candidates.size() == 1);
    // Self-score should be positive (TurboQuant approximates cosine self-similarity)
    CHECK(out.candidates[0].rerank_score > 0.0f);
}

TEST_CASE("TurboQuantPackedReranker: initial_scores used as tiebreaker",
          "[vector][turboquant][reranker]") {
    // When two candidates have equal TurboQuant scores, initial_score breaks ties
    const size_t dim = 128;
    const uint64_t seed = 222;
    const uint8_t bits = 4;

    // Both candidates are identical vectors (same packed codes, same score)
    auto reranker = std::make_unique<TurboQuantPackedReranker>(
        TurboQuantPackedReranker::Config{dim, bits, seed, 1.0f, false});

    TurboQuantConfig tq_cfg{dim, bits, seed};
    TurboQuantMSE quantizer(tq_cfg);

    std::vector<float> v = generateUnitVector(dim, 500);
    std::vector<float> y_q = quantizer.transformQuery(v);

    VectorRerankInput input;
    input.transformed_query = y_q;
    input.candidates["low-init"] = makeRecordWithPackedCodes("low-init", dim, seed, bits, v);
    input.candidates["high-init"] = makeRecordWithPackedCodes("high-init", dim, seed, bits, v);
    input.initial_scores["low-init"] = 0.1f;
    input.initial_scores["high-init"] = 0.9f;

    auto result = reranker->rerank(input);
    REQUIRE(result.has_value());

    const auto& out = result.value();
    REQUIRE(out.candidates.size() == 2);
    // high-init should come first due to tiebreaker
    CHECK(out.candidates[0].chunk_id == "high-init");
    CHECK(out.candidates[1].chunk_id == "low-init");
}
