/**
 * @file result_ranker_catch2_test.cpp
 * @brief Tests for ResultRanker and AdvancedRanker (search/result_ranker.h)
 *
 * Covers: calculateTFIDF, calculateFreshnessScore, calculateSizeScore,
 *         calculatePositionScore, calculatePhraseBonus, calculateExactMatchBonus,
 *         extractQueryTerms, rankResults, AdvancedRanker algorithm selection.
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/query_ast.h>
#include <yams/search/result_ranker.h>
#include <yams/search/search_results.h>

#include <chrono>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

using namespace yams::search;
using Catch::Approx;
using yams::DocumentId;

// ────────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────────
namespace {

/// Build a minimal SearchResultItem for scoring tests
SearchResultItem makeItem(float relevance = 0.0f, size_t fileSize = 10000,
                          std::chrono::system_clock::time_point lastMod = {},
                          float termFreq = 0.0f) {
    SearchResultItem item;
    item.documentId = DocumentId{1};
    item.title = "test-doc";
    item.path = "/tmp/test.txt";
    item.contentType = "text/plain";
    item.fileSize = fileSize;
    item.lastModified = lastMod;
    item.relevanceScore = relevance;
    item.termFrequency = termFreq;
    return item;
}

/// Build an item with title (for exact-match / phrase-match tests)
SearchResultItem makeItemWithTitle(const std::string& title, float relevance = 0.0f) {
    SearchResultItem item;
    item.documentId = DocumentId{1};
    item.title = title;
    item.relevanceScore = relevance;
    item.fileSize = 5000;
    return item;
}

/// Build a SearchHighlight for position-score tests
SearchHighlight makeHighlight(size_t startOffset, size_t endOffset) {
    SearchHighlight h;
    h.field = "content";
    h.snippet = "...";
    h.startOffset = startOffset;
    h.endOffset = endOffset;
    return h;
}

} // namespace

// ────────────────────────────────────────────────────────────────────────────────
// RankingConfig defaults
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("RankingConfig has sensible defaults", "[search][ranker][catch2]") {
    RankingConfig cfg;
    CHECK(cfg.termFrequencyWeight == Approx(1.0f));
    CHECK(cfg.inverseDocumentFrequencyWeight == Approx(1.0f));
    CHECK(cfg.freshnessWeight == Approx(0.1f));
    CHECK(cfg.maxFreshnessBoost == Approx(0.2f));
    CHECK(cfg.sizeWeight == Approx(0.05f));
    CHECK(cfg.optimalSize == 10000);
    CHECK(cfg.phraseMatchBonus == Approx(0.3f));
    CHECK(cfg.exactMatchBonus == Approx(0.5f));
    CHECK(cfg.positionWeight == Approx(0.1f));
    CHECK(cfg.sortLimit == 100);

    CHECK(cfg.fieldBoosts.count("title") == 1);
    CHECK(cfg.fieldBoosts.at("title") == Approx(2.0f));
    CHECK(cfg.fieldBoosts.count("content") == 1);
    CHECK(cfg.fieldBoosts.at("content") == Approx(1.0f));
}

// ────────────────────────────────────────────────────────────────────────────────
// CorpusStatistics::getIDF
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("CorpusStatistics getIDF returns expected values", "[search][ranker][catch2]") {
    CorpusStatistics stats;
    stats.totalDocuments = 100;
    stats.termDocumentFrequency["common"] = 50;
    stats.termDocumentFrequency["rare"] = 2;

    // IDF = ln(N / df)
    float idfCommon = stats.getIDF("common");
    float idfRare = stats.getIDF("rare");
    CHECK(idfCommon == Approx(std::log(100.0f / 50.0f)));
    CHECK(idfRare == Approx(std::log(100.0f / 2.0f)));

    // Unknown term → 0.0
    CHECK(stats.getIDF("unknown_term") == Approx(0.0f));

    // Zero-frequency → 0.0
    stats.termDocumentFrequency["zero"] = 0;
    CHECK(stats.getIDF("zero") == Approx(0.0f));
}

// ────────────────────────────────────────────────────────────────────────────────
// ResultRanker — construction & config
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("ResultRanker default construction", "[search][ranker][catch2]") {
    ResultRanker ranker;
    CHECK(ranker.getConfig().termFrequencyWeight == Approx(1.0f));
}

TEST_CASE("ResultRanker setConfig / getConfig round-trip", "[search][ranker][catch2]") {
    ResultRanker ranker;

    RankingConfig cfg;
    cfg.termFrequencyWeight = 2.5f;
    cfg.optimalSize = 20000;
    ranker.setConfig(cfg);

    CHECK(ranker.getConfig().termFrequencyWeight == Approx(2.5f));
    CHECK(ranker.getConfig().optimalSize == 20000);
}

// ────────────────────────────────────────────────────────────────────────────────
// calculateTFIDF
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("calculateTFIDF without corpus stats returns weighted TF", "[search][ranker][catch2]") {
    ResultRanker ranker;

    // Without corpus stats the IDF factor should be 0 for unknown terms,
    // so the score depends on how the implementation handles no-IDF.
    float score = ranker.calculateTFIDF("hello", 3.0f, "content");
    // Score should be non-negative
    CHECK(score >= 0.0f);
}

TEST_CASE("calculateTFIDF with corpus stats factors IDF", "[search][ranker][catch2]") {
    ResultRanker ranker;

    CorpusStatistics stats;
    stats.totalDocuments = 1000;
    stats.termDocumentFrequency["hello"] = 10;
    ranker.setCorpusStatistics(stats);

    float score = ranker.calculateTFIDF("hello", 5.0f, "content");
    CHECK(score > 0.0f);

    // Field boost: title should give higher score than content for same TF
    float titleScore = ranker.calculateTFIDF("hello", 5.0f, "title");
    float contentScore = ranker.calculateTFIDF("hello", 5.0f, "content");
    CHECK(titleScore >= contentScore); // title boost = 2.0
}

TEST_CASE("calculateTFIDF unknown field defaults to boost 1.0", "[search][ranker][catch2]") {
    ResultRanker ranker;

    CorpusStatistics stats;
    stats.totalDocuments = 100;
    stats.termDocumentFrequency["term"] = 10;
    ranker.setCorpusStatistics(stats);

    float knownField = ranker.calculateTFIDF("term", 1.0f, "content");  // boost 1.0
    float unknownField = ranker.calculateTFIDF("term", 1.0f, "foobar"); // boost 1.0 (default)
    CHECK(knownField == Approx(unknownField));
}

// ────────────────────────────────────────────────────────────────────────────────
// calculateFreshnessScore
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("calculateFreshnessScore: recent document scores higher", "[search][ranker][catch2]") {
    ResultRanker ranker;

    auto now = std::chrono::system_clock::now();
    auto recent = now - std::chrono::hours(1);
    auto old = now - std::chrono::hours(24 * 365);

    float recentScore = ranker.calculateFreshnessScore(recent);
    float oldScore = ranker.calculateFreshnessScore(old);

    CHECK(recentScore >= 0.0f);
    CHECK(oldScore >= 0.0f);
    CHECK(recentScore >= oldScore);
}

TEST_CASE("calculateFreshnessScore: default time_point", "[search][ranker][catch2]") {
    ResultRanker ranker;
    // Epoch (default-constructed time_point) should be very old → low score
    std::chrono::system_clock::time_point epoch{};
    float score = ranker.calculateFreshnessScore(epoch);
    CHECK(score >= 0.0f);
    CHECK(score <= ranker.getConfig().maxFreshnessBoost + 0.01f);
}

// ────────────────────────────────────────────────────────────────────────────────
// calculateSizeScore
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("calculateSizeScore: optimal size gets highest score", "[search][ranker][catch2]") {
    RankingConfig cfg;
    cfg.optimalSize = 10000;
    ResultRanker ranker(cfg);

    float optimalScore = ranker.calculateSizeScore(10000);
    float smallScore = ranker.calculateSizeScore(100);
    float largeScore = ranker.calculateSizeScore(1000000);

    CHECK(optimalScore >= smallScore);
    CHECK(optimalScore >= largeScore);
}

TEST_CASE("calculateSizeScore: zero file size", "[search][ranker][catch2]") {
    ResultRanker ranker;
    float score = ranker.calculateSizeScore(0);
    CHECK(score >= 0.0f);
}

// ────────────────────────────────────────────────────────────────────────────────
// calculatePositionScore
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("calculatePositionScore: empty highlights", "[search][ranker][catch2]") {
    ResultRanker ranker;
    std::vector<SearchHighlight> empty;
    float score = ranker.calculatePositionScore(empty);
    CHECK(score >= 0.0f);
}

TEST_CASE("calculatePositionScore: earlier matches score higher", "[search][ranker][catch2]") {
    ResultRanker ranker;

    std::vector<SearchHighlight> earlyMatch = {makeHighlight(10, 20)};
    std::vector<SearchHighlight> lateMatch = {makeHighlight(50000, 50010)};

    float earlyScore = ranker.calculatePositionScore(earlyMatch);
    float lateScore = ranker.calculatePositionScore(lateMatch);

    CHECK(earlyScore >= lateScore);
}

// ────────────────────────────────────────────────────────────────────────────────
// extractQueryTerms
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("extractQueryTerms from TermNode", "[search][ranker][catch2]") {
    ResultRanker ranker;
    TermNode term("hello");

    auto terms = ranker.extractQueryTerms(&term);
    REQUIRE(terms.size() >= 1);
    CHECK(terms[0] == "hello");
}

TEST_CASE("extractQueryTerms from PhraseNode", "[search][ranker][catch2]") {
    ResultRanker ranker;
    PhraseNode phrase("hello world");

    auto terms = ranker.extractQueryTerms(&phrase);
    // Should contain "hello world" or decomposed terms
    REQUIRE(!terms.empty());
}

TEST_CASE("extractQueryTerms from nullptr returns empty", "[search][ranker][catch2]") {
    ResultRanker ranker;
    auto terms = ranker.extractQueryTerms(nullptr);
    CHECK(terms.empty());
}

TEST_CASE("extractQueryTerms from AndNode extracts from both sides", "[search][ranker][catch2]") {
    ResultRanker ranker;

    auto left = std::make_unique<TermNode>("alpha");
    auto right = std::make_unique<TermNode>("beta");
    AndNode andNode(std::move(left), std::move(right));

    auto terms = ranker.extractQueryTerms(&andNode);
    REQUIRE(terms.size() >= 2);
    // Both terms should be present (order may vary)
    bool hasAlpha = false, hasBeta = false;
    for (const auto& t : terms) {
        if (t == "alpha")
            hasAlpha = true;
        if (t == "beta")
            hasBeta = true;
    }
    CHECK(hasAlpha);
    CHECK(hasBeta);
}

// ────────────────────────────────────────────────────────────────────────────────
// calculatePhraseBonus / calculateExactMatchBonus
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("calculatePhraseBonus: phrase query on matching title", "[search][ranker][catch2]") {
    ResultRanker ranker;
    PhraseNode phrase("test document");
    auto item = makeItemWithTitle("test document about things");

    float bonus = ranker.calculatePhraseBonus(item, &phrase);
    CHECK(bonus >= 0.0f);
}

TEST_CASE("calculatePhraseBonus: nullptr query", "[search][ranker][catch2]") {
    ResultRanker ranker;
    auto item = makeItemWithTitle("test");
    float bonus = ranker.calculatePhraseBonus(item, nullptr);
    CHECK(bonus >= 0.0f);
}

TEST_CASE("calculateExactMatchBonus: exact title match", "[search][ranker][catch2]") {
    ResultRanker ranker;
    TermNode term("test-doc");
    auto item = makeItemWithTitle("test-doc");

    float bonus = ranker.calculateExactMatchBonus(item, &term);
    CHECK(bonus >= 0.0f);
}

TEST_CASE("calculateExactMatchBonus: nullptr query", "[search][ranker][catch2]") {
    ResultRanker ranker;
    auto item = makeItemWithTitle("test");
    float bonus = ranker.calculateExactMatchBonus(item, nullptr);
    CHECK(bonus >= 0.0f);
}

// ────────────────────────────────────────────────────────────────────────────────
// calculateScore
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("calculateScore produces non-negative result", "[search][ranker][catch2]") {
    ResultRanker ranker;
    TermNode term("hello");

    auto item = makeItem(0.5f, 10000, std::chrono::system_clock::now(), 3.0f);
    float score = ranker.calculateScore(item, &term);
    CHECK(score >= 0.0f);
}

TEST_CASE("calculateScore with nullptr query", "[search][ranker][catch2]") {
    ResultRanker ranker;
    auto item = makeItem();
    float score = ranker.calculateScore(item, nullptr);
    CHECK(score >= 0.0f);
}

// ────────────────────────────────────────────────────────────────────────────────
// rankResults
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("rankResults sorts items by score descending", "[search][ranker][catch2]") {
    ResultRanker ranker;
    CorpusStatistics stats;
    stats.totalDocuments = 100;
    stats.termDocumentFrequency["important"] = 5;
    ranker.setCorpusStatistics(stats);

    TermNode term("important");

    auto now = std::chrono::system_clock::now();
    std::vector<SearchResultItem> items;
    // Items with different term frequencies should rank differently
    auto item1 = makeItem(0.2f, 10000, now, 1.0f);
    item1.title = "important document";
    auto item2 = makeItem(0.8f, 10000, now, 5.0f);
    item2.title = "very important reference";
    auto item3 = makeItem(0.1f, 10000, now, 0.1f);
    item3.title = "unrelated text";
    items.push_back(item1);
    items.push_back(item2);
    items.push_back(item3);

    ranker.rankResults(items, &term);

    // After ranking, scores should be in descending order
    for (size_t i = 1; i < items.size(); ++i) {
        CHECK(items[i - 1].relevanceScore >= items[i].relevanceScore);
    }
}

TEST_CASE("rankResults empty vector is no-op", "[search][ranker][catch2]") {
    ResultRanker ranker;
    TermNode term("test");

    std::vector<SearchResultItem> empty;
    ranker.rankResults(empty, &term);
    CHECK(empty.empty());
}

TEST_CASE("rankResults single item", "[search][ranker][catch2]") {
    ResultRanker ranker;
    TermNode term("test");

    std::vector<SearchResultItem> items = {makeItem(0.5f)};
    ranker.rankResults(items, &term);
    CHECK(items.size() == 1);
    CHECK(items[0].relevanceScore >= 0.0f);
}

// ────────────────────────────────────────────────────────────────────────────────
// AdvancedRanker
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("AdvancedRanker default uses Hybrid algorithm", "[search][ranker][catch2]") {
    AdvancedRanker ranker;
    CHECK(ranker.getAlgorithm() == RankingAlgorithm::Hybrid);
}

TEST_CASE("AdvancedRanker setAlgorithm / getAlgorithm", "[search][ranker][catch2]") {
    AdvancedRanker ranker;

    ranker.setAlgorithm(RankingAlgorithm::BM25);
    CHECK(ranker.getAlgorithm() == RankingAlgorithm::BM25);

    ranker.setAlgorithm(RankingAlgorithm::Cosine);
    CHECK(ranker.getAlgorithm() == RankingAlgorithm::Cosine);

    ranker.setAlgorithm(RankingAlgorithm::TfIdf);
    CHECK(ranker.getAlgorithm() == RankingAlgorithm::TfIdf);
}

TEST_CASE("AdvancedRanker explicit construction with algorithm", "[search][ranker][catch2]") {
    AdvancedRanker ranker(RankingAlgorithm::BM25);
    CHECK(ranker.getAlgorithm() == RankingAlgorithm::BM25);
}

TEST_CASE("AdvancedRanker getBaseRanker access", "[search][ranker][catch2]") {
    AdvancedRanker ranker;

    // Mutable access
    RankingConfig cfg;
    cfg.optimalSize = 42000;
    ranker.getBaseRanker().setConfig(cfg);
    CHECK(ranker.getBaseRanker().getConfig().optimalSize == 42000);

    // Const access
    const auto& constRanker = ranker;
    CHECK(constRanker.getBaseRanker().getConfig().optimalSize == 42000);
}

TEST_CASE("AdvancedRanker rankResults with each algorithm", "[search][ranker][catch2]") {
    auto now = std::chrono::system_clock::now();
    TermNode term("test");

    auto makeItems = [&]() {
        std::vector<SearchResultItem> items;
        auto item1 = makeItem(0.8f, 5000, now, 4.0f);
        item1.title = "test alpha";
        auto item2 = makeItem(0.3f, 15000, now, 1.0f);
        item2.title = "test beta";
        items.push_back(item1);
        items.push_back(item2);
        return items;
    };

    SECTION("TfIdf") {
        AdvancedRanker ranker(RankingAlgorithm::TfIdf);
        auto items = makeItems();
        ranker.rankResults(items, &term);
        CHECK(items[0].relevanceScore >= items[1].relevanceScore);
    }

    SECTION("BM25") {
        AdvancedRanker ranker(RankingAlgorithm::BM25);
        auto items = makeItems();
        ranker.rankResults(items, &term);
        CHECK(items[0].relevanceScore >= items[1].relevanceScore);
    }

    SECTION("Cosine") {
        AdvancedRanker ranker(RankingAlgorithm::Cosine);
        auto items = makeItems();
        ranker.rankResults(items, &term);
        CHECK(items[0].relevanceScore >= items[1].relevanceScore);
    }

    SECTION("Hybrid") {
        AdvancedRanker ranker(RankingAlgorithm::Hybrid);
        auto items = makeItems();
        ranker.rankResults(items, &term);
        CHECK(items[0].relevanceScore >= items[1].relevanceScore);
    }
}

TEST_CASE("AdvancedRanker rankResults with empty vector", "[search][ranker][catch2]") {
    AdvancedRanker ranker;
    TermNode term("test");
    std::vector<SearchResultItem> empty;
    ranker.rankResults(empty, &term);
    CHECK(empty.empty());
}

// ────────────────────────────────────────────────────────────────────────────────
// RankingConfig sortLimit exercises partial_sort path
// ────────────────────────────────────────────────────────────────────────────────

TEST_CASE("rankResults with sortLimit exercises partial_sort path", "[search][ranker][catch2]") {
    RankingConfig cfg;
    cfg.sortLimit = 5; // Force partial_sort for > 5 items
    ResultRanker ranker(cfg);

    TermNode term("doc");
    auto now = std::chrono::system_clock::now();

    std::vector<SearchResultItem> items;
    for (int i = 0; i < 20; ++i) {
        auto item =
            makeItem(static_cast<float>(i) * 0.05f, 10000, now, static_cast<float>(i) * 0.5f);
        item.title = "doc " + std::to_string(i);
        items.push_back(item);
    }

    ranker.rankResults(items, &term);

    // Top 5 should be sorted descending
    for (size_t i = 1; i < 5 && i < items.size(); ++i) {
        CHECK(items[i - 1].relevanceScore >= items[i].relevanceScore);
    }
}
