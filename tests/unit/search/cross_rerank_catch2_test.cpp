#include <sqlite3.h>
#include <catch2/catch_test_macros.hpp>

#include "src/search/cross_rerank_internal.h"

#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <random>

#include <string>
#include <vector>

using yams::ErrorCode;
using yams::Result;
using yams::search::SearchEngineConfig;
using yams::search::SearchResult;
using yams::search::detail::applyCrossRerank;
using yams::search::detail::CrossRerankOutcome;

namespace {

SearchResult makeResult(std::string hash, float score) {
    SearchResult r;
    r.document.sha256Hash = std::move(hash);
    r.document.filePath = "/x/" + r.document.sha256Hash;
    r.score = score;
    return r;
}

std::vector<SearchResult> window3() {
    return {makeResult("a", 0.9F), makeResult("b", 0.6F), makeResult("c", 0.3F)};
}

SearchEngineConfig blendConfig() {
    SearchEngineConfig cfg;
    cfg.rerankReplaceScores = false;
    cfg.rerankBlendWeight = 0.3F;
    cfg.rerankScoreGapThreshold = 0.0F;
    cfg.rerankSnippetMaxChars = 256;
    return cfg;
}

// Stub scorer that returns a fixed set of scores (metadataRepo unused → nullptr).
auto fixedScorer(std::vector<float> scores) {
    return [scores = std::move(scores)](
               const std::string&, const std::vector<std::string>&) -> Result<std::vector<float>> {
        return scores;
    };
}

} // namespace

TEST_CASE("applyCrossRerank reorders the window and reports Applied", "[search][rerank][catch2]") {
    auto results = window3();
    auto cfg = blendConfig();
    cfg.rerankReplaceScores = true; // pure rerank order
    // Reverse the fusion order: c highest, a lowest.
    auto outcome = applyCrossRerank(results, "q", cfg, 3, fixedScorer({0.1F, 0.5F, 0.9F}), nullptr);

    CHECK(outcome.status == CrossRerankOutcome::Status::Applied);
    CHECK(outcome.attempted);
    CHECK(results.front().document.sha256Hash == "c"); // reranked to the front
    CHECK(results.back().document.sha256Hash == "a");
    CHECK(outcome.docTraces.size() == 3);
    CHECK(outcome.components.size() == 3);
}

TEST_CASE("applyCrossRerank blend keeps fusion influence vs replace", "[search][rerank][catch2]") {
    // Fusion order a>b>c; reranker mildly prefers c. Replace flips to c; blend(0.3) keeps a on top.
    auto scorer = fixedScorer({0.0F, 0.4F, 1.0F});

    auto replaceResults = window3();
    auto replaceCfg = blendConfig();
    replaceCfg.rerankReplaceScores = true;
    applyCrossRerank(replaceResults, "q", replaceCfg, 3, scorer, nullptr);
    CHECK(replaceResults.front().document.sha256Hash == "c");

    auto blendResults = window3();
    applyCrossRerank(blendResults, "q", blendConfig(), 3, scorer, nullptr);
    CHECK(blendResults.front().document.sha256Hash == "a"); // fusion still dominates at w=0.3
}

TEST_CASE("applyCrossRerank skips on no score variance", "[search][rerank][catch2]") {
    auto results = window3();
    auto outcome =
        applyCrossRerank(results, "q", blendConfig(), 3, fixedScorer({0.5F, 0.5F, 0.5F}), nullptr);
    CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
    CHECK(outcome.skipReason == "no_score_variance");
    CHECK(results.front().document.sha256Hash == "a"); // unchanged
}

TEST_CASE("applyCrossRerank skips when score gap below threshold", "[search][rerank][catch2]") {
    auto results = window3();
    auto cfg = blendConfig();
    cfg.rerankScoreGapThreshold = 0.5F; // gap 1.0-0.95 = 0.05 < 0.5
    auto outcome =
        applyCrossRerank(results, "q", cfg, 3, fixedScorer({0.90F, 0.95F, 1.0F}), nullptr);
    CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
    CHECK(outcome.skipReason == "score_gap_below_threshold");
}

TEST_CASE("applyCrossRerank fails on size mismatch", "[search][rerank][catch2]") {
    auto results = window3();
    auto outcome = applyCrossRerank(results, "q", blendConfig(), 3, fixedScorer({0.1F, 0.2F}),
                                    nullptr); // 2 scores for window 3
    CHECK(outcome.status == CrossRerankOutcome::Status::Failed);
    CHECK(outcome.errorMessage == "score_size_mismatch");
}

TEST_CASE("applyCrossRerank treats NotImplemented as reranker_unavailable skip",
          "[search][rerank][catch2]") {
    auto results = window3();
    auto scorer = [](const std::string&,
                     const std::vector<std::string>&) -> Result<std::vector<float>> {
        return yams::Error{ErrorCode::NotImplemented, "no rerank"};
    };
    auto outcome = applyCrossRerank(results, "q", blendConfig(), 3, scorer, nullptr);
    CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
    CHECK(outcome.skipReason == "reranker_unavailable");
    CHECK(outcome.errorMessage == "no rerank");
    CHECK(outcome.attempted);
}

TEST_CASE("applyCrossRerank skips no_candidates and unavailable before calling",
          "[search][rerank][catch2]") {
    SECTION("empty window") {
        auto results = window3();
        auto outcome =
            applyCrossRerank(results, "q", blendConfig(), 0, fixedScorer({0.1F}), nullptr);
        CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
        CHECK(outcome.skipReason == "no_candidates");
        CHECK_FALSE(outcome.attempted);
    }
    SECTION("null reranker") {
        auto results = window3();
        auto outcome = applyCrossRerank(results, "q", blendConfig(), 3, nullptr, nullptr);
        CHECK(outcome.status == CrossRerankOutcome::Status::Skipped);
        CHECK(outcome.skipReason == "reranker_unavailable");
        CHECK_FALSE(outcome.attempted);
    }
}

namespace {

class PreviewCountingRepository : public yams::metadata::MetadataRepository {
public:
    explicit PreviewCountingRepository(yams::metadata::ConnectionPool& pool)
        : MetadataRepository(pool) {}

    Result<std::optional<yams::metadata::DocumentContent>> getContent(int64_t documentId) override {
        ++fullContentCalls;
        return MetadataRepository::getContent(documentId);
    }

    Result<std::unordered_map<int64_t, std::string>>
    batchGetContentPreview(const std::vector<int64_t>& documentIds, int maxChars,
                           int maxDocs = 0) override {
        ++previewBatchCalls;
        if (failPreview)
            return yams::Error{ErrorCode::InternalError, "injected preview failure"};
        return MetadataRepository::batchGetContentPreview(documentIds, maxChars, maxDocs);
    }

    bool failPreview = false;
    std::atomic<int> fullContentCalls{0};
    std::atomic<int> previewBatchCalls{0};
};

} // namespace

TEST_CASE("applyCrossRerank fetches window text as one preview batch", "[search][rerank][catch2]") {
    namespace fs = std::filesystem;
    const auto dir = fs::temp_directory_path() /
                     ("yams_rerank_preview_" + std::to_string(std::random_device{}()));
    fs::create_directories(dir);
    const auto dbPath = (dir / "meta.db").string();
    {
        yams::metadata::Database db;
        REQUIRE(db.open(dbPath, yams::metadata::ConnectionMode::Create));
        yams::metadata::MigrationManager mm(db);
        REQUIRE(mm.initialize());
        mm.registerMigrations(yams::metadata::YamsMetadataMigrations::getAllMigrations());
        REQUIRE(mm.migrate());
        db.close();
    }
    yams::metadata::ConnectionPoolConfig poolCfg;
    poolCfg.minConnections = 1;
    poolCfg.maxConnections = 1;
    auto pool = std::make_shared<yams::metadata::ConnectionPool>(dbPath, poolCfg);
    REQUIRE(pool->initialize().has_value());
    auto repo = std::make_shared<PreviewCountingRepository>(*pool);

    std::vector<SearchResult> results;
    for (int i = 0; i < 3; ++i) {
        yams::metadata::DocumentInfo info;
        info.filePath = "/tmp/rerank-" + std::to_string(i) + ".txt";
        info.fileName = "rerank-" + std::to_string(i) + ".txt";
        info.fileExtension = ".txt";
        info.fileSize = 64;
        info.sha256Hash = "rerank-hash-" + std::to_string(i);
        info.mimeType = "text/plain";
        info.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        info.modifiedTime = info.createdTime;
        info.indexedTime = info.createdTime;
        auto id = repo->insertDocument(info);
        REQUIRE(id.has_value());
        yams::metadata::DocumentContent content;
        content.documentId = id.value();
        content.contentText = std::string(2000, 'a' + i);
        content.contentLength = 2000;
        content.extractionMethod = "test";
        content.language = "en";
        REQUIRE(repo->insertContent(content).has_value());
        SearchResult r = makeResult(info.sha256Hash, 0.9F - 0.2F * static_cast<float>(i));
        r.document.id = id.value();
        results.push_back(r);
    }

    auto cfg = blendConfig();
    cfg.rerankSnippetMaxChars = 256;
    std::vector<std::string> seenTexts;
    auto scorer =
        [&seenTexts](const std::string&,
                     const std::vector<std::string>& texts) -> Result<std::vector<float>> {
        seenTexts = texts;
        return std::vector<float>{0.1F, 0.5F, 0.9F};
    };
    auto outcome = applyCrossRerank(results, "q", cfg, 3, scorer, repo);
    CHECK(outcome.attempted);
    REQUIRE(seenTexts.size() == 3);
    for (const auto& text : seenTexts) {
        CHECK(text.size() <= 256);
        const bool hasBody = text.find(std::string(16, 'a')) != std::string::npos ||
                             text.find(std::string(16, 'b')) != std::string::npos ||
                             text.find(std::string(16, 'c')) != std::string::npos;
        CHECK(hasBody);
    }
    // One bounded preview query for the window instead of a full-content read per result.
    CHECK(repo->previewBatchCalls.load() == 1);
    CHECK(repo->fullContentCalls.load() == 0);

    {
        int previousLimit = 0;
        REQUIRE(pool->withConnection([&](yams::metadata::Database& db) -> Result<void> {
            previousLimit = sqlite3_limit(db.rawHandle(), SQLITE_LIMIT_VARIABLE_NUMBER, 3);
            return {};
        }));
        struct RestoreLimit {
            yams::metadata::ConnectionPool& pool;
            int limit;
            ~RestoreLimit() {
                (void)pool.withConnection([&](yams::metadata::Database& db) -> Result<void> {
                    sqlite3_limit(db.rawHandle(), SQLITE_LIMIT_VARIABLE_NUMBER, limit);
                    return {};
                });
            }
        } restore{*pool, previousLimit};
        seenTexts.clear();
        const auto beforeLimitFailure = results;
        auto limited = applyCrossRerank(results, "q", cfg, 3, scorer, repo);
        CHECK_FALSE(limited.attempted);
        CHECK(limited.status == CrossRerankOutcome::Status::Failed);
        CHECK_FALSE(limited.errorMessage.empty());
        CHECK(seenTexts.empty());
        for (size_t i = 0; i < results.size(); ++i) {
            CHECK(results[i].document.sha256Hash == beforeLimitFailure[i].document.sha256Hash);
            CHECK(results[i].score == beforeLimitFailure[i].score);
        }
    }

    repo->failPreview = true;
    seenTexts.clear();
    const auto beforeFailure = results;
    auto failed = applyCrossRerank(results, "q", cfg, 3, scorer, repo);
    CHECK_FALSE(failed.attempted);
    CHECK(failed.status == CrossRerankOutcome::Status::Failed);
    CHECK(failed.errorMessage == "injected preview failure");
    CHECK(seenTexts.empty());
    REQUIRE(results.size() == beforeFailure.size());
    for (size_t i = 0; i < results.size(); ++i) {
        CHECK(results[i].document.sha256Hash == beforeFailure[i].document.sha256Hash);
        CHECK(results[i].score == beforeFailure[i].score);
    }

    repo.reset();
    pool->shutdown();
    pool.reset();
    std::error_code ec;
    fs::remove_all(dir, ec);
}
