#pragma once

#include "tests/benchmarks/beir_loader.h"

#include <cstdint>
#include <filesystem>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace yams::bench {

struct TestQuery {
    std::string query;
    std::set<std::string> relevantFiles;
    std::set<std::string> relevantDocIds;
    std::map<std::string, int> relevanceGrades;
    bool useDocIds = false;
};

enum class SyntheticCorpusMode { Generic, CommunityGraph, HardLexical };

struct CorpusGenerator {
    std::vector<std::string> topics = {"authentication", "database", "network", "parsing",
                                       "encryption",     "testing",  "logging", "storage"};
    std::vector<std::string> terms = {"user",       "password", "token",    "session",
                                      "connection", "request",  "response", "cache",
                                      "error",      "file",     "data"};
    std::filesystem::path corpusDir;
    std::vector<std::string> createdFiles;
    std::mt19937 rng{42};
    SyntheticCorpusMode mode = SyntheticCorpusMode::Generic;

    explicit CorpusGenerator(const std::filesystem::path& dir,
                             SyntheticCorpusMode corpusMode = SyntheticCorpusMode::Generic);

    void generateDocuments(int count);
    std::vector<TestQuery> generateQueries(int numQueries);
};

struct BEIRCorpusLoader {
    BEIRDataset dataset;
    std::filesystem::path corpusDir;
    std::map<std::string, std::string> docIdToHash;

    BEIRCorpusLoader(const BEIRDataset& ds, const std::filesystem::path& dir);

    void writeDocumentsAsFiles();
    std::vector<TestQuery> generateTestQueries();
    std::vector<std::string> getDocumentPaths() const;
};

yams::Result<BEIRDataset> loadBenchmarkManifestDataset(const std::string& datasetName,
                                                       const std::filesystem::path& manifestRoot);

struct RetrievalMetrics {
    double mrr = 0.0;
    double recallAtK = 0.0;
    double precisionAtK = 0.0;
    double ndcgAtK = 0.0;
    double map = 0.0;
    double duplicateRateAtK = 0.0;
    int numQueries = 0;
};

struct StageMetricAccumulator {
    double totalMRR = 0.0;
    double totalRecall = 0.0;
    double totalPrecision = 0.0;
    double totalNDCG = 0.0;
    double totalMAP = 0.0;
    std::uint64_t queryCount = 0;
};

struct QueryScoreSample {
    int firstRelevantRank = -1;
    int numRelevantInTopK = 0;
    int numRelevantSeen = 0;
    bool hadDuplicateInTopK = false;
    double reciprocalRank = 0.0;
    double recallAtK = 0.0;
    double precisionAtK = 0.0;
    double averagePrecision = 0.0;
    double ndcgAtK = 0.0;
    std::vector<int> retrievedGrades;
    std::vector<std::string> duplicateDocIds;
};

std::string canonicalResultDocId(const TestQuery& query, const std::string& fileName);
bool isRelevantDocForQuery(const TestQuery& query, const std::string& docId);
int relevanceGradeForQuery(const TestQuery& query, const std::string& docId);

QueryScoreSample scoreRankedDocIds(const TestQuery& query,
                                   const std::vector<std::string>& rankedDocIds, int k);
void addStageMetricSample(StageMetricAccumulator& acc, const std::vector<std::string>& rankedDocIds,
                          const TestQuery& query, int k);
RetrievalMetrics finalizeStageMetrics(const StageMetricAccumulator& acc);
nlohmann::json
stageRetrievalMetricsToJson(const std::map<std::string, StageMetricAccumulator>& stageMetrics);

} // namespace yams::bench
