#pragma once

#include "tests/benchmarks/beir_loader.h"

#include <cstddef>
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

struct LexicalTopologyDocument {
    std::string documentHash;
    std::string text;
};

struct LexicalTopologyNeighbor {
    std::string sourceHash;
    std::string targetHash;
    float score{0.0F};
};

struct GraphTopologyFeature {
    std::int64_t nodeId = 0;
    float weight = 0.0F;
};

struct GraphTopologyDocument {
    std::string documentHash;
    std::vector<GraphTopologyFeature> relatedNodes;
};

struct KeyphraseTopologyDocument {
    std::string documentHash;
    std::string text;
};

enum class BenchmarkTopologySignalKind {
    LexicalCohesion,
    LexicalDisruption,
    Keyphrase,
    Entity,
    Ontology,
    Structure,
    Embedding,
};

std::string topologySignalKindName(BenchmarkTopologySignalKind kind);

struct BenchmarkTopologyGateConfig {
    float minSignalWeight = 1.0F;
    std::size_t maxSignalDocumentFrequency = 64;
    float minNeighborScore = 0.0F;
    std::size_t maxComponentDocs = 64;
};

struct BenchmarkTopologySignal {
    std::string documentHash;
    std::string featureId;
    BenchmarkTopologySignalKind kind = BenchmarkTopologySignalKind::LexicalCohesion;
    float weight = 0.0F;
    std::size_t documentFrequency = 0;
};

struct BenchmarkTopologyNeighborObservation {
    std::string sourceHash;
    std::string targetHash;
    std::string sourceSegmentId;
    std::string targetSegmentId;
    float score = 0.0F;
};

struct BenchmarkTopologyEdgeWitness {
    std::string sourceHash;
    std::string targetHash;
    std::string sourceSegmentId;
    std::string targetSegmentId;
    float score = 0.0F;
    std::string featureId;
    BenchmarkTopologySignalKind kind = BenchmarkTopologySignalKind::LexicalCohesion;
    float sourceWeight = 0.0F;
    float targetWeight = 0.0F;
    std::size_t documentFrequency = 0;
    bool reciprocal = false;
    bool signalGatePassed = false;
};

struct BenchmarkTopologyComponentSummary {
    std::string rootHash;
    std::vector<std::string> memberHashes;
};

struct BenchmarkTopologyConstructionCertificate {
    std::string sourceName;
    BenchmarkTopologyGateConfig gates;
    std::vector<BenchmarkTopologySignal> signals;
    std::vector<BenchmarkTopologyNeighborObservation> observations;
    std::vector<BenchmarkTopologyEdgeWitness> edgeWitnesses;
    std::vector<BenchmarkTopologyComponentSummary> components;
};

struct BenchmarkTopologyValidationResult {
    bool ok = true;
    std::vector<std::string> errors;
};

BenchmarkTopologyValidationResult validateTopologyConstructionCertificate(
    const BenchmarkTopologyConstructionCertificate& certificate);
nlohmann::json
topologyConstructionCertificateToJson(const BenchmarkTopologyConstructionCertificate& certificate,
                                      const BenchmarkTopologyValidationResult& validation = {});

std::vector<LexicalTopologyNeighbor>
buildLexicalTopologyNeighbors(const std::vector<LexicalTopologyDocument>& documents,
                              std::size_t topK, float minScore, bool reciprocalOnly = true,
                              BenchmarkTopologyConstructionCertificate* certificate = nullptr);

std::vector<LexicalTopologyNeighbor>
buildGraphTopologyNeighbors(const std::vector<GraphTopologyDocument>& documents, std::size_t topK,
                            float minScore, bool reciprocalOnly = true,
                            BenchmarkTopologyConstructionCertificate* certificate = nullptr);

std::vector<LexicalTopologyNeighbor> buildKeyphraseTopologyNeighbors(
    const std::vector<KeyphraseTopologyDocument>& documents, std::size_t topK, float minScore,
    std::size_t maxFeaturesPerDocument = 16, std::size_t maxFeatureDocumentFrequency = 64,
    bool reciprocalOnly = true, BenchmarkTopologyConstructionCertificate* certificate = nullptr);

std::vector<LexicalTopologyNeighbor> buildSegmentKeyphraseTopologyNeighbors(
    const std::vector<KeyphraseTopologyDocument>& documents, std::size_t topK, float minScore,
    std::size_t segmentTokenBudget = 192, std::size_t segmentTokenOverlap = 32,
    std::size_t maxFeaturesPerSegment = 12, std::size_t maxFeatureDocumentFrequency = 64,
    std::size_t maxComponentDocs = 64, std::size_t maxEdgesPerFeature = 64,
    bool reciprocalOnly = true, BenchmarkTopologyConstructionCertificate* certificate = nullptr);

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
