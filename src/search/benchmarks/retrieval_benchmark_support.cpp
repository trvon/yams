#include "retrieval_benchmark_support.h"

#include <yams/common/fs_utils.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <functional>
#include <set>
#include <sstream>
#include <unordered_set>

#include <spdlog/spdlog.h>

namespace fs = std::filesystem;

namespace yams::bench {

namespace {

double computeDCG(const std::vector<int>& grades, int k) {
    double dcg = 0.0;
    for (int i = 0; i < std::min(k, static_cast<int>(grades.size())); ++i) {
        dcg += (std::pow(2.0, grades[i]) - 1.0) / std::log2(i + 2.0);
    }
    return dcg;
}

double computeIDCG(std::vector<int> grades, int k) {
    std::sort(grades.begin(), grades.end(), std::greater<int>());
    return computeDCG(grades, k);
}

std::vector<int> collectAllGrades(const TestQuery& query) {
    std::vector<int> allGrades;
    allGrades.reserve(query.relevanceGrades.size());
    for (const auto& [_, grade] : query.relevanceGrades) {
        allGrades.push_back(grade);
    }
    return allGrades;
}

void writeBenchmarkDocument(const fs::path& root, const std::string& filename,
                            const std::string& content) {
    std::ofstream(root / filename) << content;
}

Result<std::string> readBenchmarkTextFile(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return Error{ErrorCode::NotFound, "failed to open benchmark document: " + path.string()};
    }
    std::ostringstream buffer;
    buffer << in.rdbuf();
    return buffer.str();
}

struct HardLexicalDocSpec {
    const char* filename;
    const char* content;
};

const std::vector<HardLexicalDocSpec>& hardLexicalDocs() {
    static const std::vector<HardLexicalDocSpec> kDocs = {
        {"auth_password_reset_primary.txt",
         "Incident memo\n\npassword reset token email recovery failed after mailbox delay. "
         "The password reset token email recovery flow depends on one-time token rotation and "
         "mailbox confirmation.\n"},
        {"auth_password_reset_secondary.txt",
         "Runbook\n\nEmail password recovery uses reset token validation, resend controls, "
         "and account confirmation checkpoints.\n"},
        {"auth_session_revocation_distractor.txt",
         "Session note\n\nSession token revocation after device logout uses token expiry, "
         "account audit, and recovery fallback checks.\n"},
        {"storage_cache_invalidation_primary.txt",
         "Design review\n\ncache invalidation stale reads eviction must clear shard-local "
         "entries before background refresh. The cache invalidation stale reads eviction "
         "sequence prevents stale session snapshots.\n"},
        {"storage_cache_invalidation_secondary.txt",
         "Operations note\n\nStale read troubleshooting requires cache eviction ordering, "
         "refresh backpressure, and invalidation confirmation.\n"},
        {"storage_replication_distractor.txt",
         "Replication memo\n\nShard replication lag, eviction pressure, and snapshot transfer "
         "tuning affect stale follower reads.\n"},
        {"network_retry_budget_primary.txt",
         "Tuning guide\n\nretry budget backoff timeout circuit breaker tuning protects "
         "degraded upstreams. A retry budget backoff timeout circuit breaker should decay on "
         "persistent 5xx bursts.\n"},
        {"network_retry_budget_secondary.txt",
         "Playbook\n\nTimeout backoff tuning and retry budget accounting keep client retry "
         "storms bounded before breaker open.\n"},
        {"network_tls_handshake_distractor.txt",
         "TLS debug\n\nHandshake timeout, certificate retry, and connection warmup issues can "
         "look like circuit breaker incidents.\n"},
        {"database_schema_change_primary.txt",
         "Migration memo\n\nonline schema change lock timeout rollback planning needs lock "
         "sampling, replica lag checks, and rollback windows. The online schema change lock "
         "timeout rollback checklist gates cutover.\n"},
        {"database_schema_change_secondary.txt",
         "DBA checklist\n\nSchema migration lock timeout recovery covers blocker analysis, "
         "rollback rehearsal, and replica catch-up.\n"},
        {"database_replica_distractor.txt",
         "Replica note\n\nReplica rollback after failover, delayed apply, and lock-free read "
         "routing are separate from schema cutover planning.\n"},
    };
    return kDocs;
}

std::vector<TestQuery> hardLexicalQueriesForFiles(const std::vector<std::string>& createdFiles,
                                                  int numQueries) {
    const std::set<std::string> available(createdFiles.begin(), createdFiles.end());
    struct QuerySpec {
        const char* query;
        std::vector<std::pair<const char*, int>> relevant;
    };

    const std::vector<QuerySpec> specs = {
        {"password reset token email recovery",
         {{"auth_password_reset_primary.txt", 3},
          {"auth_password_reset_secondary.txt", 2},
          {"auth_session_revocation_distractor.txt", 1}}},
        {"cache invalidation stale reads eviction",
         {{"storage_cache_invalidation_primary.txt", 3},
          {"storage_cache_invalidation_secondary.txt", 2},
          {"storage_replication_distractor.txt", 1}}},
        {"retry budget backoff timeout circuit breaker",
         {{"network_retry_budget_primary.txt", 3},
          {"network_retry_budget_secondary.txt", 2},
          {"network_tls_handshake_distractor.txt", 1}}},
        {"online schema change lock timeout rollback",
         {{"database_schema_change_primary.txt", 3},
          {"database_schema_change_secondary.txt", 2},
          {"database_replica_distractor.txt", 1}}},
    };

    std::vector<TestQuery> queries;
    for (const auto& spec : specs) {
        TestQuery query;
        query.query = spec.query;
        for (const auto& [filename, grade] : spec.relevant) {
            if (available.count(filename) == 0) {
                continue;
            }
            query.relevantFiles.insert(filename);
            query.relevanceGrades[filename] = grade;
        }
        if (!query.relevantFiles.empty()) {
            queries.push_back(std::move(query));
        }
    }

    if (queries.empty()) {
        return queries;
    }
    if (numQueries <= 0) {
        return {queries.front()};
    }
    if (static_cast<int>(queries.size()) <= numQueries) {
        return queries;
    }
    queries.resize(static_cast<std::size_t>(numQueries));
    return queries;
}

Result<BEIRDocument> parseManifestDocument(const nlohmann::json& item,
                                           const fs::path& manifestRoot) {
    BEIRDocument doc;
    doc.id = item.value("id", item.value("_id", std::string{}));
    doc.title = item.value("title", std::string{});
    if (doc.id.empty()) {
        return Error{ErrorCode::InvalidArgument, "benchmark manifest document missing id"};
    }

    if (item.contains("text") && item["text"].is_string()) {
        doc.text = item["text"].get<std::string>();
        return doc;
    }
    if (item.contains("content") && item["content"].is_string()) {
        doc.text = item["content"].get<std::string>();
        return doc;
    }
    if (item.contains("path") && item["path"].is_string()) {
        fs::path sourcePath = manifestRoot / item["path"].get<std::string>();
        auto text = readBenchmarkTextFile(sourcePath);
        if (!text) {
            return text.error();
        }
        doc.text = std::move(text.value());
        return doc;
    }

    return Error{ErrorCode::InvalidArgument,
                 "benchmark manifest document '" + doc.id + "' must provide text/content/path"};
}

} // namespace

CorpusGenerator::CorpusGenerator(const fs::path& dir, SyntheticCorpusMode corpusMode)
    : corpusDir(dir), mode(corpusMode) {
    yams::common::ensureDirectories(corpusDir);
}

void CorpusGenerator::generateDocuments(int count) {
    createdFiles.clear();

    if (mode == SyntheticCorpusMode::CommunityGraph) {
        struct CommunityDocSpec {
            const char* filename;
            const char* content;
        };

        static constexpr CommunityDocSpec kCommunityDocs[] = {
            {"community_target.txt", "Target note\n\nalpha rival target evidence\n"},
            {"community_partner.txt", "Partner note\n\nalpha companion evidence\n"},
            {"community_rival.txt", "Rival alpha note\n\nalpha rival alpha rival evidence\n"},
        };

        for (const auto& doc : kCommunityDocs) {
            writeBenchmarkDocument(corpusDir, doc.filename, doc.content);
            createdFiles.push_back(doc.filename);
        }
        return;
    }

    if (mode == SyntheticCorpusMode::HardLexical) {
        const auto& docs = hardLexicalDocs();
        const int docCount = std::clamp(count, 1, static_cast<int>(docs.size()));
        for (int i = 0; i < docCount; ++i) {
            writeBenchmarkDocument(corpusDir, docs[static_cast<std::size_t>(i)].filename,
                                   docs[static_cast<std::size_t>(i)].content);
            createdFiles.push_back(docs[static_cast<std::size_t>(i)].filename);
        }
        return;
    }

    std::uniform_int_distribution<int> topicDist(0, static_cast<int>(topics.size()) - 1);
    std::uniform_int_distribution<int> termDist(0, static_cast<int>(terms.size()) - 1);
    for (int i = 0; i < count; ++i) {
        std::string topicName = topics[topicDist(rng)];
        std::string content = "This document covers " + topicName + " functionality.\nThe " +
                              topicName + " system handles ";
        for (int t = 0; t < 10; ++t) {
            content += terms[termDist(rng)] + " ";
        }
        content += "\nImplementation of " + topicName + " requires careful design.\n";
        std::string filename = topicName + "_" + std::to_string(i) + ".txt";
        writeBenchmarkDocument(corpusDir, filename, content);
        createdFiles.push_back(filename);
    }
}

std::vector<TestQuery> CorpusGenerator::generateQueries(int numQueries) {
    if (mode == SyntheticCorpusMode::CommunityGraph) {
        std::vector<TestQuery> queries;
        const int repeatedQueries = std::max(1, numQueries);
        for (int i = 0; i < repeatedQueries; ++i) {
            TestQuery query;
            query.query = "alpha rival";
            query.useDocIds = true;
            query.relevantDocIds.insert("community_target");
            query.relevanceGrades["community_target"] = 3;
            queries.push_back(std::move(query));
        }
        return queries;
    }

    if (mode == SyntheticCorpusMode::HardLexical) {
        return hardLexicalQueriesForFiles(createdFiles, numQueries);
    }

    std::vector<TestQuery> queries;
    std::uniform_int_distribution<int> topicDist(0, static_cast<int>(topics.size()) - 1);
    for (int q = 0; q < numQueries; ++q) {
        std::string topic = topics[topicDist(rng)];
        TestQuery query;
        query.query = topic + " system";
        for (const auto& filename : createdFiles) {
            if (filename.find(topic) != std::string::npos) {
                query.relevantFiles.insert(filename);
                query.relevanceGrades[filename] = (filename.find(topic) == 0) ? 3 : 2;
            }
        }
        if (!query.relevantFiles.empty()) {
            queries.push_back(std::move(query));
        }
    }
    return queries;
}

BEIRCorpusLoader::BEIRCorpusLoader(const BEIRDataset& ds, const fs::path& dir)
    : dataset(ds), corpusDir(dir) {}

void BEIRCorpusLoader::writeDocumentsAsFiles() {
    yams::common::ensureDirectories(corpusDir);
    for (const auto& [id, doc] : dataset.documents) {
        fs::path filePath = corpusDir / (id + ".txt");
        std::ofstream outFile(filePath);
        if (doc.title.empty()) {
            outFile << doc.text;
        } else {
            outFile << doc.title << "\n\n" << doc.text;
        }
        outFile.close();
        docIdToHash[id] = filePath.string();
    }
    spdlog::info("Wrote {} documents to {}", dataset.documents.size(), corpusDir.string());
}

std::vector<TestQuery> BEIRCorpusLoader::generateTestQueries() {
    std::vector<TestQuery> testQueries;
    for (const auto& [queryId, queryText] : dataset.queries) {
        TestQuery query;
        query.query = queryText.text;
        query.useDocIds = true;

        auto range = dataset.qrels.equal_range(queryId);
        for (auto it = range.first; it != range.second; ++it) {
            const auto& [docId, score] = it->second;
            query.relevantDocIds.insert(docId);
            query.relevanceGrades[docId] = score;
        }

        if (!query.relevantDocIds.empty()) {
            testQueries.push_back(std::move(query));
        }
    }
    spdlog::info("Generated {} test queries from BEIR dataset", testQueries.size());
    return testQueries;
}

std::vector<std::string> BEIRCorpusLoader::getDocumentPaths() const {
    std::vector<std::string> paths;
    for (const auto& [id, _] : dataset.documents) {
        paths.push_back((corpusDir / (id + ".txt")).string());
    }
    return paths;
}

Result<BEIRDataset> loadBenchmarkManifestDataset(const std::string& datasetName,
                                                 const fs::path& manifestRoot) {
    fs::path root = manifestRoot;
    if (root.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "benchmark manifest dataset requires YAMS_BENCH_DATASET_PATH"};
    }

    if (fs::is_directory(root) && fs::exists(root / "corpus.jsonl") &&
        fs::exists(root / "queries.jsonl")) {
        return loadBEIRDataset(datasetName, root);
    }

    fs::path manifestPath = root;
    if (fs::is_directory(root)) {
        if (fs::exists(root / "benchmark_manifest.json")) {
            manifestPath = root / "benchmark_manifest.json";
        } else if (fs::exists(root / "manifest.json")) {
            manifestPath = root / "manifest.json";
        } else {
            return Error{ErrorCode::NotFound,
                         "benchmark dataset path must contain corpus.jsonl/queries.jsonl or "
                         "benchmark_manifest.json"};
        }
    }

    std::ifstream in(manifestPath);
    if (!in) {
        return Error{ErrorCode::NotFound,
                     "failed to open benchmark manifest: " + manifestPath.string()};
    }

    nlohmann::json rootJson;
    try {
        in >> rootJson;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidArgument,
                     "failed to parse benchmark manifest: " + std::string(e.what())};
    }

    if (!rootJson.contains("documents") || !rootJson["documents"].is_array()) {
        return Error{ErrorCode::InvalidArgument, "benchmark manifest missing documents array"};
    }
    if (!rootJson.contains("queries") || !rootJson["queries"].is_array()) {
        return Error{ErrorCode::InvalidArgument, "benchmark manifest missing queries array"};
    }
    if (!rootJson.contains("qrels") || !rootJson["qrels"].is_array()) {
        return Error{ErrorCode::InvalidArgument, "benchmark manifest missing qrels array"};
    }

    BEIRDataset dataset;
    dataset.name = rootJson.value("name", datasetName);
    dataset.basePath = manifestPath.parent_path();

    for (const auto& item : rootJson["documents"]) {
        auto docResult = parseManifestDocument(item, dataset.basePath);
        if (!docResult) {
            return docResult.error();
        }
        auto doc = std::move(docResult.value());
        dataset.documents[doc.id] = std::move(doc);
    }

    for (const auto& item : rootJson["queries"]) {
        BEIRQuery query;
        query.id = item.value("id", item.value("_id", std::string{}));
        query.text = item.value("text", item.value("query", std::string{}));
        if (query.id.empty() || query.text.empty()) {
            return Error{ErrorCode::InvalidArgument, "benchmark manifest query missing id or text"};
        }
        dataset.queries[query.id] = std::move(query);
    }

    for (const auto& item : rootJson["qrels"]) {
        const std::string queryId = item.value("query_id", item.value("queryId", std::string{}));
        const std::string docId = item.value("doc_id", item.value("docId", std::string{}));
        const int score = item.value("score", 0);
        if (queryId.empty() || docId.empty() || score <= 0) {
            return Error{ErrorCode::InvalidArgument,
                         "benchmark manifest qrel missing query_id/doc_id/positive score"};
        }
        if (!dataset.documents.contains(docId)) {
            return Error{ErrorCode::InvalidArgument,
                         "benchmark manifest qrel references unknown doc: " + docId};
        }
        if (!dataset.queries.contains(queryId)) {
            return Error{ErrorCode::InvalidArgument,
                         "benchmark manifest qrel references unknown query: " + queryId};
        }
        dataset.qrels.emplace(queryId, std::make_pair(docId, score));
    }

    spdlog::info("Loaded benchmark manifest dataset '{}' with {} docs, {} queries, {} qrels",
                 dataset.name, dataset.documents.size(), dataset.queries.size(),
                 dataset.qrels.size());
    return dataset;
}

std::string canonicalResultDocId(const TestQuery& query, const std::string& fileName) {
    if (!query.useDocIds) {
        return fileName;
    }

    std::string docId = fileName;
    if (docId.size() > 4 && docId.compare(docId.size() - 4, 4, ".txt") == 0) {
        docId.resize(docId.size() - 4);
    }
    return docId;
}

bool isRelevantDocForQuery(const TestQuery& query, const std::string& docId) {
    if (docId.empty()) {
        return false;
    }
    if (query.useDocIds) {
        return query.relevantDocIds.count(docId) > 0;
    }
    return query.relevantFiles.count(docId) > 0 || query.relevantFiles.count(docId + ".txt") > 0;
}

int relevanceGradeForQuery(const TestQuery& query, const std::string& docId) {
    if (auto it = query.relevanceGrades.find(docId); it != query.relevanceGrades.end()) {
        return it->second;
    }
    if (!query.useDocIds) {
        if (auto it = query.relevanceGrades.find(docId + ".txt");
            it != query.relevanceGrades.end()) {
            return it->second;
        }
    }
    return 0;
}

QueryScoreSample scoreRankedDocIds(const TestQuery& query,
                                   const std::vector<std::string>& rankedDocIds, int k) {
    QueryScoreSample sample;
    const size_t metricK = static_cast<size_t>(std::max(0, k));
    const size_t topK = std::min(metricK, rankedDocIds.size());
    sample.retrievedGrades.reserve(topK);

    std::unordered_set<std::string> seenDocIds;
    seenDocIds.reserve(topK);

    double averagePrecision = 0.0;
    for (size_t i = 0; i < topK; ++i) {
        const auto& docId = rankedDocIds[i];
        const bool isDuplicate = !seenDocIds.insert(docId).second;
        const bool isRelevant = !isDuplicate && isRelevantDocForQuery(query, docId);
        if (isDuplicate) {
            sample.hadDuplicateInTopK = true;
            sample.duplicateDocIds.push_back(docId);
        }
        if (isRelevant) {
            sample.numRelevantInTopK++;
            if (sample.firstRelevantRank < 0) {
                sample.firstRelevantRank = static_cast<int>(i + 1);
            }
            sample.numRelevantSeen++;
            averagePrecision +=
                static_cast<double>(sample.numRelevantSeen) / static_cast<double>(i + 1);
        }
        sample.retrievedGrades.push_back(isDuplicate ? 0 : relevanceGradeForQuery(query, docId));
    }

    if (sample.firstRelevantRank > 0) {
        sample.reciprocalRank = 1.0 / static_cast<double>(sample.firstRelevantRank);
    }

    const auto relevantTotal =
        query.useDocIds ? query.relevantDocIds.size() : query.relevantFiles.size();
    if (relevantTotal > 0) {
        sample.recallAtK =
            static_cast<double>(sample.numRelevantInTopK) / static_cast<double>(relevantTotal);
    }
    if (topK > 0) {
        sample.precisionAtK =
            static_cast<double>(sample.numRelevantInTopK) / static_cast<double>(topK);
    }
    if (sample.numRelevantSeen > 0) {
        sample.averagePrecision = averagePrecision / static_cast<double>(sample.numRelevantSeen);
    }

    const auto allGrades = collectAllGrades(query);
    const double dcg = computeDCG(sample.retrievedGrades, k);
    const double idcg = computeIDCG(allGrades, k);
    sample.ndcgAtK = (idcg > 0.0) ? dcg / idcg : 0.0;

    return sample;
}

void addStageMetricSample(StageMetricAccumulator& acc, const std::vector<std::string>& rankedDocIds,
                          const TestQuery& query, int k) {
    const auto sample = scoreRankedDocIds(query, rankedDocIds, k);
    acc.queryCount++;
    acc.totalMRR += sample.reciprocalRank;
    acc.totalRecall += sample.recallAtK;
    acc.totalPrecision += sample.precisionAtK;
    acc.totalMAP += sample.averagePrecision;
    acc.totalNDCG += sample.ndcgAtK;
}

RetrievalMetrics finalizeStageMetrics(const StageMetricAccumulator& acc) {
    RetrievalMetrics metrics;
    metrics.numQueries = static_cast<int>(acc.queryCount);
    if (acc.queryCount == 0) {
        return metrics;
    }

    const double count = static_cast<double>(acc.queryCount);
    metrics.mrr = acc.totalMRR / count;
    metrics.recallAtK = acc.totalRecall / count;
    metrics.precisionAtK = acc.totalPrecision / count;
    metrics.ndcgAtK = acc.totalNDCG / count;
    metrics.map = acc.totalMAP / count;
    return metrics;
}

nlohmann::json
stageRetrievalMetricsToJson(const std::map<std::string, StageMetricAccumulator>& stageMetrics) {
    nlohmann::json out = nlohmann::json::object();
    if (stageMetrics.empty()) {
        return out;
    }

    std::optional<RetrievalMetrics> finalMetrics;
    if (auto finalIt = stageMetrics.find("final"); finalIt != stageMetrics.end()) {
        finalMetrics = finalizeStageMetrics(finalIt->second);
    }

    for (const auto& [stage, acc] : stageMetrics) {
        const auto metrics = finalizeStageMetrics(acc);
        nlohmann::json item = {
            {"num_queries", metrics.numQueries}, {"mrr", metrics.mrr},
            {"recall_at_k", metrics.recallAtK},  {"precision_at_k", metrics.precisionAtK},
            {"ndcg_at_k", metrics.ndcgAtK},      {"map", metrics.map},
        };
        if (finalMetrics.has_value()) {
            item["mrr_delta_vs_final"] = metrics.mrr - finalMetrics->mrr;
            item["recall_delta_vs_final"] = metrics.recallAtK - finalMetrics->recallAtK;
            item["ndcg_delta_vs_final"] = metrics.ndcgAtK - finalMetrics->ndcgAtK;
        }
        out[stage] = std::move(item);
    }

    return out;
}

} // namespace yams::bench
