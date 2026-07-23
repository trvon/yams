#include "retrieval_benchmark_support.h"

#include <yams/common/fs_utils.h>

#include <simeon/bm25.hpp>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <functional>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#if defined(__APPLE__) || defined(__GLIBC__)
#include <sys/resource.h>
#endif

#include <spdlog/spdlog.h>

namespace fs = std::filesystem;

namespace yams::bench {

bool benchmarkSearchIndexReusable(yams::vector::VectorSearchEngine engine,
                                  bool backendPersistedReusable) {
    switch (engine) {
        case yams::vector::VectorSearchEngine::Vec0L2:
            return false;
        case yams::vector::VectorSearchEngine::SimeonPqAdc:
            return backendPersistedReusable;
        case yams::vector::VectorSearchEngine::ExactScan:
            return false;
    }
    return false;
}

bool benchmarkSearchIndexQueryReady(yams::vector::VectorSearchEngine engine,
                                    bool persistedIndexReusable, bool preparedThisRun) {
    if (engine == yams::vector::VectorSearchEngine::ExactScan) {
        return true;
    }
    return persistedIndexReusable || preparedThisRun;
}

void writeTopologyDisabledEmbeddingSelection(std::ostream& out, bool topologyDisabled) {
    if (!topologyDisabled) {
        return;
    }
    out << "\n[embeddings.selection]\n";
    out << "update_semantic_graph_during_ingest = false\n\n";
}

std::optional<double> processPeakRssMb() noexcept {
#if defined(__APPLE__) || defined(__GLIBC__)
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) != 0 || usage.ru_maxrss <= 0) {
        return std::nullopt;
    }
#if defined(__APPLE__)
    constexpr double kBytesPerMiB = 1024.0 * 1024.0;
    return static_cast<double>(usage.ru_maxrss) / kBytesPerMiB;
#else
    constexpr double kKiBPerMiB = 1024.0;
    return static_cast<double>(usage.ru_maxrss) / kKiBPerMiB;
#endif
#else
    return std::nullopt;
#endif
}

std::string topologySignalKindName(BenchmarkTopologySignalKind kind) {
    switch (kind) {
        case BenchmarkTopologySignalKind::LexicalCohesion:
            return "lexical_cohesion";
        case BenchmarkTopologySignalKind::LexicalDisruption:
            return "lexical_disruption";
        case BenchmarkTopologySignalKind::Keyphrase:
            return "keyphrase";
        case BenchmarkTopologySignalKind::Entity:
            return "entity";
        case BenchmarkTopologySignalKind::Ontology:
            return "ontology";
        case BenchmarkTopologySignalKind::Structure:
            return "structure";
        case BenchmarkTopologySignalKind::Embedding:
            return "embedding";
    }
    return "unknown";
}

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

std::vector<std::string> tokenizeTopologyText(const std::string& text) {
    std::vector<std::string> tokens;
    std::string current;
    for (unsigned char ch : text) {
        if (std::isalnum(ch) != 0) {
            current.push_back(static_cast<char>(std::tolower(ch)));
            continue;
        }
        if (current.size() >= 3) {
            tokens.push_back(current);
        }
        current.clear();
    }
    if (current.size() >= 3) {
        tokens.push_back(current);
    }
    return tokens;
}

using FeatureWeights = std::unordered_map<std::string, float>;

bool isTopologyStopword(const std::string& token) {
    static const std::unordered_set<std::string> kStopwords = {
        "about", "after", "also",  "and",   "are",  "before", "between", "but",   "can",
        "for",   "from",  "has",   "have",  "into", "not",    "the",     "their", "this",
        "that",  "then",  "there", "these", "they", "with",   "within",  "will",  "your",
    };
    return kStopwords.contains(token);
}

std::vector<FeatureWeights>
buildLexicalFeatureWeights(const std::vector<LexicalTopologyDocument>& documents) {
    std::vector<FeatureWeights> byDoc(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        const auto tokens = tokenizeTopologyText(documents[i].text);
        auto& features = byDoc[i];
        for (const auto& token : tokens) {
            features["term:" + token] += 1.0F;
        }
        for (std::size_t j = 1; j < tokens.size(); ++j) {
            features["bigram:" + tokens[j - 1] + "_" + tokens[j]] += 1.0F;
        }
    }
    return byDoc;
}

std::unordered_map<std::string, std::size_t>
computeDocumentFrequency(const std::vector<FeatureWeights>& featuresByDoc);

std::vector<FeatureWeights>
buildGraphFeatureWeights(const std::vector<GraphTopologyDocument>& documents) {
    std::vector<FeatureWeights> byDoc(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        auto& features = byDoc[i];
        for (const auto& related : documents[i].relatedNodes) {
            if (related.nodeId <= 0 || related.weight <= 0.0F) {
                continue;
            }
            const std::string feature = "kg:" + std::to_string(related.nodeId);
            auto [it, inserted] = features.emplace(feature, std::min(related.weight, 1.0F));
            if (!inserted) {
                it->second = std::max(it->second, std::min(related.weight, 1.0F));
            }
        }
    }
    return byDoc;
}

std::vector<FeatureWeights>
buildKeyphraseFeatureWeights(const std::vector<KeyphraseTopologyDocument>& documents,
                             std::size_t maxFeaturesPerDocument,
                             std::size_t maxFeatureDocumentFrequency) {
    std::vector<FeatureWeights> rawByDoc(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        const auto tokens = tokenizeTopologyText(documents[i].text);
        auto& features = rawByDoc[i];
        for (const auto& token : tokens) {
            if (isTopologyStopword(token)) {
                continue;
            }
            features["term:" + token] += 1.0F;
        }
        for (std::size_t j = 1; j < tokens.size(); ++j) {
            if (isTopologyStopword(tokens[j - 1]) || isTopologyStopword(tokens[j])) {
                continue;
            }
            features["bigram:" + tokens[j - 1] + "_" + tokens[j]] += 2.0F;
        }
    }

    const auto df = computeDocumentFrequency(rawByDoc);
    const double totalDocs = static_cast<double>(std::max<std::size_t>(documents.size(), 1));
    std::vector<FeatureWeights> out(documents.size());
    for (std::size_t i = 0; i < rawByDoc.size(); ++i) {
        std::vector<std::tuple<std::string, float, std::size_t>> ranked;
        ranked.reserve(rawByDoc[i].size());
        for (const auto& [feature, tf] : rawByDoc[i]) {
            const auto dfIt = df.find(feature);
            const std::size_t featureDf = dfIt == df.end() ? 0U : dfIt->second;
            if (featureDf < 2 || featureDf > maxFeatureDocumentFrequency) {
                continue;
            }
            const double idf =
                std::log((totalDocs + 1.0) / (static_cast<double>(featureDf) + 1.0)) + 1.0;
            ranked.emplace_back(feature, static_cast<float>(static_cast<double>(tf) * idf),
                                featureDf);
        }
        std::sort(ranked.begin(), ranked.end(), [](const auto& left, const auto& right) {
            if (std::get<1>(left) != std::get<1>(right)) {
                return std::get<1>(left) > std::get<1>(right);
            }
            if (std::get<2>(left) != std::get<2>(right)) {
                return std::get<2>(left) < std::get<2>(right);
            }
            return std::get<0>(left) < std::get<0>(right);
        });
        if (ranked.size() > maxFeaturesPerDocument) {
            ranked.resize(maxFeaturesPerDocument);
        }
        float maxWeight = 0.0F;
        for (const auto& item : ranked) {
            maxWeight = std::max(maxWeight, std::get<1>(item));
        }
        if (maxWeight <= 0.0F) {
            continue;
        }
        for (const auto& item : ranked) {
            out[i][std::get<0>(item)] = std::clamp(std::get<1>(item) / maxWeight, 0.0F, 1.0F);
        }
    }
    return out;
}

std::unordered_map<std::string, std::size_t>
computeDocumentFrequency(const std::vector<FeatureWeights>& featuresByDoc) {
    std::unordered_map<std::string, std::size_t> df;
    for (const auto& features : featuresByDoc) {
        for (const auto& [feature, _] : features) {
            ++df[feature];
        }
    }
    return df;
}

void populateCertificateSignals(BenchmarkTopologyConstructionCertificate& cert,
                                const std::vector<std::string>& hashes,
                                const std::vector<FeatureWeights>& featuresByDoc,
                                BenchmarkTopologySignalKind kind) {
    const auto df = computeDocumentFrequency(featuresByDoc);
    for (std::size_t i = 0; i < featuresByDoc.size(); ++i) {
        if (hashes[i].empty()) {
            continue;
        }
        for (const auto& [feature, weight] : featuresByDoc[i]) {
            const auto dfIt = df.find(feature);
            cert.signals.push_back(BenchmarkTopologySignal{
                .documentHash = hashes[i],
                .featureId = feature,
                .kind = kind,
                .weight = weight,
                .documentFrequency = dfIt == df.end() ? 0U : dfIt->second,
            });
        }
    }
}

BenchmarkTopologyEdgeWitness
makeEdgeWitness(const std::vector<std::string>& hashes,
                const std::vector<FeatureWeights>& featuresByDoc,
                const std::unordered_map<std::string, std::size_t>& df,
                BenchmarkTopologySignalKind kind, const BenchmarkTopologyGateConfig& gates,
                const std::vector<std::vector<std::pair<std::size_t, float>>>& topByDoc,
                std::size_t i, std::size_t j, float score) {
    BenchmarkTopologyEdgeWitness witness{
        .sourceHash = hashes[i],
        .targetHash = hashes[j],
        .score = score,
        .kind = kind,
    };

    const auto& left = featuresByDoc[i];
    const auto& right = featuresByDoc[j];
    const auto* smaller = &left;
    const auto* larger = &right;
    if (smaller->size() > larger->size()) {
        std::swap(smaller, larger);
    }

    bool found = false;
    float bestWeight = 0.0F;
    std::size_t bestDf = 0;
    for (const auto& [feature, leftWeightFromSmaller] : *smaller) {
        (void)leftWeightFromSmaller;
        const auto jt = larger->find(feature);
        if (jt == larger->end()) {
            continue;
        }
        const float leftWeight = left.at(feature);
        const float rightWeight = right.at(feature);
        const auto dfIt = df.find(feature);
        const std::size_t featureDf = dfIt == df.end() ? 0U : dfIt->second;
        const bool passes = leftWeight >= gates.minSignalWeight &&
                            rightWeight >= gates.minSignalWeight &&
                            featureDf <= gates.maxSignalDocumentFrequency;
        if (!passes) {
            continue;
        }
        const float combined = leftWeight + rightWeight;
        if (!found || featureDf < bestDf || (featureDf == bestDf && combined > bestWeight) ||
            (featureDf == bestDf && combined == bestWeight && feature < witness.featureId)) {
            found = true;
            bestWeight = combined;
            bestDf = featureDf;
            witness.featureId = feature;
            witness.sourceWeight = leftWeight;
            witness.targetWeight = rightWeight;
            witness.documentFrequency = featureDf;
            witness.signalGatePassed = true;
        }
    }

    const auto& reverse = topByDoc[j];
    witness.reciprocal = std::any_of(reverse.begin(), reverse.end(),
                                     [i](const auto& item) { return item.first == i; });
    return witness;
}

std::vector<BenchmarkTopologyComponentSummary>
buildComponentSummaries(const std::vector<BenchmarkTopologyEdgeWitness>& edges) {
    std::unordered_map<std::string, std::size_t> indexByHash;
    std::vector<std::string> hashes;
    auto intern = [&](const std::string& hash) mutable {
        auto [it, inserted] = indexByHash.emplace(hash, hashes.size());
        if (inserted) {
            hashes.push_back(hash);
        }
        return it->second;
    };

    for (const auto& edge : edges) {
        if (edge.sourceHash.empty() || edge.targetHash.empty()) {
            continue;
        }
        intern(edge.sourceHash);
        intern(edge.targetHash);
    }
    std::vector<std::size_t> parent(hashes.size());
    for (std::size_t i = 0; i < parent.size(); ++i) {
        parent[i] = i;
    }
    auto findRoot = [&](std::size_t start) mutable {
        std::size_t root = start;
        while (parent[root] != root) {
            root = parent[root];
        }
        while (parent[start] != start) {
            const auto next = parent[start];
            parent[start] = root;
            start = next;
        }
        return root;
    };
    auto unite = [&](std::size_t left, std::size_t right) mutable {
        left = findRoot(left);
        right = findRoot(right);
        if (left != right) {
            parent[std::max(left, right)] = std::min(left, right);
        }
    };
    for (const auto& edge : edges) {
        if (edge.sourceHash.empty() || edge.targetHash.empty()) {
            continue;
        }
        unite(indexByHash.at(edge.sourceHash), indexByHash.at(edge.targetHash));
    }

    std::map<std::size_t, std::vector<std::string>> grouped;
    for (std::size_t i = 0; i < hashes.size(); ++i) {
        grouped[findRoot(i)].push_back(hashes[i]);
    }
    std::vector<BenchmarkTopologyComponentSummary> components;
    components.reserve(grouped.size());
    for (auto& [_, members] : grouped) {
        std::sort(members.begin(), members.end());
        components.push_back(BenchmarkTopologyComponentSummary{.rootHash = members.front(),
                                                               .memberHashes = members});
    }
    return components;
}

void finishCertificate(BenchmarkTopologyConstructionCertificate* certificate,
                       const std::string& sourceName, const BenchmarkTopologyGateConfig& gates,
                       const std::vector<BenchmarkTopologySignal>& signals,
                       const std::vector<BenchmarkTopologyNeighborObservation>& observations,
                       const std::vector<BenchmarkTopologyEdgeWitness>& witnesses) {
    if (certificate == nullptr) {
        return;
    }
    certificate->sourceName = sourceName;
    certificate->gates = gates;
    certificate->signals = signals;
    certificate->observations = observations;
    certificate->edgeWitnesses = witnesses;
    certificate->components = buildComponentSummaries(certificate->edgeWitnesses);
}

} // namespace

std::vector<LexicalTopologyNeighbor>
buildLexicalTopologyNeighbors(const std::vector<LexicalTopologyDocument>& documents,
                              std::size_t topK, float minScore, bool reciprocalOnly,
                              BenchmarkTopologyConstructionCertificate* certificate) {
    if (documents.size() < 2 || topK == 0) {
        return {};
    }

    BenchmarkTopologyGateConfig gates;
    gates.minSignalWeight = minScore;
    gates.minNeighborScore = minScore;
    std::vector<std::string> hashes;
    hashes.reserve(documents.size());
    for (const auto& doc : documents) {
        hashes.push_back(doc.documentHash);
    }
    const auto featureWeights = buildLexicalFeatureWeights(documents);
    const auto featureDf = computeDocumentFrequency(featureWeights);
    BenchmarkTopologyConstructionCertificate certScratch;
    populateCertificateSignals(certScratch, hashes, featureWeights,
                               BenchmarkTopologySignalKind::LexicalCohesion);

    simeon::Bm25Config bm25Config;
    bm25Config.variant = simeon::Bm25Variant::Atire;
    bm25Config.build_word_bigrams = true;
    simeon::Bm25Index index{bm25Config};
    index.reserve_docs(documents.size());
    for (const auto& doc : documents) {
        index.add_doc(doc.text);
    }
    index.finalize();

    std::vector<std::vector<std::pair<std::size_t, float>>> topByDoc(documents.size());
    std::vector<float> scores(documents.size(), 0.0F);
    for (std::size_t i = 0; i < documents.size(); ++i) {
        if (documents[i].documentHash.empty() || documents[i].text.empty()) {
            continue;
        }
        std::fill(scores.begin(), scores.end(), 0.0F);
        index.score_sdm(documents[i].text, scores);
        scores[i] = 0.0F;
        const float maxScore = *std::max_element(scores.begin(), scores.end());
        if (maxScore <= 0.0F) {
            continue;
        }

        std::vector<std::pair<std::size_t, float>> scored;
        for (std::size_t j = 0; j < scores.size(); ++j) {
            if (i == j || documents[j].documentHash.empty()) {
                continue;
            }
            const float normalized = std::clamp(scores[j] / maxScore, 0.0F, 1.0F);
            if (normalized >= minScore) {
                scored.emplace_back(j, normalized);
            }
        }
        std::sort(scored.begin(), scored.end(), [&](const auto& left, const auto& right) {
            if (left.second != right.second) {
                return left.second > right.second;
            }
            return documents[left.first].documentHash < documents[right.first].documentHash;
        });
        if (scored.size() > topK) {
            scored.resize(topK);
        }
        topByDoc[i] = scored;
    }

    std::vector<BenchmarkTopologyNeighborObservation> observations;
    for (std::size_t i = 0; i < topByDoc.size(); ++i) {
        for (const auto& [j, score] : topByDoc[i]) {
            observations.push_back(BenchmarkTopologyNeighborObservation{
                .sourceHash = documents[i].documentHash,
                .targetHash = documents[j].documentHash,
                .score = score,
            });
        }
    }

    std::vector<BenchmarkTopologyEdgeWitness> witnesses;
    std::vector<LexicalTopologyNeighbor> out;
    for (std::size_t i = 0; i < topByDoc.size(); ++i) {
        for (const auto& [j, score] : topByDoc[i]) {
            if (reciprocalOnly) {
                const auto& reverse = topByDoc[j];
                const bool hasReverse =
                    std::any_of(reverse.begin(), reverse.end(),
                                [i](const auto& item) { return item.first == i; });
                if (!hasReverse) {
                    continue;
                }
            }
            witnesses.push_back(makeEdgeWitness(hashes, featureWeights, featureDf,
                                                BenchmarkTopologySignalKind::LexicalCohesion, gates,
                                                topByDoc, i, j, score));
            out.push_back(LexicalTopologyNeighbor{.sourceHash = documents[i].documentHash,
                                                  .targetHash = documents[j].documentHash,
                                                  .score = score});
        }
    }
    std::sort(out.begin(), out.end(), [](const auto& left, const auto& right) {
        if (left.sourceHash != right.sourceHash) {
            return left.sourceHash < right.sourceHash;
        }
        return left.targetHash < right.targetHash;
    });
    std::sort(witnesses.begin(), witnesses.end(), [](const auto& left, const auto& right) {
        if (left.sourceHash != right.sourceHash) {
            return left.sourceHash < right.sourceHash;
        }
        return left.targetHash < right.targetHash;
    });
    finishCertificate(certificate, "lexical", gates, certScratch.signals, observations, witnesses);
    return out;
}

std::vector<LexicalTopologyNeighbor>
buildGraphTopologyNeighbors(const std::vector<GraphTopologyDocument>& documents, std::size_t topK,
                            float minScore, bool reciprocalOnly,
                            BenchmarkTopologyConstructionCertificate* certificate) {
    if (documents.size() < 2 || topK == 0) {
        return {};
    }

    BenchmarkTopologyGateConfig gates;
    gates.minSignalWeight = minScore;
    gates.minNeighborScore = minScore;
    std::vector<std::string> hashes;
    hashes.reserve(documents.size());
    for (const auto& doc : documents) {
        hashes.push_back(doc.documentHash);
    }
    const auto signalWeights = buildGraphFeatureWeights(documents);
    const auto featureDf = computeDocumentFrequency(signalWeights);
    BenchmarkTopologyConstructionCertificate certScratch;
    populateCertificateSignals(certScratch, hashes, signalWeights,
                               BenchmarkTopologySignalKind::Entity);

    std::vector<std::unordered_map<std::int64_t, float>> features;
    features.reserve(documents.size());
    std::vector<double> norms(documents.size(), 0.0);
    for (std::size_t i = 0; i < documents.size(); ++i) {
        features.emplace_back();
        auto& byNode = features.back();
        for (const auto& related : documents[i].relatedNodes) {
            if (related.nodeId <= 0 || related.weight <= 0.0F) {
                continue;
            }
            auto [it, inserted] = byNode.emplace(related.nodeId, std::min(related.weight, 1.0F));
            if (!inserted) {
                it->second = std::max(it->second, std::min(related.weight, 1.0F));
            }
        }
        for (const auto& [_, weight] : byNode) {
            norms[i] += static_cast<double>(weight) * static_cast<double>(weight);
        }
    }

    std::vector<std::vector<std::pair<std::size_t, float>>> topByDoc(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        if (documents[i].documentHash.empty() || norms[i] <= 0.0) {
            continue;
        }
        std::vector<std::pair<std::size_t, float>> scored;
        for (std::size_t j = 0; j < documents.size(); ++j) {
            if (i == j || documents[j].documentHash.empty() || norms[j] <= 0.0) {
                continue;
            }
            double dot = 0.0;
            const auto* smaller = &features[i];
            const auto* larger = &features[j];
            if (smaller->size() > larger->size()) {
                std::swap(smaller, larger);
            }
            for (const auto& [nodeId, weight] : *smaller) {
                auto jt = larger->find(nodeId);
                if (jt != larger->end()) {
                    dot += static_cast<double>(weight) * static_cast<double>(jt->second);
                }
            }
            const float score = static_cast<float>(dot / std::sqrt(norms[i] * norms[j]));
            if (score >= minScore) {
                scored.emplace_back(j, score);
            }
        }
        std::sort(scored.begin(), scored.end(), [&](const auto& left, const auto& right) {
            if (left.second != right.second) {
                return left.second > right.second;
            }
            return documents[left.first].documentHash < documents[right.first].documentHash;
        });
        if (scored.size() > topK) {
            scored.resize(topK);
        }
        topByDoc[i] = scored;
    }

    std::vector<BenchmarkTopologyNeighborObservation> observations;
    for (std::size_t i = 0; i < topByDoc.size(); ++i) {
        for (const auto& [j, score] : topByDoc[i]) {
            observations.push_back(BenchmarkTopologyNeighborObservation{
                .sourceHash = documents[i].documentHash,
                .targetHash = documents[j].documentHash,
                .score = score,
            });
        }
    }

    std::vector<BenchmarkTopologyEdgeWitness> witnesses;
    std::vector<LexicalTopologyNeighbor> out;
    for (std::size_t i = 0; i < topByDoc.size(); ++i) {
        for (const auto& [j, score] : topByDoc[i]) {
            if (reciprocalOnly) {
                const auto& reverse = topByDoc[j];
                const bool hasReverse =
                    std::any_of(reverse.begin(), reverse.end(),
                                [i](const auto& item) { return item.first == i; });
                if (!hasReverse) {
                    continue;
                }
            }
            witnesses.push_back(makeEdgeWitness(hashes, signalWeights, featureDf,
                                                BenchmarkTopologySignalKind::Entity, gates,
                                                topByDoc, i, j, score));
            out.push_back(LexicalTopologyNeighbor{.sourceHash = documents[i].documentHash,
                                                  .targetHash = documents[j].documentHash,
                                                  .score = score});
        }
    }
    std::sort(out.begin(), out.end(), [](const auto& left, const auto& right) {
        if (left.sourceHash != right.sourceHash) {
            return left.sourceHash < right.sourceHash;
        }
        return left.targetHash < right.targetHash;
    });
    std::sort(witnesses.begin(), witnesses.end(), [](const auto& left, const auto& right) {
        if (left.sourceHash != right.sourceHash) {
            return left.sourceHash < right.sourceHash;
        }
        return left.targetHash < right.targetHash;
    });
    finishCertificate(certificate, "graph", gates, certScratch.signals, observations, witnesses);
    return out;
}

std::vector<LexicalTopologyNeighbor> buildKeyphraseTopologyNeighbors(
    const std::vector<KeyphraseTopologyDocument>& documents, std::size_t topK, float minScore,
    std::size_t maxFeaturesPerDocument, std::size_t maxFeatureDocumentFrequency,
    bool reciprocalOnly, BenchmarkTopologyConstructionCertificate* certificate) {
    if (documents.size() < 2 || topK == 0 || maxFeaturesPerDocument == 0) {
        return {};
    }

    BenchmarkTopologyGateConfig gates;
    gates.minSignalWeight = 0.05F;
    gates.maxSignalDocumentFrequency = maxFeatureDocumentFrequency;
    gates.minNeighborScore = minScore;

    std::vector<std::string> hashes;
    hashes.reserve(documents.size());
    for (const auto& doc : documents) {
        hashes.push_back(doc.documentHash);
    }
    const auto featureWeights = buildKeyphraseFeatureWeights(documents, maxFeaturesPerDocument,
                                                             maxFeatureDocumentFrequency);
    const auto featureDf = computeDocumentFrequency(featureWeights);
    BenchmarkTopologyConstructionCertificate certScratch;
    populateCertificateSignals(certScratch, hashes, featureWeights,
                               BenchmarkTopologySignalKind::Keyphrase);

    std::vector<double> norms(featureWeights.size(), 0.0);
    for (std::size_t i = 0; i < featureWeights.size(); ++i) {
        for (const auto& [_, weight] : featureWeights[i]) {
            norms[i] += static_cast<double>(weight) * static_cast<double>(weight);
        }
    }

    std::vector<std::vector<std::pair<std::size_t, float>>> topByDoc(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        if (documents[i].documentHash.empty() || norms[i] <= 0.0) {
            continue;
        }
        std::vector<std::pair<std::size_t, float>> scored;
        for (std::size_t j = 0; j < documents.size(); ++j) {
            if (i == j || documents[j].documentHash.empty() || norms[j] <= 0.0) {
                continue;
            }
            double dot = 0.0;
            const auto* smaller = &featureWeights[i];
            const auto* larger = &featureWeights[j];
            if (smaller->size() > larger->size()) {
                std::swap(smaller, larger);
            }
            for (const auto& [feature, weight] : *smaller) {
                const auto jt = larger->find(feature);
                if (jt != larger->end()) {
                    dot += static_cast<double>(weight) * static_cast<double>(jt->second);
                }
            }
            const float score = static_cast<float>(dot / std::sqrt(norms[i] * norms[j]));
            if (score >= minScore) {
                scored.emplace_back(j, score);
            }
        }
        std::sort(scored.begin(), scored.end(), [&](const auto& left, const auto& right) {
            if (left.second != right.second) {
                return left.second > right.second;
            }
            return documents[left.first].documentHash < documents[right.first].documentHash;
        });
        if (scored.size() > topK) {
            scored.resize(topK);
        }
        topByDoc[i] = scored;
    }

    std::vector<BenchmarkTopologyNeighborObservation> observations;
    for (std::size_t i = 0; i < topByDoc.size(); ++i) {
        for (const auto& [j, score] : topByDoc[i]) {
            observations.push_back(BenchmarkTopologyNeighborObservation{
                .sourceHash = documents[i].documentHash,
                .targetHash = documents[j].documentHash,
                .score = score,
            });
        }
    }

    std::vector<BenchmarkTopologyEdgeWitness> witnesses;
    std::vector<LexicalTopologyNeighbor> out;
    for (std::size_t i = 0; i < topByDoc.size(); ++i) {
        for (const auto& [j, score] : topByDoc[i]) {
            if (reciprocalOnly) {
                const auto& reverse = topByDoc[j];
                const bool hasReverse =
                    std::any_of(reverse.begin(), reverse.end(),
                                [i](const auto& item) { return item.first == i; });
                if (!hasReverse) {
                    continue;
                }
            }
            witnesses.push_back(makeEdgeWitness(hashes, featureWeights, featureDf,
                                                BenchmarkTopologySignalKind::Keyphrase, gates,
                                                topByDoc, i, j, score));
            out.push_back(LexicalTopologyNeighbor{.sourceHash = documents[i].documentHash,
                                                  .targetHash = documents[j].documentHash,
                                                  .score = score});
        }
    }
    std::sort(out.begin(), out.end(), [](const auto& left, const auto& right) {
        if (left.sourceHash != right.sourceHash) {
            return left.sourceHash < right.sourceHash;
        }
        return left.targetHash < right.targetHash;
    });
    std::sort(witnesses.begin(), witnesses.end(), [](const auto& left, const auto& right) {
        if (left.sourceHash != right.sourceHash) {
            return left.sourceHash < right.sourceHash;
        }
        return left.targetHash < right.targetHash;
    });
    finishCertificate(certificate, "keyphrase", gates, certScratch.signals, observations,
                      witnesses);
    return out;
}

std::vector<LexicalTopologyNeighbor> buildSegmentKeyphraseTopologyNeighbors(
    const std::vector<KeyphraseTopologyDocument>& documents, std::size_t topK, float minScore,
    std::size_t segmentTokenBudget, std::size_t segmentTokenOverlap,
    std::size_t maxFeaturesPerSegment, std::size_t maxFeatureDocumentFrequency,
    std::size_t maxComponentDocs, std::size_t maxEdgesPerFeature, bool reciprocalOnly,
    BenchmarkTopologyConstructionCertificate* certificate) {
    if (documents.size() < 2 || topK == 0 || segmentTokenBudget == 0 ||
        maxFeaturesPerSegment == 0) {
        return {};
    }

    struct SegmentSeed {
        std::string documentHash;
        std::string segmentId;
        std::string text;
    };

    std::vector<SegmentSeed> segments;
    for (const auto& doc : documents) {
        if (doc.documentHash.empty()) {
            continue;
        }
        const auto tokens = tokenizeTopologyText(doc.text);
        if (tokens.empty()) {
            continue;
        }
        const std::size_t overlap = std::min(segmentTokenOverlap, segmentTokenBudget - 1);
        const std::size_t stride = std::max<std::size_t>(1, segmentTokenBudget - overlap);
        std::size_t segmentIndex = 0;
        for (std::size_t start = 0; start < tokens.size(); start += stride) {
            const std::size_t end = std::min(tokens.size(), start + segmentTokenBudget);
            std::ostringstream segmentText;
            for (std::size_t i = start; i < end; ++i) {
                if (i != start) {
                    segmentText << ' ';
                }
                segmentText << tokens[i];
            }
            segments.push_back(
                SegmentSeed{.documentHash = doc.documentHash,
                            .segmentId = doc.documentHash + "#seg" + std::to_string(segmentIndex),
                            .text = segmentText.str()});
            ++segmentIndex;
            if (end == tokens.size()) {
                break;
            }
        }
    }
    if (segments.size() < 2) {
        return {};
    }

    std::vector<KeyphraseTopologyDocument> segmentDocs;
    std::vector<std::string> parentHashes;
    std::vector<std::string> segmentIds;
    segmentDocs.reserve(segments.size());
    parentHashes.reserve(segments.size());
    segmentIds.reserve(segments.size());
    for (const auto& segment : segments) {
        segmentDocs.push_back({.documentHash = segment.documentHash, .text = segment.text});
        parentHashes.push_back(segment.documentHash);
        segmentIds.push_back(segment.segmentId);
    }

    BenchmarkTopologyGateConfig gates;
    gates.minSignalWeight = 0.05F;
    gates.maxSignalDocumentFrequency = maxFeatureDocumentFrequency;
    gates.minNeighborScore = minScore;
    gates.maxComponentDocs = maxComponentDocs;

    const auto featureWeights = buildKeyphraseFeatureWeights(segmentDocs, maxFeaturesPerSegment,
                                                             maxFeatureDocumentFrequency);
    const auto featureDf = computeDocumentFrequency(featureWeights);
    BenchmarkTopologyConstructionCertificate certScratch;
    populateCertificateSignals(certScratch, parentHashes, featureWeights,
                               BenchmarkTopologySignalKind::Keyphrase);

    std::vector<double> norms(featureWeights.size(), 0.0);
    for (std::size_t i = 0; i < featureWeights.size(); ++i) {
        for (const auto& [_, weight] : featureWeights[i]) {
            norms[i] += static_cast<double>(weight) * static_cast<double>(weight);
        }
    }

    std::vector<std::vector<std::pair<std::size_t, float>>> topBySegment(segments.size());
    for (std::size_t i = 0; i < segments.size(); ++i) {
        if (norms[i] <= 0.0) {
            continue;
        }
        std::vector<std::pair<std::size_t, float>> scored;
        for (std::size_t j = 0; j < segments.size(); ++j) {
            if (i == j || parentHashes[i] == parentHashes[j] || norms[j] <= 0.0) {
                continue;
            }
            double dot = 0.0;
            const auto* smaller = &featureWeights[i];
            const auto* larger = &featureWeights[j];
            if (smaller->size() > larger->size()) {
                std::swap(smaller, larger);
            }
            for (const auto& [feature, weight] : *smaller) {
                const auto jt = larger->find(feature);
                if (jt != larger->end()) {
                    dot += static_cast<double>(weight) * static_cast<double>(jt->second);
                }
            }
            const float score = static_cast<float>(dot / std::sqrt(norms[i] * norms[j]));
            if (score >= minScore) {
                scored.emplace_back(j, score);
            }
        }
        std::sort(scored.begin(), scored.end(), [&](const auto& left, const auto& right) {
            if (left.second != right.second) {
                return left.second > right.second;
            }
            if (parentHashes[left.first] != parentHashes[right.first]) {
                return parentHashes[left.first] < parentHashes[right.first];
            }
            return segmentIds[left.first] < segmentIds[right.first];
        });
        if (scored.size() > topK) {
            scored.resize(topK);
        }
        topBySegment[i] = scored;
    }

    std::vector<BenchmarkTopologyNeighborObservation> observations;
    for (std::size_t i = 0; i < topBySegment.size(); ++i) {
        for (const auto& [j, score] : topBySegment[i]) {
            observations.push_back(BenchmarkTopologyNeighborObservation{
                .sourceHash = parentHashes[i],
                .targetHash = parentHashes[j],
                .sourceSegmentId = segmentIds[i],
                .targetSegmentId = segmentIds[j],
                .score = score,
            });
        }
    }

    struct CandidateWitness {
        BenchmarkTopologyEdgeWitness witness;
        std::size_t sourceIndex = 0;
        std::size_t targetIndex = 0;
    };
    std::vector<CandidateWitness> candidates;
    for (std::size_t i = 0; i < topBySegment.size(); ++i) {
        for (const auto& [j, score] : topBySegment[i]) {
            if (reciprocalOnly) {
                const auto& reverse = topBySegment[j];
                const bool hasReverse =
                    std::any_of(reverse.begin(), reverse.end(),
                                [i](const auto& item) { return item.first == i; });
                if (!hasReverse) {
                    continue;
                }
            }
            auto witness = makeEdgeWitness(parentHashes, featureWeights, featureDf,
                                           BenchmarkTopologySignalKind::Keyphrase, gates,
                                           topBySegment, i, j, score);
            witness.sourceSegmentId = segmentIds[i];
            witness.targetSegmentId = segmentIds[j];
            if (!witness.signalGatePassed || witness.featureId.empty()) {
                continue;
            }
            candidates.push_back(
                CandidateWitness{.witness = witness, .sourceIndex = i, .targetIndex = j});
        }
    }
    std::sort(candidates.begin(), candidates.end(), [](const auto& left, const auto& right) {
        if (left.witness.score != right.witness.score) {
            return left.witness.score > right.witness.score;
        }
        if (left.witness.documentFrequency != right.witness.documentFrequency) {
            return left.witness.documentFrequency < right.witness.documentFrequency;
        }
        if (left.witness.sourceHash != right.witness.sourceHash) {
            return left.witness.sourceHash < right.witness.sourceHash;
        }
        return left.witness.targetHash < right.witness.targetHash;
    });

    struct DisjointSet {
        std::unordered_map<std::string, std::string> parent;
        std::unordered_map<std::string, std::size_t> size;

        std::string find(const std::string& value) {
            auto [it, inserted] = parent.emplace(value, value);
            if (inserted) {
                size[value] = 1;
                return value;
            }
            if (it->second == value) {
                return value;
            }
            it->second = find(it->second);
            return it->second;
        }

        bool uniteIfWithinCap(const std::string& left, const std::string& right,
                              std::size_t maxSize) {
            auto leftRoot = find(left);
            auto rightRoot = find(right);
            if (leftRoot == rightRoot) {
                return true;
            }
            const std::size_t mergedSize = size[leftRoot] + size[rightRoot];
            if (mergedSize > maxSize) {
                return false;
            }
            if (size[leftRoot] < size[rightRoot]) {
                std::swap(leftRoot, rightRoot);
            }
            parent[rightRoot] = leftRoot;
            size[leftRoot] = mergedSize;
            return true;
        }
    } components;

    std::unordered_map<std::string, std::size_t> acceptedByFeature;
    std::unordered_set<std::string> acceptedDirectedPairs;
    std::vector<BenchmarkTopologyEdgeWitness> witnesses;
    std::vector<LexicalTopologyNeighbor> out;
    for (const auto& candidate : candidates) {
        const auto& witness = candidate.witness;
        const std::string pairKey = witness.sourceHash + "\n" + witness.targetHash;
        if (!acceptedDirectedPairs.insert(pairKey).second) {
            continue;
        }
        if (maxEdgesPerFeature > 0 && acceptedByFeature[witness.featureId] >= maxEdgesPerFeature) {
            continue;
        }
        if (!components.uniteIfWithinCap(witness.sourceHash, witness.targetHash,
                                         maxComponentDocs)) {
            continue;
        }
        ++acceptedByFeature[witness.featureId];
        witnesses.push_back(witness);
        out.push_back(LexicalTopologyNeighbor{.sourceHash = witness.sourceHash,
                                              .targetHash = witness.targetHash,
                                              .score = witness.score});
    }

    std::sort(out.begin(), out.end(), [](const auto& left, const auto& right) {
        if (left.sourceHash != right.sourceHash) {
            return left.sourceHash < right.sourceHash;
        }
        return left.targetHash < right.targetHash;
    });
    std::sort(witnesses.begin(), witnesses.end(), [](const auto& left, const auto& right) {
        if (left.sourceHash != right.sourceHash) {
            return left.sourceHash < right.sourceHash;
        }
        return left.targetHash < right.targetHash;
    });
    finishCertificate(certificate, "segment_keyphrase", gates, certScratch.signals, observations,
                      witnesses);
    return out;
}

BenchmarkTopologyValidationResult validateTopologyConstructionCertificate(
    const BenchmarkTopologyConstructionCertificate& certificate) {
    BenchmarkTopologyValidationResult result;
    const auto addError = [&](const std::string& message) {
        result.ok = false;
        result.errors.push_back(message);
    };

    for (const auto& witness : certificate.edgeWitnesses) {
        if (witness.sourceHash.empty() || witness.targetHash.empty()) {
            addError("topology edge witness has empty endpoint");
        }
        if (witness.score < certificate.gates.minNeighborScore) {
            addError("topology edge witness score is below minNeighborScore");
        }
        if (!witness.reciprocal) {
            addError("topology edge witness is missing reciprocal top-k observation");
        }
        if (!witness.signalGatePassed || witness.featureId.empty()) {
            addError("topology edge witness is missing an admitted source signal");
        }
        if (witness.sourceWeight < certificate.gates.minSignalWeight ||
            witness.targetWeight < certificate.gates.minSignalWeight) {
            addError("topology edge witness signal weight is below minSignalWeight");
        }
        if (witness.documentFrequency > certificate.gates.maxSignalDocumentFrequency) {
            addError("topology edge witness feature exceeds maxSignalDocumentFrequency");
        }

        if (certificate.sourceName == "segment_keyphrase" &&
            (witness.sourceSegmentId.empty() || witness.targetSegmentId.empty())) {
            addError("segment topology edge witness is missing segment identifiers");
        }

        const bool hasForward = std::any_of(
            certificate.observations.begin(), certificate.observations.end(), [&](const auto& obs) {
                const bool endpointsMatch = obs.sourceHash == witness.sourceHash &&
                                            obs.targetHash == witness.targetHash &&
                                            obs.score >= certificate.gates.minNeighborScore;
                if (!endpointsMatch || certificate.sourceName != "segment_keyphrase") {
                    return endpointsMatch;
                }
                return obs.sourceSegmentId == witness.sourceSegmentId &&
                       obs.targetSegmentId == witness.targetSegmentId;
            });
        const bool hasReverse = std::any_of(
            certificate.observations.begin(), certificate.observations.end(), [&](const auto& obs) {
                const bool endpointsMatch = obs.sourceHash == witness.targetHash &&
                                            obs.targetHash == witness.sourceHash &&
                                            obs.score >= certificate.gates.minNeighborScore;
                if (!endpointsMatch || certificate.sourceName != "segment_keyphrase") {
                    return endpointsMatch;
                }
                return obs.sourceSegmentId == witness.targetSegmentId &&
                       obs.targetSegmentId == witness.sourceSegmentId;
            });
        if (!hasForward || !hasReverse) {
            addError("topology edge witness lacks forward/reverse observation evidence");
        }
    }

    for (const auto& component : certificate.components) {
        if (component.memberHashes.size() > certificate.gates.maxComponentDocs) {
            addError("topology component exceeds maxComponentDocs");
        }
    }
    return result;
}

nlohmann::json
topologyConstructionCertificateToJson(const BenchmarkTopologyConstructionCertificate& certificate,
                                      const BenchmarkTopologyValidationResult& validation) {
    nlohmann::json j;
    j["source"] = certificate.sourceName;
    j["gates"] = {
        {"min_signal_weight", certificate.gates.minSignalWeight},
        {"max_signal_document_frequency", certificate.gates.maxSignalDocumentFrequency},
        {"min_neighbor_score", certificate.gates.minNeighborScore},
        {"max_component_docs", certificate.gates.maxComponentDocs},
    };
    j["summary"] = {
        {"signals", certificate.signals.size()},
        {"observations", certificate.observations.size()},
        {"edge_witnesses", certificate.edgeWitnesses.size()},
        {"components", certificate.components.size()},
    };
    j["validation"] = {{"ok", validation.ok}, {"errors", validation.errors}};

    auto& signals = j["signals"] = nlohmann::json::array();
    for (const auto& signal : certificate.signals) {
        signals.push_back({
            {"doc", signal.documentHash},
            {"feature", signal.featureId},
            {"kind", topologySignalKindName(signal.kind)},
            {"weight", signal.weight},
            {"document_frequency", signal.documentFrequency},
        });
    }

    auto& observations = j["observations"] = nlohmann::json::array();
    for (const auto& obs : certificate.observations) {
        observations.push_back({
            {"source", obs.sourceHash},
            {"target", obs.targetHash},
            {"source_segment", obs.sourceSegmentId},
            {"target_segment", obs.targetSegmentId},
            {"score", obs.score},
        });
    }

    auto& edgeWitnesses = j["edge_witnesses"] = nlohmann::json::array();
    for (const auto& witness : certificate.edgeWitnesses) {
        edgeWitnesses.push_back({
            {"source", witness.sourceHash},
            {"target", witness.targetHash},
            {"source_segment", witness.sourceSegmentId},
            {"target_segment", witness.targetSegmentId},
            {"score", witness.score},
            {"feature", witness.featureId},
            {"kind", topologySignalKindName(witness.kind)},
            {"source_weight", witness.sourceWeight},
            {"target_weight", witness.targetWeight},
            {"document_frequency", witness.documentFrequency},
            {"reciprocal", witness.reciprocal},
            {"signal_gate_passed", witness.signalGatePassed},
        });
    }

    auto& components = j["components"] = nlohmann::json::array();
    for (const auto& component : certificate.components) {
        components.push_back({
            {"root", component.rootHash},
            {"member_count", component.memberHashes.size()},
            {"members", component.memberHashes},
        });
    }
    return j;
}

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
            query.relevantDocIds.insert(it->second.docId);
            query.relevanceGrades[it->second.docId] = it->second.score;
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
        dataset.qrels.emplace(queryId, BEIRQrel{.docId = docId, .score = score});
    }

    if (rootJson.contains("kg_entities")) {
        if (!rootJson["kg_entities"].is_array()) {
            return Error{ErrorCode::InvalidArgument,
                         "benchmark manifest kg_entities must be an array"};
        }
        for (const auto& item : rootJson["kg_entities"]) {
            BenchmarkKgEntity entity;
            entity.docId = item.value("doc_id", item.value("docId", std::string{}));
            entity.entityKey = item.value(
                "entity_key",
                item.value("entityKey",
                           item.value("entity_id", item.value("entityId", std::string{}))));
            entity.label = item.value("label", item.value("text", entity.entityKey));
            entity.type = item.value("type", std::string{"benchmark_entity"});
            entity.relation = item.value("relation", std::string{"mentioned_in"});
            entity.extractor = item.value("extractor", std::string{"benchmark_manifest"});
            entity.weight = std::clamp(item.value("weight", 1.0F), 0.0F, 1.0F);
            if (entity.docId.empty() || entity.entityKey.empty()) {
                return Error{ErrorCode::InvalidArgument,
                             "benchmark manifest kg_entity missing doc_id or entity_key"};
            }
            if (!dataset.documents.contains(entity.docId)) {
                return Error{ErrorCode::InvalidArgument,
                             "benchmark manifest kg_entity references unknown doc: " +
                                 entity.docId};
            }
            dataset.kgEntities.push_back(entity);
        }
    }

    spdlog::info(
        "Loaded benchmark manifest dataset '{}' with {} docs, {} queries, {} qrels, {} kg entities",
        dataset.name, dataset.documents.size(), dataset.queries.size(), dataset.qrels.size(),
        dataset.kgEntities.size());
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
