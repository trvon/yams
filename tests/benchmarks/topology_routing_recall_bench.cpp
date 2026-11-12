// Topology routing-recall bench.
//
// Question: when a query is routed to a few clusters, how often is its true best document
// inside them, and how much of that is lost to the way clusters are scored?
//
// The corpus is partitioned once with the k-means topology engine. Every router below then
// ranks the same clusters for the same held-out queries, so differences come from cluster
// scoring alone:
//
//   centroid        cosine to the cluster centroid
//   prod_router     the production router's full ranking with one representative: the same
//                   cosine plus its query-independent persistence bonus
//   prod_fitted     the production router's ranking given the lead-fitted vectors
//   farthest_R      max over the centroid and R-1 farthest-first member embeddings (the
//                   production multi-representative cover)
//   hot_R           max over the centroid and the R-1 members that are most often the exact
//                   best match for the training queries
//   ridge           yams::topology::fitRouteVectors: centroid plus a per-cluster correction
//                   fitted by ridge regression to the cluster's support function
//                   h_c(q) = max over members of <q, key> on the training queries. The
//                   strength is chosen on held-out training queries, never on the evaluation
//                   queries; "ridge(kept)" means the fit kept the centroids.
//   maxaff_R        R fitted vectors per cluster scored by their maximum (a max-affine, hence
//                   convex, model of h_c), trained by gradient descent from the ridge vector
//                   and the hot members, stopped at the best held-out training epoch
//   oracle          h_c(q) itself: the best any cluster scorer can do on this partition
//
// Training queries are synthesized from the corpus alone, in several styles, or taken from the
// even-indexed real queries:
//   lead      title and first sentence
//   span12    one 12-word span
//   span6     one 6-word span
//   keywords  the document's five rarest words
//   mix       all of the above
//   real      even-indexed real queries (an in-distribution upper bound)
//
// Evaluation uses the odd-indexed real queries, which no router is fitted on. A corpus with
// fewer than 100 real queries (for example a local corpus exported without a query log) is
// evaluated on a separate draw of span6 and keywords queries instead; the header line says
// which, and those numbers only show how well a router fits synthetic queries.
//
// Recall here is routing recall of the exact top-1 document under the embedding, not
// relevance: it isolates the router from embedding quality.
//
// Run (BEIR layout: corpus.jsonl, queries.jsonl):
//   topology_routing_recall_bench --dataset build/benchmarks/data/scifact \
//       [--engine kmeans|connected] [--clusters 0] [--max-docs 0] [--max-train 3000]
//       [--max-doc-chars 0]
//       [--out routing_recall.jsonl]

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_route_fit.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/simeon_embedding_backend.h>

namespace {

using json = nlohmann::json;
namespace fs = std::filesystem;

// Row-major float matrix.
struct Matrix {
    std::size_t rows{0};
    std::size_t dim{0};
    std::vector<float> data;

    [[nodiscard]] const float* row(std::size_t r) const { return data.data() + r * dim; }
    [[nodiscard]] float* row(std::size_t r) { return data.data() + r * dim; }
};

// The bench is single-threaded; leaving this kernel uninstrumented keeps a sanitizer build
// usable for the exhaustive scans.
#if defined(__clang__) || defined(__GNUC__)
__attribute__((no_sanitize("thread")))
#endif
float dot(const float* a, const float* b, std::size_t n) {
    float acc[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    std::size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        for (std::size_t j = 0; j < 8; ++j) {
            acc[j] += a[i + j] * b[i + j];
        }
    }
    float sum = 0.0F;
    for (; i < n; ++i) {
        sum += a[i] * b[i];
    }
    for (const float part : acc) {
        sum += part;
    }
    return sum;
}

struct Options {
    fs::path dataset;
    fs::path out;
    std::size_t clusters{0};
    std::size_t maxDocs{0};
    std::size_t maxTrain{3000};
    // Documents are cut to this many bytes before embedding; 0 keeps them whole.
    std::size_t maxDocChars{0};
    // "kmeans", or "connected" for the production connected-components engine over an exact
    // 32-nearest-neighbour graph with the daemon's construction settings.
    std::string engine{"kmeans"};
};

bool parseArgs(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        const auto value = [&]() -> const char* { return i + 1 < argc ? argv[++i] : nullptr; };
        if (arg == "--dataset") {
            if (const char* v = value()) {
                options.dataset = v;
            }
        } else if (arg == "--out") {
            if (const char* v = value()) {
                options.out = v;
            }
        } else if (arg == "--clusters") {
            if (const char* v = value()) {
                options.clusters = std::stoull(v);
            }
        } else if (arg == "--max-docs") {
            if (const char* v = value()) {
                options.maxDocs = std::stoull(v);
            }
        } else if (arg == "--max-train") {
            if (const char* v = value()) {
                options.maxTrain = std::stoull(v);
            }
        } else if (arg == "--engine") {
            if (const char* v = value()) {
                options.engine = v;
            }
        } else if (arg == "--max-doc-chars") {
            if (const char* v = value()) {
                options.maxDocChars = std::stoull(v);
            }
        } else {
            std::cerr << "unknown argument: " << arg << "\n";
            return false;
        }
    }
    return !options.dataset.empty();
}

struct Document {
    std::string id;
    std::string title;
    std::string text;
};

std::vector<Document> loadCorpus(const fs::path& path, std::size_t maxDocs) {
    std::vector<Document> docs;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const auto row = json::parse(line, nullptr, false);
        if (row.is_discarded()) {
            continue;
        }
        docs.push_back({row.value("_id", std::string{}), row.value("title", std::string{}),
                        row.value("text", std::string{})});
        if (maxDocs != 0 && docs.size() >= maxDocs) {
            break;
        }
    }
    return docs;
}

struct QueryRow {
    std::string id;
    std::string text;
};

std::vector<QueryRow> loadQueries(const fs::path& path) {
    std::vector<QueryRow> queries;
    std::ifstream in(path);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const auto row = json::parse(line, nullptr, false);
        if (row.is_discarded()) {
            continue;
        }
        auto text = row.value("text", std::string{});
        if (!text.empty()) {
            queries.push_back({row.value("_id", std::string{}), std::move(text)});
        }
    }
    return queries;
}

// BEIR qrels: "query-id<TAB>corpus-id<TAB>score" with a header line. Positive grades only.
std::unordered_map<std::string, std::vector<std::pair<std::string, int>>>
loadQrels(const fs::path& path) {
    std::unordered_map<std::string, std::vector<std::pair<std::string, int>>> qrels;
    std::ifstream in(path);
    std::string line;
    bool header = true;
    while (std::getline(in, line)) {
        if (header) {
            header = false;
            continue;
        }
        const auto first = line.find('\t');
        const auto second = first == std::string::npos ? first : line.find('\t', first + 1);
        if (second == std::string::npos) {
            continue;
        }
        const int grade = std::atoi(line.c_str() + second + 1);
        if (grade > 0) {
            qrels[line.substr(0, first)].emplace_back(line.substr(first + 1, second - first - 1),
                                                      grade);
        }
    }
    return qrels;
}

std::vector<std::string_view> splitWords(std::string_view text) {
    std::vector<std::string_view> words;
    std::size_t i = 0;
    while (i < text.size()) {
        while (i < text.size() && text[i] == ' ') {
            ++i;
        }
        const std::size_t start = i;
        while (i < text.size() && text[i] != ' ') {
            ++i;
        }
        if (i > start) {
            words.push_back(text.substr(start, i - start));
        }
    }
    return words;
}

std::string joinWords(const std::vector<std::string_view>& words, std::size_t start,
                      std::size_t count) {
    std::string out;
    for (std::size_t w = start; w < start + count && w < words.size(); ++w) {
        if (!out.empty()) {
            out.push_back(' ');
        }
        out.append(words[w]);
    }
    return out;
}

std::string lowerAlnum(std::string_view word) {
    std::string out;
    for (const char c : word) {
        const auto u = static_cast<unsigned char>(c);
        if ((u >= '0' && u <= '9') || (u >= 'a' && u <= 'z')) {
            out.push_back(c);
        } else if (u >= 'A' && u <= 'Z') {
            out.push_back(static_cast<char>(u - 'A' + 'a'));
        }
    }
    return out;
}

enum class QueryStyle { Lead, Span12, Span6, Keywords, Mix };

// Queries synthesized from the corpus alone. `seed` separates a training draw from an
// evaluation draw of the same style.
std::vector<std::string> generateQueries(const std::vector<Document>& docs, QueryStyle style,
                                         std::size_t limit, std::uint64_t seed) {
    const bool lead = style == QueryStyle::Lead || style == QueryStyle::Mix;
    const bool span12 = style == QueryStyle::Span12 || style == QueryStyle::Mix;
    const bool span6 = style == QueryStyle::Span6 || style == QueryStyle::Mix;
    const bool keywords = style == QueryStyle::Keywords || style == QueryStyle::Mix;

    std::unordered_map<std::string, std::uint32_t> documentFrequency;
    if (keywords) {
        std::vector<std::string> seen;
        for (const auto& doc : docs) {
            seen.clear();
            for (const auto word : splitWords(doc.text)) {
                auto token = lowerAlnum(word);
                if (token.size() >= 4) {
                    seen.push_back(std::move(token));
                }
            }
            std::ranges::sort(seen);
            seen.erase(std::unique(seen.begin(), seen.end()), seen.end());
            for (const auto& token : seen) {
                ++documentFrequency[token];
            }
        }
    }

    std::vector<std::string> all;
    std::mt19937_64 rng(seed);
    for (const auto& doc : docs) {
        const auto words = splitWords(doc.text);
        if (lead) {
            if (!doc.title.empty()) {
                all.push_back(doc.title);
            }
            if (const auto end = doc.text.find(". "); end != std::string::npos && end > 20) {
                all.push_back(doc.text.substr(0, end));
            }
        }
        if (span12 && words.size() > 24) {
            all.push_back(joinWords(words, rng() % (words.size() - 12), 12));
        }
        if (span6 && words.size() > 12) {
            all.push_back(joinWords(words, rng() % (words.size() - 6), 6));
        }
        if (keywords) {
            // The five rarest words that still occur in another document, in reading order.
            std::vector<std::pair<std::uint32_t, std::size_t>> ranked;
            std::vector<std::string> tokens(words.size());
            for (std::size_t w = 0; w < words.size(); ++w) {
                tokens[w] = lowerAlnum(words[w]);
                const auto it = documentFrequency.find(tokens[w]);
                if (it != documentFrequency.end() && it->second >= 2) {
                    ranked.emplace_back(it->second, w);
                }
            }
            std::ranges::sort(ranked);
            std::vector<std::size_t> chosen;
            for (const auto& [frequency, position] : ranked) {
                (void)frequency;
                const bool duplicate = std::ranges::any_of(
                    chosen, [&](std::size_t other) { return tokens[other] == tokens[position]; });
                if (!duplicate) {
                    chosen.push_back(position);
                }
                if (chosen.size() == 5) {
                    break;
                }
            }
            if (chosen.size() >= 3) {
                std::ranges::sort(chosen);
                std::string query;
                for (const auto position : chosen) {
                    if (!query.empty()) {
                        query.push_back(' ');
                    }
                    query.append(tokens[position]);
                }
                all.push_back(std::move(query));
            }
        }
    }
    if (limit == 0 || all.size() <= limit) {
        return all;
    }
    // Even stride keeps every part of the corpus represented.
    std::vector<std::string> sampled;
    sampled.reserve(limit);
    for (std::size_t i = 0; i < limit; ++i) {
        sampled.push_back(std::move(all[i * all.size() / limit]));
    }
    return sampled;
}

bool embed(yams::vector::IEmbeddingBackend& backend, const std::vector<std::string>& texts,
           Matrix& out) {
    out.rows = texts.size();
    out.dim = backend.getEmbeddingDimension();
    out.data.assign(out.rows * out.dim, 0.0F);
    for (std::size_t i = 0; i < texts.size(); ++i) {
        auto embedding = backend.generateEmbedding(texts[i]);
        if (!embedding || embedding.value().size() != out.dim) {
            return false;
        }
        std::copy(embedding.value().begin(), embedding.value().end(), out.row(i));
    }
    return true;
}

// The partition every router is measured on.
struct Partition {
    std::size_t clusters{0};
    std::vector<std::uint32_t> clusterOfDoc;
    std::vector<std::uint32_t> memberCount;
    // Unit centroid directions, one row per cluster.
    Matrix centroids;
    // Farthest-first member representatives per cluster (document indices), in selection order.
    std::vector<std::vector<std::uint32_t>> farthest;
};

// Support function values h_c(q) for every query and cluster, plus the exact best document.
struct Support {
    // rows = queries, dim = clusters.
    Matrix values;
    std::vector<std::uint32_t> bestDoc;
};

Support computeSupport(const Matrix& queries, const Matrix& keys, const Partition& partition) {
    Support support;
    support.values.rows = queries.rows;
    support.values.dim = partition.clusters;
    support.values.data.assign(queries.rows * partition.clusters, -2.0F);
    support.bestDoc.assign(queries.rows, 0);
    for (std::size_t q = 0; q < queries.rows; ++q) {
        float* perCluster = support.values.row(q);
        float best = -2.0F;
        for (std::size_t k = 0; k < keys.rows; ++k) {
            const float score = dot(queries.row(q), keys.row(k), keys.dim);
            float& slot = perCluster[partition.clusterOfDoc[k]];
            slot = std::max(slot, score);
            if (score > best) {
                best = score;
                support.bestDoc[q] = static_cast<std::uint32_t>(k);
            }
        }
    }
    return support;
}

constexpr std::size_t kProbes[] = {1, 2, 3, 5, 8, 16, 32, 64};
constexpr double kBudgets[] = {0.01, 0.02, 0.05, 0.10, 0.20};

// Exhaustive ranking of the corpus for every evaluation query, and its judged documents. Set
// once in main when the dataset has relevance judgments.
struct Retrieval {
    // Per query: every document index, best score first.
    std::vector<std::vector<std::uint32_t>> ranked;
    // Per query: (document index, grade) for judged-relevant documents in the corpus.
    std::vector<std::vector<std::pair<std::uint32_t, int>>> relevant;
};
const Retrieval* gRetrieval = nullptr;

constexpr std::size_t kCutoff = 10;

// Recall@10 and nDCG@10 of the first ten ranked documents that pass `admitted`.
template <typename Admitted>
void scoreTopTen(const std::vector<std::uint32_t>& ranked,
                 const std::vector<std::pair<std::uint32_t, int>>& relevant, Admitted&& admitted,
                 double& recall, double& ndcg) {
    double dcg = 0.0;
    std::size_t hits = 0;
    std::size_t taken = 0;
    for (const auto doc : ranked) {
        if (!admitted(doc)) {
            continue;
        }
        for (const auto& [relevantDoc, grade] : relevant) {
            if (relevantDoc == doc) {
                ++hits;
                dcg += static_cast<double>(grade) / std::log2(static_cast<double>(taken) + 2.0);
                break;
            }
        }
        if (++taken == kCutoff) {
            break;
        }
    }
    std::vector<int> grades;
    grades.reserve(relevant.size());
    for (const auto& [doc, grade] : relevant) {
        (void)doc;
        grades.push_back(grade);
    }
    std::ranges::sort(grades, std::greater<>{});
    double ideal = 0.0;
    for (std::size_t i = 0; i < std::min(grades.size(), kCutoff); ++i) {
        ideal += static_cast<double>(grades[i]) / std::log2(static_cast<double>(i) + 2.0);
    }
    recall = static_cast<double>(hits) / static_cast<double>(relevant.size());
    ndcg = ideal > 0.0 ? dcg / ideal : 0.0;
}

struct Curve {
    std::string arm;
    std::string training;
    std::size_t vectorsPerCluster{1};
    double recallAtProbe[std::size(kProbes)] = {};
    double scanAtProbe[std::size(kProbes)] = {};
    double recallAtBudget[std::size(kBudgets)] = {};
    double meanRankOfTrueCluster{0.0};
    // Retrieval quality against relevance judgments when only the top-s clusters are searched.
    double recall10AtProbe[std::size(kProbes)] = {};
    double ndcg10AtProbe[std::size(kProbes)] = {};
    std::size_t judgedQueries{0};
};

// scores: rows = queries, dim = clusters; higher is better.
Curve evaluate(std::string arm, std::string training, std::size_t vectorsPerCluster,
               const Matrix& scores, const Support& truth, const Partition& partition,
               std::size_t corpusSize) {
    Curve curve;
    curve.arm = std::move(arm);
    curve.training = std::move(training);
    curve.vectorsPerCluster = vectorsPerCluster;
    std::vector<std::uint32_t> order(partition.clusters);
    double rankSum = 0.0;
    for (std::size_t q = 0; q < scores.rows; ++q) {
        const float* row = scores.row(q);
        std::iota(order.begin(), order.end(), 0U);
        std::ranges::sort(order, [&](std::uint32_t a, std::uint32_t b) {
            return row[a] != row[b] ? row[a] > row[b] : a < b;
        });
        const std::uint32_t trueCluster = partition.clusterOfDoc[truth.bestDoc[q]];
        const std::size_t rank =
            static_cast<std::size_t>(std::ranges::find(order, trueCluster) - order.begin());
        rankSum += static_cast<double>(rank + 1);

        if (gRetrieval != nullptr && !gRetrieval->relevant[q].empty()) {
            std::vector<std::uint32_t> clusterRank(partition.clusters);
            for (std::size_t position = 0; position < order.size(); ++position) {
                clusterRank[order[position]] = static_cast<std::uint32_t>(position);
            }
            ++curve.judgedQueries;
            for (std::size_t p = 0; p < std::size(kProbes); ++p) {
                double recall = 0.0;
                double ndcg = 0.0;
                scoreTopTen(
                    gRetrieval->ranked[q], gRetrieval->relevant[q],
                    [&](std::uint32_t doc) {
                        return clusterRank[partition.clusterOfDoc[doc]] < kProbes[p];
                    },
                    recall, ndcg);
                curve.recall10AtProbe[p] += recall;
                curve.ndcg10AtProbe[p] += ndcg;
            }
        }

        std::size_t scanned = 0;
        std::size_t probeIndex = 0;
        std::size_t budgetIndex = 0;
        for (std::size_t position = 0; position < order.size(); ++position) {
            // A budget admits every cluster that fits, and always the first one.
            while (budgetIndex < std::size(kBudgets) && position > 0 &&
                   static_cast<double>(scanned + partition.memberCount[order[position]]) >
                       kBudgets[budgetIndex] * static_cast<double>(corpusSize)) {
                curve.recallAtBudget[budgetIndex] += rank < position ? 1.0 : 0.0;
                ++budgetIndex;
            }
            scanned += partition.memberCount[order[position]];
            while (probeIndex < std::size(kProbes) && position + 1 == kProbes[probeIndex]) {
                curve.recallAtProbe[probeIndex] += rank <= position ? 1.0 : 0.0;
                curve.scanAtProbe[probeIndex] +=
                    static_cast<double>(scanned) / static_cast<double>(corpusSize);
                ++probeIndex;
            }
        }
        for (; budgetIndex < std::size(kBudgets); ++budgetIndex) {
            curve.recallAtBudget[budgetIndex] += 1.0;
        }
        for (; probeIndex < std::size(kProbes); ++probeIndex) {
            curve.recallAtProbe[probeIndex] += 1.0;
            curve.scanAtProbe[probeIndex] += 1.0;
        }
    }
    const double n = static_cast<double>(std::max<std::size_t>(1, scores.rows));
    for (auto& value : curve.recallAtProbe) {
        value /= n;
    }
    for (auto& value : curve.scanAtProbe) {
        value /= n;
    }
    for (auto& value : curve.recallAtBudget) {
        value /= n;
    }
    curve.meanRankOfTrueCluster = rankSum / n;
    if (curve.judgedQueries > 0) {
        for (auto& value : curve.recall10AtProbe) {
            value /= static_cast<double>(curve.judgedQueries);
        }
        for (auto& value : curve.ndcg10AtProbe) {
            value /= static_cast<double>(curve.judgedQueries);
        }
    }
    return curve;
}

// score(q, c) = max over the cluster's vectors of <q, v>. vectors[c] holds row indices into
// `pool`; the centroid row of cluster c is always included by the caller.
Matrix scoreMaxOver(const Matrix& queries, const Matrix& centroids, const Matrix& keys,
                    const std::vector<std::vector<std::uint32_t>>& extraDocs,
                    std::size_t extraPerCluster) {
    Matrix scores;
    scores.rows = queries.rows;
    scores.dim = centroids.rows;
    scores.data.assign(scores.rows * scores.dim, 0.0F);
    for (std::size_t q = 0; q < queries.rows; ++q) {
        for (std::size_t c = 0; c < centroids.rows; ++c) {
            float best = dot(queries.row(q), centroids.row(c), centroids.dim);
            const auto& extras = extraDocs[c];
            for (std::size_t e = 0; e < std::min(extraPerCluster, extras.size()); ++e) {
                best = std::max(best, dot(queries.row(q), keys.row(extras[e]), keys.dim));
            }
            scores.row(q)[c] = best;
        }
    }
    return scores;
}

Matrix centroidScores(const Matrix& queries, const Matrix& centroids) {
    // One empty extra list per cluster: the centroid is the only vector scored.
    const std::vector<std::vector<std::uint32_t>> noExtras(centroids.rows);
    return scoreMaxOver(queries, centroids, centroids, noExtras, 0);
}

Matrix selectRows(const Matrix& source, std::size_t parity) {
    Matrix out;
    out.dim = source.dim;
    for (std::size_t r = parity; r < source.rows; r += 2) {
        out.data.insert(out.data.end(), source.row(r), source.row(r) + source.dim);
        ++out.rows;
    }
    return out;
}

// Every fifth training query is held out for model selection.
bool isHoldout(std::size_t index) {
    return index % 5 == 4;
}

Matrix splitRows(const Matrix& source, bool holdout) {
    Matrix out;
    out.dim = source.dim;
    for (std::size_t r = 0; r < source.rows; ++r) {
        if (isHoldout(r) == holdout) {
            out.data.insert(out.data.end(), source.row(r), source.row(r) + source.dim);
            ++out.rows;
        }
    }
    return out;
}

Support splitSupport(const Support& source, bool holdout) {
    Support out;
    out.values = splitRows(source.values, holdout);
    for (std::size_t r = 0; r < source.bestDoc.size(); ++r) {
        if (isHoldout(r) == holdout) {
            out.bestDoc.push_back(source.bestDoc[r]);
        }
    }
    return out;
}

// Model-selection score: share of queries whose true cluster is in the top five.
double recallAtFive(const Matrix& scores, const Support& truth, const Partition& partition) {
    std::size_t hits = 0;
    for (std::size_t q = 0; q < scores.rows; ++q) {
        const float* row = scores.row(q);
        const std::uint32_t trueCluster = partition.clusterOfDoc[truth.bestDoc[q]];
        std::size_t better = 0;
        for (std::size_t c = 0; c < scores.dim; ++c) {
            if (row[c] > row[trueCluster] || (row[c] == row[trueCluster] && c < trueCluster)) {
                ++better;
            }
        }
        hits += better < 5 ? 1 : 0;
    }
    return scores.rows == 0 ? 0.0 : static_cast<double>(hits) / static_cast<double>(scores.rows);
}

// R vectors per cluster, laid out cluster-major: row c * pieces + r.
struct MaxAffine {
    std::size_t pieces{1};
    Matrix vectors;
};

Matrix scoreMaxAffine(const Matrix& queries, const MaxAffine& model) {
    Matrix scores;
    scores.rows = queries.rows;
    scores.dim = model.vectors.rows / model.pieces;
    scores.data.assign(scores.rows * scores.dim, 0.0F);
    for (std::size_t q = 0; q < queries.rows; ++q) {
        for (std::size_t c = 0; c < scores.dim; ++c) {
            float best = dot(queries.row(q), model.vectors.row(c * model.pieces), queries.dim);
            for (std::size_t r = 1; r < model.pieces; ++r) {
                best = std::max(best, dot(queries.row(q), model.vectors.row(c * model.pieces + r),
                                          queries.dim));
            }
            scores.row(q)[c] = best;
        }
    }
    return scores;
}

// Fit max_r <q, v_cr> to h_c(q) by gradient descent on squared error. Only the piece that
// attains the maximum moves, so the model stays a maximum of linear functions. Returns the
// epoch with the best held-out recall, which may be the starting point.
MaxAffine fitMaxAffine(MaxAffine model, const Matrix& fitQueries, const Support& fitSupport,
                       const Matrix& holdoutQueries, const Support& holdoutSupport,
                       const Partition& partition) {
    constexpr std::size_t kEpochs = 12;
    const std::size_t clusters = partition.clusters;
    const std::size_t d = fitQueries.dim;
    std::vector<std::uint32_t> order(fitQueries.rows);
    std::iota(order.begin(), order.end(), 0U);
    std::mt19937_64 rng(0xC0FFEEULL);

    MaxAffine best = model;
    double bestScore =
        recallAtFive(scoreMaxAffine(holdoutQueries, model), holdoutSupport, partition);
    float rate = 0.2F;
    for (std::size_t epoch = 0; epoch < kEpochs; ++epoch) {
        std::shuffle(order.begin(), order.end(), rng);
        for (const auto n : order) {
            const float* q = fitQueries.row(n);
            const float* target = fitSupport.values.row(n);
            for (std::size_t c = 0; c < clusters; ++c) {
                std::size_t active = 0;
                float prediction = dot(q, model.vectors.row(c * model.pieces), d);
                for (std::size_t r = 1; r < model.pieces; ++r) {
                    const float value = dot(q, model.vectors.row(c * model.pieces + r), d);
                    if (value > prediction) {
                        prediction = value;
                        active = r;
                    }
                }
                const float step = rate * (prediction - target[c]);
                float* vector = model.vectors.row(c * model.pieces + active);
                for (std::size_t i = 0; i < d; ++i) {
                    vector[i] -= step * q[i];
                }
            }
        }
        rate *= 0.8F;
        const double score =
            recallAtFive(scoreMaxAffine(holdoutQueries, model), holdoutSupport, partition);
        if (score > bestScore) {
            bestScore = score;
            best = model;
        }
    }
    return best;
}

void print(const Curve& c) {
    std::printf("%-12s %-9s %3zu |", c.arm.c_str(), c.training.c_str(), c.vectorsPerCluster);
    for (std::size_t i = 0; i < std::size(kProbes); ++i) {
        std::printf(" %5.1f", 100.0 * c.recallAtProbe[i]);
    }
    std::printf(" |");
    for (std::size_t i = 0; i < std::size(kBudgets); ++i) {
        std::printf(" %5.1f", 100.0 * c.recallAtBudget[i]);
    }
    std::printf(" | %6.2f\n", c.meanRankOfTrueCluster);
}

} // namespace

int main(int argc, char** argv) {
    Options options;
    if (!parseArgs(argc, argv, options)) {
        std::cerr << "usage: topology_routing_recall_bench --dataset <beir-dir> [--clusters N] "
                     "[--max-docs N] [--max-train N] [--max-doc-chars N] [--out file.jsonl]\n";
        return 2;
    }

    const auto docs = loadCorpus(options.dataset / "corpus.jsonl", options.maxDocs);
    const auto queryRows = loadQueries(options.dataset / "queries.jsonl");
    const auto qrels = loadQrels(options.dataset / "qrels" / "test.tsv");
    // With relevance judgments, the judged queries are the evaluation set and every other
    // query may be trained on. Without them, odd-indexed queries are evaluated.
    std::vector<std::string> realTrainTexts;
    std::vector<std::string> realTestTexts;
    std::vector<std::string> realTestIds;
    for (std::size_t i = 0; i < queryRows.size(); ++i) {
        const bool evaluated = qrels.empty() ? i % 2 == 1 : qrels.contains(queryRows[i].id);
        if (evaluated) {
            realTestTexts.push_back(queryRows[i].text);
            realTestIds.push_back(queryRows[i].id);
        } else {
            realTrainTexts.push_back(queryRows[i].text);
        }
    }
    if (docs.size() < 16) {
        std::cerr << "dataset too small or unreadable: " << options.dataset << "\n";
        return 2;
    }

    yams::vector::EmbeddingConfig embeddingConfig;
    auto backend = yams::vector::makeSimeonBackend(embeddingConfig);
    if (!backend || !backend->initialize()) {
        std::cerr << "embedding backend unavailable\n";
        return 2;
    }

    std::vector<std::string> docTexts;
    docTexts.reserve(docs.size());
    for (const auto& doc : docs) {
        auto text = doc.title.empty() ? doc.text : doc.title + "\n" + doc.text;
        if (options.maxDocChars != 0 && text.size() > options.maxDocChars) {
            text.resize(options.maxDocChars);
        }
        docTexts.push_back(std::move(text));
    }

    const bool realEvaluation = realTestTexts.size() >= 50;
    Matrix keys;
    Matrix test;
    Matrix trainReal;
    if (!embed(*backend, docTexts, keys)) {
        std::cerr << "embedding failed\n";
        return 2;
    }
    if (realEvaluation) {
        if (!embed(*backend, realTestTexts, test) ||
            (!realTrainTexts.empty() && !embed(*backend, realTrainTexts, trainReal))) {
            std::cerr << "embedding failed\n";
            return 2;
        }
    } else {
        // No usable query log: evaluate on a separate draw of short synthetic queries.
        auto evaluation = generateQueries(docs, QueryStyle::Span6, 500, 0xE7A1ULL);
        const auto keywordEvaluation = generateQueries(docs, QueryStyle::Keywords, 500, 0xE7A2ULL);
        evaluation.insert(evaluation.end(), keywordEvaluation.begin(), keywordEvaluation.end());
        if (!embed(*backend, evaluation, test)) {
            std::cerr << "embedding failed\n";
            return 2;
        }
    }

    // Partition with the production k-means engine, asking for a deep representative cover so
    // every farthest_R arm reads a prefix of the same selection.
    constexpr std::size_t kMaxRepresentatives = 8;
    std::vector<yams::topology::TopologyDocumentInput> inputs(docs.size());
    std::unordered_map<std::string, std::uint32_t> docIndex;
    for (std::size_t i = 0; i < docs.size(); ++i) {
        inputs[i].documentHash = "doc-" + std::to_string(i);
        inputs[i].embedding.assign(keys.row(i), keys.row(i) + keys.dim);
        docIndex.emplace(inputs[i].documentHash, static_cast<std::uint32_t>(i));
    }
    yams::topology::TopologyBuildConfig buildConfig;
    buildConfig.embeddingSpaceIdentity = backend->getEmbeddingSpaceIdentity();
    buildConfig.kmeansK = options.clusters;
    buildConfig.routingRepresentativeCount = kMaxRepresentatives;
    buildConfig.emitOutliers = false;
    const bool connected = options.engine == "connected";
    if (connected) {
        // The daemon's construction: reciprocal semantic-neighbour edges at or above 0.25,
        // 32 neighbours per document, components capped at 64 documents.
        constexpr std::size_t kNeighbors = 32;
        buildConfig.minEdgeScore = 0.25;
        buildConfig.maxComponentDocs = 64;
        buildConfig.reciprocalOnly = true;
        buildConfig.maxNeighborsPerDocument = kNeighbors;
        std::vector<std::vector<std::pair<float, std::uint32_t>>> nearest(docs.size());
        std::vector<std::pair<float, std::uint32_t>> scored(docs.size());
        for (std::size_t i = 0; i < docs.size(); ++i) {
            for (std::size_t j = 0; j < docs.size(); ++j) {
                scored[j] = {i == j ? -2.0F : dot(keys.row(i), keys.row(j), keys.dim),
                             static_cast<std::uint32_t>(j)};
            }
            const auto keep = std::min(kNeighbors, docs.size() - 1);
            std::partial_sort(scored.begin(), scored.begin() + static_cast<std::ptrdiff_t>(keep),
                              scored.end(), std::greater<>{});
            nearest[i].assign(scored.begin(), scored.begin() + static_cast<std::ptrdiff_t>(keep));
        }
        for (std::size_t i = 0; i < docs.size(); ++i) {
            for (const auto& [score, j] : nearest[i]) {
                const bool reciprocal = std::ranges::any_of(
                    nearest[j], [&](const auto& back) { return back.second == i; });
                inputs[i].neighbors.push_back({inputs[j].documentHash, score, reciprocal});
            }
        }
    }
    yams::topology::KMeansTopologyEngine kmeansEngine;
    yams::topology::ConnectedComponentTopologyEngine connectedEngine;
    auto built = connected ? connectedEngine.buildArtifacts(inputs, buildConfig)
                           : kmeansEngine.buildArtifacts(inputs, buildConfig);
    if (!built) {
        std::cerr << "topology build failed: " << built.error().message << "\n";
        return 2;
    }
    const auto& artifacts = built.value();

    Partition partition;
    partition.clusters = artifacts.clusters.size();
    partition.clusterOfDoc.assign(docs.size(), 0);
    partition.memberCount.assign(partition.clusters, 0);
    partition.centroids.rows = partition.clusters;
    partition.centroids.dim = keys.dim;
    partition.centroids.data.assign(partition.clusters * keys.dim, 0.0F);
    partition.farthest.resize(partition.clusters);
    std::size_t assigned = 0;
    for (std::size_t c = 0; c < artifacts.clusters.size(); ++c) {
        const auto& cluster = artifacts.clusters[c];
        if (cluster.centroidEmbedding.size() == keys.dim) {
            const float norm = std::sqrt(
                dot(cluster.centroidEmbedding.data(), cluster.centroidEmbedding.data(), keys.dim));
            for (std::size_t i = 0; i < keys.dim; ++i) {
                partition.centroids.row(c)[i] =
                    norm > 0.0F ? cluster.centroidEmbedding[i] / norm : 0.0F;
            }
        }
        for (const auto& hash : cluster.memberDocumentHashes) {
            if (const auto it = docIndex.find(hash); it != docIndex.end()) {
                partition.clusterOfDoc[it->second] = static_cast<std::uint32_t>(c);
                ++partition.memberCount[c];
                ++assigned;
            }
        }
        for (const auto& representative : cluster.routingRepresentatives) {
            if (const auto it = docIndex.find(representative.documentHash); it != docIndex.end()) {
                partition.farthest[c].push_back(it->second);
            }
        }
    }
    if (assigned != docs.size()) {
        std::cerr << "partition does not cover the corpus exactly once: assigned=" << assigned
                  << " docs=" << docs.size() << "\n";
        return 2;
    }

    const Support truth = computeSupport(test, keys, partition);

    Retrieval retrieval;
    double exhaustiveRecall = 0.0;
    double exhaustiveNdcg = 0.0;
    std::size_t judged = 0;
    if (realEvaluation && !qrels.empty()) {
        std::unordered_map<std::string, std::uint32_t> corpusIndex;
        for (std::size_t i = 0; i < docs.size(); ++i) {
            corpusIndex.emplace(docs[i].id, static_cast<std::uint32_t>(i));
        }
        retrieval.ranked.resize(test.rows);
        retrieval.relevant.resize(test.rows);
        std::vector<float> scores(docs.size());
        for (std::size_t q = 0; q < test.rows; ++q) {
            for (std::size_t k = 0; k < docs.size(); ++k) {
                scores[k] = dot(test.row(q), keys.row(k), keys.dim);
            }
            auto& ranked = retrieval.ranked[q];
            ranked.resize(docs.size());
            std::iota(ranked.begin(), ranked.end(), 0U);
            std::ranges::sort(ranked, [&](std::uint32_t a, std::uint32_t b) {
                return scores[a] != scores[b] ? scores[a] > scores[b] : a < b;
            });
            if (const auto it = qrels.find(realTestIds[q]); it != qrels.end()) {
                for (const auto& [docId, grade] : it->second) {
                    if (const auto doc = corpusIndex.find(docId); doc != corpusIndex.end()) {
                        retrieval.relevant[q].emplace_back(doc->second, grade);
                    }
                }
            }
            if (!retrieval.relevant[q].empty()) {
                double recall = 0.0;
                double ndcg = 0.0;
                scoreTopTen(
                    ranked, retrieval.relevant[q], [](std::uint32_t) { return true; }, recall,
                    ndcg);
                exhaustiveRecall += recall;
                exhaustiveNdcg += ndcg;
                ++judged;
            }
        }
        if (judged > 0) {
            exhaustiveRecall /= static_cast<double>(judged);
            exhaustiveNdcg /= static_cast<double>(judged);
            gRetrieval = &retrieval;
        }
    }
    const Matrix testCentroid = centroidScores(test, partition.centroids);

    // The production router's own ranking of every cluster. Its route score is the dense
    // signal plus a query-independent persistence bonus, so it can differ from pure cosine.
    const auto productionScores = [&](const yams::topology::TopologyArtifactBatch& batch,
                                      std::size_t maxRepresentatives) {
        Matrix scores;
        scores.rows = test.rows;
        scores.dim = batch.clusters.size();
        scores.data.assign(scores.rows * scores.dim, -1.0e9F);
        std::unordered_map<std::string, std::uint32_t> clusterIndex;
        for (std::size_t c = 0; c < batch.clusters.size(); ++c) {
            clusterIndex.emplace(batch.clusters[c].clusterId, static_cast<std::uint32_t>(c));
        }
        const yams::topology::SparseGuidedClusterRouter router;
        const auto index = yams::topology::SparseGuidedClusterRouter::buildRouteIndex(
            batch, /*buildDenseAnnIndex=*/false, /*buildBqIndex=*/false);
        for (std::size_t q = 0; q < test.rows; ++q) {
            yams::topology::TopologyRouteRequest request;
            request.queryEmbedding.assign(test.row(q), test.row(q) + test.dim);
            request.sparseDenseAlpha = 0.0F;
            request.limit = batch.clusters.size();
            request.maxRoutingRepresentatives = maxRepresentatives;
            auto routes = router.route(request, batch, index);
            if (!routes) {
                continue;
            }
            for (const auto& route : routes.value()) {
                if (const auto it = clusterIndex.find(route.clusterId); it != clusterIndex.end()) {
                    scores.row(q)[it->second] = static_cast<float>(route.routeScore);
                }
            }
        }
        return scores;
    };
    const auto topAgreement = [&](const Matrix& a, const Matrix& b) {
        std::size_t agreed = 0;
        for (std::size_t q = 0; q < a.rows; ++q) {
            const auto top = [&](const Matrix& m) {
                return std::max_element(m.row(q), m.row(q) + m.dim) - m.row(q);
            };
            agreed += top(a) == top(b) ? 1 : 0;
        }
        return agreed;
    };
    const Matrix productionCentroid = productionScores(artifacts, 1);
    const std::size_t parityChecked = test.rows;
    const std::size_t parityAgreed = topAgreement(productionCentroid, testCentroid);

    std::vector<Curve> curves;
    curves.push_back(evaluate("centroid", "none", 1, testCentroid, truth, partition, docs.size()));
    curves.push_back(
        evaluate("prod_router", "none", 1, productionCentroid, truth, partition, docs.size()));
    for (const std::size_t r : {2, 4, 8}) {
        curves.push_back(
            evaluate("farthest_" + std::to_string(r), "none", r,
                     scoreMaxOver(test, partition.centroids, keys, partition.farthest, r - 1),
                     truth, partition, docs.size()));
    }

    std::size_t trainGeneratedRows = 0;
    std::size_t fittedParityChecked = 0;
    std::size_t fittedParityAgreed = 0;
    const auto fitAndEvaluate = [&](const Matrix& train, const std::string& label, bool deep) {
        const Support trainSupport = computeSupport(train, keys, partition);

        // Members that are most often the exact best match, then the farthest-first cover.
        std::vector<std::uint32_t> wins(docs.size(), 0);
        for (const auto best : trainSupport.bestDoc) {
            ++wins[best];
        }
        std::vector<std::vector<std::uint32_t>> hot(partition.clusters);
        for (std::uint32_t k = 0; k < docs.size(); ++k) {
            if (wins[k] > 0) {
                hot[partition.clusterOfDoc[k]].push_back(k);
            }
        }
        for (std::size_t c = 0; c < partition.clusters; ++c) {
            std::ranges::sort(hot[c], [&](std::uint32_t a, std::uint32_t b) {
                return wins[a] != wins[b] ? wins[a] > wins[b] : a < b;
            });
            for (const auto member : partition.farthest[c]) {
                if (std::ranges::find(hot[c], member) == hot[c].end()) {
                    hot[c].push_back(member);
                }
            }
        }
        if (deep) {
            curves.push_back(evaluate("hot_8", label, 8,
                                      scoreMaxOver(test, partition.centroids, keys, hot, 7), truth,
                                      partition, docs.size()));
        }

        const Matrix fitQueries = splitRows(train, false);
        const Matrix holdoutQueries = splitRows(train, true);
        const Support fitSupport = splitSupport(trainSupport, false);
        const Support holdoutSupport = splitSupport(trainSupport, true);

        // Ridge correction from the production fitting code, which chooses its strength on the
        // same held-out training queries and keeps the centroids when it cannot beat them.
        Matrix bestRouted = partition.centroids;
        std::string ridgeName = "ridge(kept)";
        {
            const yams::topology::RouteFitInput fitInput{.dim = keys.dim,
                                                         .keys = keys.data,
                                                         .clusterOfKey = partition.clusterOfDoc,
                                                         .centroids = partition.centroids.data,
                                                         .queries = train.data};
            auto fitted = yams::topology::fitRouteVectors(fitInput);
            if (!fitted) {
                std::cerr << "route fit failed: " << fitted.error().message << "\n";
                return;
            }
            if (fitted.value().adopted) {
                bestRouted.data = std::move(fitted.value().routeVectors);
                char name[32];
                std::snprintf(name, sizeof(name), "ridge(%g)", fitted.value().chosenStrength);
                ridgeName = name;
            }
        }
        const Matrix fittedScores = centroidScores(test, bestRouted);
        curves.push_back(
            evaluate(ridgeName, label, 1, fittedScores, truth, partition, docs.size()));

        if (label == "lead") {
            // The same vectors, ranked by the production router.
            auto fittedArtifacts = artifacts;
            for (std::size_t c = 0; c < fittedArtifacts.clusters.size(); ++c) {
                fittedArtifacts.clusters[c].fittedRouteVector.assign(bestRouted.row(c),
                                                                     bestRouted.row(c) + keys.dim);
            }
            const Matrix productionFitted = productionScores(fittedArtifacts, 1);
            fittedParityChecked = test.rows;
            fittedParityAgreed = topAgreement(productionFitted, fittedScores);
            curves.push_back(
                evaluate("prod_fitted", label, 1, productionFitted, truth, partition, docs.size()));
        }

        if (deep) {
            for (const std::size_t pieces : {4, 8}) {
                MaxAffine model;
                model.pieces = pieces;
                model.vectors.rows = partition.clusters * pieces;
                model.vectors.dim = keys.dim;
                model.vectors.data.assign(model.vectors.rows * keys.dim, 0.0F);
                for (std::size_t c = 0; c < partition.clusters; ++c) {
                    // Piece 0 starts at the ridge solution, the rest at the hot members; a
                    // cluster with too few members repeats its first piece.
                    std::copy(bestRouted.row(c), bestRouted.row(c) + keys.dim,
                              model.vectors.row(c * pieces));
                    for (std::size_t r = 1; r < pieces; ++r) {
                        const float* source =
                            r - 1 < hot[c].size() ? keys.row(hot[c][r - 1]) : bestRouted.row(c);
                        std::copy(source, source + keys.dim, model.vectors.row(c * pieces + r));
                    }
                }
                model = fitMaxAffine(std::move(model), fitQueries, fitSupport, holdoutQueries,
                                     holdoutSupport, partition);
                curves.push_back(evaluate("maxaff_" + std::to_string(pieces), label, pieces,
                                          scoreMaxAffine(test, model), truth, partition,
                                          docs.size()));
            }
        }
    };

    const std::pair<QueryStyle, const char*> styles[] = {
        {QueryStyle::Lead, "lead"},   {QueryStyle::Span12, "span12"},
        {QueryStyle::Span6, "span6"}, {QueryStyle::Keywords, "keywords"},
        {QueryStyle::Mix, "mix"},
    };
    for (const auto& [style, label] : styles) {
        Matrix train;
        if (!embed(*backend, generateQueries(docs, style, options.maxTrain, 0x5EEDULL), train)) {
            std::cerr << "embedding failed\n";
            return 2;
        }
        if (style == QueryStyle::Mix) {
            trainGeneratedRows = train.rows;
        }
        fitAndEvaluate(train, label, style == QueryStyle::Mix && partition.clusters <= 512);
    }
    if (realEvaluation && trainReal.rows > 0) {
        fitAndEvaluate(trainReal, "real", partition.clusters <= 512);
    }

    curves.push_back(evaluate("oracle", "none", 0, truth.values, truth, partition, docs.size()));

    std::printf("dataset=%s engine=%s docs=%zu clusters=%zu dim=%zu evaluation=%s test_queries=%zu "
                "train_mix=%zu train_real=%zu space=%s\n",
                options.dataset.filename().string().c_str(), options.engine.c_str(), docs.size(),
                partition.clusters, keys.dim, realEvaluation ? "real" : "generated", test.rows,
                trainGeneratedRows, trainReal.rows, backend->getEmbeddingSpaceIdentity().c_str());
    std::printf("production router top-1 agreement with the centroid arm: %zu/%zu\n", parityAgreed,
                parityChecked);
    std::printf("production router top-1 agreement with the fitted arm (lead): %zu/%zu\n",
                fittedParityAgreed, fittedParityChecked);
    std::printf(
        "%-12s %-9s %3s | recall%% at probes 1 2 3 5 8 16 32 64               | recall%% at scan "
        "1%% 2%% 5%% 10%% 20%%   | mean rank\n",
        "arm", "training", "vec");
    for (const auto& curve : curves) {
        print(curve);
    }
    if (gRetrieval != nullptr) {
        std::printf("\nretrieval against relevance judgments (%zu judged queries), searching only "
                    "the top-s clusters\nexhaustive search of this embedding: recall@10 %.1f "
                    "nDCG@10 %.1f\n",
                    judged, 100.0 * exhaustiveRecall, 100.0 * exhaustiveNdcg);
        std::printf(
            "%-12s %-9s %3s | recall@10 %% at probes 1 2 3 5 8 16 32 64            | nDCG@10 %% at "
            "probes 1 2 3 5 8 16 32 64\n",
            "arm", "training", "vec");
        for (const auto& c : curves) {
            std::printf("%-12s %-9s %3zu |", c.arm.c_str(), c.training.c_str(),
                        c.vectorsPerCluster);
            for (std::size_t p = 0; p < std::size(kProbes); ++p) {
                std::printf(" %5.1f", 100.0 * c.recall10AtProbe[p]);
            }
            std::printf(" |");
            for (std::size_t p = 0; p < std::size(kProbes); ++p) {
                std::printf(" %5.1f", 100.0 * c.ndcg10AtProbe[p]);
            }
            std::printf("\n");
        }
    }

    if (!options.out.empty()) {
        std::ofstream out(options.out, std::ios::app);
        if (!out) {
            std::cerr << "cannot write results to " << options.out << "\n";
            return 1;
        }
        for (const auto& c : curves) {
            json row;
            row["dataset"] = options.dataset.filename().string();
            row["docs"] = docs.size();
            row["clusters"] = partition.clusters;
            row["engine"] = options.engine;
            row["dim"] = keys.dim;
            row["test_queries"] = test.rows;
            row["evaluation"] = realEvaluation ? "real" : "generated";
            row["arm"] = c.arm;
            row["training"] = c.training;
            row["vectors_per_cluster"] = c.vectorsPerCluster;
            row["mean_rank_of_true_cluster"] = c.meanRankOfTrueCluster;
            for (std::size_t i = 0; i < std::size(kProbes); ++i) {
                row["recall_at_probes"][std::to_string(kProbes[i])] = c.recallAtProbe[i];
                row["scan_fraction_at_probes"][std::to_string(kProbes[i])] = c.scanAtProbe[i];
                if (c.judgedQueries > 0) {
                    row["recall10_at_probes"][std::to_string(kProbes[i])] = c.recall10AtProbe[i];
                    row["ndcg10_at_probes"][std::to_string(kProbes[i])] = c.ndcg10AtProbe[i];
                }
            }
            for (std::size_t i = 0; i < std::size(kBudgets); ++i) {
                row["recall_at_scan_budget"][std::to_string(kBudgets[i])] = c.recallAtBudget[i];
            }
            out << row.dump() << "\n";
        }
        out.flush();
        if (!out) {
            std::cerr << "failed while writing results to " << options.out << "\n";
            return 1;
        }
    }
    return 0;
}
