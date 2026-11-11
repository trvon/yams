// Topology routing-recall bench.
//
// Question: when a query is routed to a few clusters, how often is its true best document
// inside them, and how much of that is lost to the way clusters are scored?
//
// The corpus is partitioned once with the k-means topology engine. Every router below then
// ranks the same clusters for the same held-out queries, so differences come from cluster
// scoring alone:
//
//   centroid        cosine to the cluster centroid (the production dense route, one
//                   representative per cluster)
//   farthest_R      max over the centroid and R-1 farthest-first member embeddings (the
//                   production multi-representative cover)
//   hot_R           max over the centroid and the R-1 members that are most often the exact
//                   best match for the training queries
//   ridge_L         centroid plus a per-cluster correction vector fitted by ridge regression
//                   to the cluster's support function h_c(q) = max over members of <q, key>
//                   on the training queries (a one-layer form of a learned support-function
//                   router; L is the regularization strength)
//   oracle          h_c(q) itself: the best any cluster scorer can do on this partition
//
// Training queries come either from the corpus (title, first sentence, a word span: the
// "generated queries" setting, needing no query log) or from the even-indexed real queries.
// Evaluation always uses the odd-indexed real queries, which no router is fitted on.
//
// Recall here is routing recall of the exact top-1 document under the embedding, not
// relevance: it isolates the router from embedding quality.
//
// Run (BEIR layout: corpus.jsonl, queries.jsonl):
//   topology_routing_recall_bench --dataset build/benchmarks/data/scifact \
//       [--clusters 0] [--max-docs 0] [--max-train 3000] [--out routing_recall.jsonl]

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
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

std::vector<std::string> loadQueries(const fs::path& path) {
    std::vector<std::string> queries;
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
            queries.push_back(std::move(text));
        }
    }
    return queries;
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

// Queries synthesized from the corpus alone: title, first sentence, and one 12-word span.
std::vector<std::string> generateQueries(const std::vector<Document>& docs, std::size_t limit) {
    std::vector<std::string> all;
    std::mt19937_64 rng(0x5eedULL);
    for (const auto& doc : docs) {
        if (!doc.title.empty()) {
            all.push_back(doc.title);
        }
        if (const auto end = doc.text.find(". "); end != std::string::npos && end > 20) {
            all.push_back(doc.text.substr(0, end));
        }
        const auto words = splitWords(doc.text);
        if (words.size() > 24) {
            const std::size_t start = rng() % (words.size() - 12);
            std::string span;
            for (std::size_t w = start; w < start + 12; ++w) {
                if (!span.empty()) {
                    span.push_back(' ');
                }
                span.append(words[w]);
            }
            all.push_back(std::move(span));
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

constexpr std::size_t kProbes[] = {1, 2, 3, 5, 8, 16};
constexpr double kBudgets[] = {0.01, 0.02, 0.05, 0.10, 0.20};

struct Curve {
    std::string arm;
    std::string training;
    std::size_t vectorsPerCluster{1};
    double recallAtProbe[std::size(kProbes)] = {};
    double scanAtProbe[std::size(kProbes)] = {};
    double recallAtBudget[std::size(kBudgets)] = {};
    double meanRankOfTrueCluster{0.0};
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

// Solve (QtQ + lambda I) X = Qt R for X (dim x clusters) by Cholesky; returns row-major
// clusters x dim correction vectors. R is the residual h_c(q) - <q, centroid_c>.
Matrix fitRidgeCorrections(const Matrix& trainQueries, const Matrix& residuals, double lambda) {
    const std::size_t d = trainQueries.dim;
    const std::size_t k = residuals.dim;
    std::vector<double> gram(d * d, 0.0);
    for (std::size_t n = 0; n < trainQueries.rows; ++n) {
        const float* q = trainQueries.row(n);
        for (std::size_t i = 0; i < d; ++i) {
            const double qi = q[i];
            double* gramRow = gram.data() + i * d;
            for (std::size_t j = 0; j <= i; ++j) {
                gramRow[j] += qi * static_cast<double>(q[j]);
            }
        }
    }
    double meanDiagonal = 0.0;
    for (std::size_t i = 0; i < d; ++i) {
        meanDiagonal += gram[i * d + i];
    }
    meanDiagonal /= static_cast<double>(d);
    for (std::size_t i = 0; i < d; ++i) {
        gram[i * d + i] += lambda * meanDiagonal;
    }
    // In-place lower Cholesky.
    for (std::size_t i = 0; i < d; ++i) {
        for (std::size_t j = 0; j <= i; ++j) {
            double sum = gram[i * d + j];
            for (std::size_t p = 0; p < j; ++p) {
                sum -= gram[i * d + p] * gram[j * d + p];
            }
            gram[i * d + j] = i == j ? std::sqrt(std::max(sum, 1e-12)) : sum / gram[j * d + j];
        }
    }
    Matrix corrections;
    corrections.rows = k;
    corrections.dim = d;
    corrections.data.assign(k * d, 0.0F);
    std::vector<double> rhs(d);
    for (std::size_t c = 0; c < k; ++c) {
        std::fill(rhs.begin(), rhs.end(), 0.0);
        for (std::size_t n = 0; n < trainQueries.rows; ++n) {
            const double r = residuals.row(n)[c];
            const float* q = trainQueries.row(n);
            for (std::size_t i = 0; i < d; ++i) {
                rhs[i] += r * static_cast<double>(q[i]);
            }
        }
        for (std::size_t i = 0; i < d; ++i) {
            double sum = rhs[i];
            for (std::size_t p = 0; p < i; ++p) {
                sum -= gram[i * d + p] * rhs[p];
            }
            rhs[i] = sum / gram[i * d + i];
        }
        for (std::size_t i = d; i-- > 0;) {
            double sum = rhs[i];
            for (std::size_t p = i + 1; p < d; ++p) {
                sum -= gram[p * d + i] * rhs[p];
            }
            rhs[i] = sum / gram[i * d + i];
        }
        for (std::size_t i = 0; i < d; ++i) {
            corrections.row(c)[i] = static_cast<float>(rhs[i]);
        }
    }
    return corrections;
}

Matrix centroidScores(const Matrix& queries, const Matrix& centroids) {
    return scoreMaxOver(queries, centroids, centroids,
                        {centroids.rows, std::vector<std::uint32_t>{}}, 0);
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
                     "[--max-docs N] [--max-train N] [--out file.jsonl]\n";
        return 2;
    }

    const auto docs = loadCorpus(options.dataset / "corpus.jsonl", options.maxDocs);
    const auto realQueries = loadQueries(options.dataset / "queries.jsonl");
    if (docs.size() < 16 || realQueries.size() < 4) {
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
        docTexts.push_back(doc.title.empty() ? doc.text : doc.title + "\n" + doc.text);
    }
    const auto generated = generateQueries(docs, options.maxTrain);

    Matrix keys;
    Matrix allReal;
    Matrix trainGenerated;
    if (!embed(*backend, docTexts, keys) || !embed(*backend, realQueries, allReal) ||
        !embed(*backend, generated, trainGenerated)) {
        std::cerr << "embedding failed\n";
        return 2;
    }
    const Matrix trainReal = selectRows(allReal, 0);
    const Matrix test = selectRows(allReal, 1);

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
    yams::topology::KMeansTopologyEngine engine;
    auto built = engine.buildArtifacts(inputs, buildConfig);
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
    const Matrix testCentroid = centroidScores(test, partition.centroids);

    // How often the production router agrees with this bench's centroid ranking on top-1.
    std::size_t parityChecked = 0;
    std::size_t parityAgreed = 0;
    {
        const yams::topology::SparseGuidedClusterRouter router;
        std::unordered_map<std::string, std::uint32_t> clusterIndex;
        for (std::size_t c = 0; c < artifacts.clusters.size(); ++c) {
            clusterIndex.emplace(artifacts.clusters[c].clusterId, static_cast<std::uint32_t>(c));
        }
        for (std::size_t q = 0; q < std::min<std::size_t>(test.rows, 64); ++q) {
            yams::topology::TopologyRouteRequest request;
            request.queryEmbedding.assign(test.row(q), test.row(q) + test.dim);
            request.sparseDenseAlpha = 0.0F;
            request.limit = 1;
            request.maxRoutingRepresentatives = 1;
            auto routes = router.route(request, artifacts);
            if (!routes || routes.value().empty()) {
                continue;
            }
            const float* row = testCentroid.row(q);
            const auto benchTop =
                static_cast<std::uint32_t>(std::max_element(row, row + partition.clusters) - row);
            ++parityChecked;
            const auto it = clusterIndex.find(routes.value().front().clusterId);
            parityAgreed += (it != clusterIndex.end() && it->second == benchTop) ? 1 : 0;
        }
    }

    std::vector<Curve> curves;
    curves.push_back(evaluate("centroid", "none", 1, testCentroid, truth, partition, docs.size()));
    for (const std::size_t r : {2, 4, 8}) {
        curves.push_back(
            evaluate("farthest_" + std::to_string(r), "none", r,
                     scoreMaxOver(test, partition.centroids, keys, partition.farthest, r - 1),
                     truth, partition, docs.size()));
    }

    const auto fitAndEvaluate = [&](const Matrix& train, const std::string& label) {
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
        for (const std::size_t r : {2, 4, 8}) {
            curves.push_back(evaluate("hot_" + std::to_string(r), label, r,
                                      scoreMaxOver(test, partition.centroids, keys, hot, r - 1),
                                      truth, partition, docs.size()));
        }

        const Matrix trainCentroid = centroidScores(train, partition.centroids);
        Matrix residuals = trainSupport.values;
        for (std::size_t i = 0; i < residuals.data.size(); ++i) {
            residuals.data[i] -= trainCentroid.data[i];
        }
        for (const double lambda : {0.1, 1.0, 10.0}) {
            Matrix routed = partition.centroids;
            const Matrix corrections = fitRidgeCorrections(train, residuals, lambda);
            for (std::size_t i = 0; i < routed.data.size(); ++i) {
                routed.data[i] += corrections.data[i];
            }
            char name[32];
            std::snprintf(name, sizeof(name), "ridge_%g", lambda);
            curves.push_back(evaluate(name, label, 1, centroidScores(test, routed), truth,
                                      partition, docs.size()));
        }
    };
    fitAndEvaluate(trainGenerated, "generated");
    fitAndEvaluate(trainReal, "real");

    curves.push_back(evaluate("oracle", "none", 0, truth.values, truth, partition, docs.size()));

    std::printf("dataset=%s docs=%zu clusters=%zu dim=%zu test_queries=%zu "
                "train_generated=%zu train_real=%zu space=%s\n",
                options.dataset.filename().string().c_str(), docs.size(), partition.clusters,
                keys.dim, test.rows, trainGenerated.rows, trainReal.rows,
                backend->getEmbeddingSpaceIdentity().c_str());
    std::printf("production router top-1 agreement with the centroid arm: %zu/%zu\n", parityAgreed,
                parityChecked);
    std::printf("%-12s %-9s %3s | recall%% at probes 1 2 3 5 8 16         | recall%% at scan "
                "1%% 2%% 5%% 10%% 20%%   | mean rank\n",
                "arm", "training", "vec");
    for (const auto& curve : curves) {
        print(curve);
    }

    if (!options.out.empty()) {
        std::ofstream out(options.out, std::ios::app);
        for (const auto& c : curves) {
            json row;
            row["dataset"] = options.dataset.filename().string();
            row["docs"] = docs.size();
            row["clusters"] = partition.clusters;
            row["dim"] = keys.dim;
            row["test_queries"] = test.rows;
            row["arm"] = c.arm;
            row["training"] = c.training;
            row["vectors_per_cluster"] = c.vectorsPerCluster;
            row["mean_rank_of_true_cluster"] = c.meanRankOfTrueCluster;
            for (std::size_t i = 0; i < std::size(kProbes); ++i) {
                row["recall_at_probes"][std::to_string(kProbes[i])] = c.recallAtProbe[i];
                row["scan_fraction_at_probes"][std::to_string(kProbes[i])] = c.scanAtProbe[i];
            }
            for (std::size_t i = 0; i < std::size(kBudgets); ++i) {
                row["recall_at_scan_budget"][std::to_string(kBudgets[i])] = c.recallAtBudget[i];
            }
            out << row.dump() << "\n";
        }
    }
    return 0;
}
