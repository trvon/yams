// metapath_ab_bench: SearchEngine-level meta-path A/B cells on scifact.
//
// Cells (differ ONLY in the post-fusion meta-path boost config):
//   A. enableMetaPathRouting=false                         (floor: pure dense ranking)
//   B. =true, knobs off                                    (binary Phase P: M_sem, w_sem=1)
//   C. B + metaPathUseEdgeWeights=true + metaPathMinSeedSimilarity=0.3
//   D. C + metaPathReciprocalOnly=true
//
// Approach (per task option #3): build doc-level simeon embeddings + a
// semantic_neighbor KG in-process, then evaluate base dense ranking vs
// base + computeMetaPathBoosts(...) reranking, applying the exact post-fusion
// formula from SearchEngine::Impl::searchInternal:
//     factor = 1 + metaPathBoostAlpha * docBoost[hash]; score *= factor;
// This avoids standing up a full SearchEngine and matches what Phase P measured.

#include "tests/benchmarks/beir_loader.h"
#include "tests/benchmarks/search/retrieval_benchmark_support.h"

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/meta_path_router.h>
#include <yams/search/search_engine_config.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace yams;

namespace {

constexpr int kMetricK = 10;
// EmbeddingService semantic graph default (src/daemon/components/EmbeddingService.cpp:341-349:
// YAMS_GRAPH_SEMANTIC_TOPK defaults to 4). NOT metaPathSeedK (=8), which is the seed count.
constexpr std::size_t kEdgeTopK = 4;

struct Cell {
    std::string id;
    std::string label;
    search::SearchEngineConfig config;
};

float dotCosine(const std::vector<float>& a, const std::vector<float>& b) {
    // simeon backend L2-normalizes outputs, so dot == cosine.
    float s = 0.0f;
    const std::size_t n = std::min(a.size(), b.size());
    for (std::size_t i = 0; i < n; ++i)
        s += a[i] * b[i];
    return s;
}

fs::path scifactDir() {
    const char* home = std::getenv("HOME");
    fs::path base = home ? fs::path(home) / ".cache" / "yams" / "benchmarks" / "scifact"
                         : fs::path("scifact");
    return base;
}

} // namespace

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::warn);

    fs::path cacheDir = scifactDir();
    if (argc > 1)
        cacheDir = argv[1];

    auto dsRes = yams::bench::loadBEIRDataset("scifact", cacheDir);
    if (!dsRes) {
        std::cerr << "Failed to load scifact: " << dsRes.error().message << "\n";
        return 1;
    }
    const auto& ds = dsRes.value();
    std::cout << "Loaded scifact: " << ds.documents.size() << " docs, " << ds.queries.size()
              << " queries, " << ds.qrels.size() << " qrels\n";

    // --- Build test queries from qrels (doc-id mode) ---
    std::map<std::string, yams::bench::TestQuery> queryMap;
    for (const auto& [qid, docScore] : ds.qrels) {
        const auto& [docId, score] = docScore;
        if (score <= 0)
            continue;
        auto qit = ds.queries.find(qid);
        if (qit == ds.queries.end())
            continue;
        auto& tq = queryMap[qid];
        tq.query = qit->second.text;
        tq.useDocIds = true;
        tq.relevantDocIds.insert(docId);
        tq.relevanceGrades[docId] = score;
    }
    std::cout << "Evaluable queries: " << queryMap.size() << "\n";

    // --- Embedding generator (simeon, in-process, deterministic) ---
    vector::EmbeddingConfig ecfg;
    ecfg.backend = vector::EmbeddingConfig::Backend::Simeon;
    ecfg.embedding_dim = 768;
    ecfg.normalize_embeddings = true;
    auto gen = std::make_shared<vector::EmbeddingGenerator>(ecfg);
    if (!gen->initialize()) {
        std::cerr << "Failed to initialize simeon embedding generator\n";
        return 1;
    }
    const std::size_t dim = gen->getEmbeddingDimension();
    std::cout << "Simeon embedding dim: " << dim << "\n";

    // --- Embed corpus (doc-level) ---
    std::vector<std::string> docIds;
    std::vector<std::vector<float>> docEmb;
    docIds.reserve(ds.documents.size());
    docEmb.reserve(ds.documents.size());
    {
        std::vector<std::string> texts;
        texts.reserve(ds.documents.size());
        for (const auto& [id, doc] : ds.documents) {
            docIds.push_back(id);
            std::string t = doc.title;
            if (!t.empty() && !doc.text.empty())
                t += "\n";
            t += doc.text;
            texts.push_back(std::move(t));
        }
        std::cout << "Embedding " << texts.size() << " docs...\n";
        docEmb = gen->generateEmbeddings(texts);
        if (docEmb.size() != docIds.size()) {
            std::cerr << "Embedding count mismatch\n";
            return 1;
        }
    }
    std::unordered_map<std::string, std::size_t> idIndex;
    for (std::size_t i = 0; i < docIds.size(); ++i)
        idIndex[docIds[i]] = i;

    // --- Temp dir + vector database (Vec0L2 exact engine) ---
    fs::path tmp = fs::temp_directory_path() /
                   ("metapath_ab_" + std::to_string(::getpid()));
    fs::create_directories(tmp);

    vector::VectorDatabaseConfig vcfg;
    vcfg.database_path = (tmp / "vectors.db").string();
    vcfg.embedding_dim = dim;
    vcfg.search_engine = vector::VectorSearchEngine::Vec0L2; // exact, no PQ training
    auto vectorDb = std::shared_ptr<vector::VectorDatabase>(
        vector::createVectorDatabase(vcfg).release());
    if (!vectorDb || !vectorDb->initialize()) {
        std::cerr << "Failed to initialize vector database\n";
        return 1;
    }
    {
        std::vector<vector::VectorRecord> recs;
        recs.reserve(docIds.size());
        for (std::size_t i = 0; i < docIds.size(); ++i) {
            vector::VectorRecord r;
            r.chunk_id = docIds[i] + "_doc";
            r.document_hash = docIds[i];
            r.embedding = docEmb[i];
            r.embedding_dim = dim;
            r.level = vector::EmbeddingLevel::DOCUMENT;
            r.model_id = "simeon";
            recs.push_back(std::move(r));
        }
        if (!vectorDb->insertVectorsBatch(recs)) {
            std::cerr << "Failed to insert vectors\n";
            return 1;
        }
    }
    std::cout << "Indexed " << docIds.size() << " doc-level vectors\n";

    // --- KG store: doc nodes + semantic_neighbor kNN edges ---
    auto kgRes = metadata::makeSqliteKnowledgeGraphStore((tmp / "kg.db").string(), {});
    if (!kgRes) {
        std::cerr << "Failed to create KG store: " << kgRes.error().message << "\n";
        return 1;
    }
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore(std::move(kgRes.value()));

    std::vector<std::int64_t> nodeId(docIds.size(), 0);
    {
        std::vector<metadata::KGNode> nodes;
        nodes.reserve(docIds.size());
        for (const auto& id : docIds) {
            metadata::KGNode n;
            n.nodeKey = "doc:" + id;
            n.type = std::string("document");
            nodes.push_back(std::move(n));
        }
        auto ids = kgStore->upsertNodes(nodes);
        if (!ids) {
            std::cerr << "upsertNodes failed: " << ids.error().message << "\n";
            return 1;
        }
        nodeId = ids.value();
    }

    // kNN edges: for each doc keep top kEdgeTopK neighbors by cosine, both directions
    // (mirrors EmbeddingService.cpp:722-766 / 978-1029: relation=semantic_neighbor,
    //  weight=clamp(cosine,thr,1), bidirectional reverse edge, unique=true; adaptive
    //  threshold = sim of the kth kept neighbor).
    std::cout << "Building semantic_neighbor kNN graph (k=" << kEdgeTopK << ")...\n";
    {
        std::vector<metadata::KGEdge> edges;
        edges.reserve(docIds.size() * kEdgeTopK * 2);
        const std::size_t N = docIds.size();
        std::vector<std::pair<float, std::size_t>> sims;
        for (std::size_t i = 0; i < N; ++i) {
            sims.clear();
            for (std::size_t j = 0; j < N; ++j) {
                if (j == i)
                    continue;
                float s = dotCosine(docEmb[i], docEmb[j]);
                if (s <= 0.0f)
                    continue;
                sims.emplace_back(s, j);
            }
            const std::size_t keep = std::min<std::size_t>(kEdgeTopK, sims.size());
            if (keep == 0)
                continue;
            std::partial_sort(sims.begin(), sims.begin() + keep, sims.end(),
                              [](const auto& a, const auto& b) { return a.first > b.first; });
            const float thr = sims[keep - 1].first;
            for (std::size_t r = 0; r < keep; ++r) {
                const float w = std::clamp(sims[r].first, thr, 1.0f);
                const std::size_t j = sims[r].second;
                metadata::KGEdge e;
                e.srcNodeId = nodeId[i];
                e.dstNodeId = nodeId[j];
                e.relation = "semantic_neighbor";
                e.weight = w;
                edges.push_back(e);
                metadata::KGEdge rev;
                rev.srcNodeId = nodeId[j];
                rev.dstNodeId = nodeId[i];
                rev.relation = "semantic_neighbor";
                rev.weight = w;
                edges.push_back(rev);
            }
        }
        if (auto er = kgStore->addEdgesUnique(edges); !er) {
            std::cerr << "addEdgesUnique failed: " << er.error().message << "\n";
            return 1;
        }
        std::cout << "Inserted " << edges.size() << " directed semantic_neighbor edges\n";
    }

    // --- Cell configs ---
    // Seed-search cosine floor: simeon char/word n-gram embeddings top out ~0.23
    // query-doc cosine, so the production default (0.7) yields zero seeds. This is a
    // fixed harness constant matched to the embedding backend's scale, NOT one of the
    // A/B knobs — it is held identical across B/C/D so the knobs are isolated.
    constexpr float kSeedFloor = 0.05f;
    auto makeBase = [&]() {
        search::SearchEngineConfig c;
        c.enableMetaPathRouting = true; // B/C/D
        c.metaPathSeedSimilarity = kSeedFloor;
        // Only semantic_neighbor edges exist; M_call/M_def/M_entity/M_blob walks no-op.
        return c;
    };
    std::vector<Cell> cells;
    {
        search::SearchEngineConfig a;
        a.enableMetaPathRouting = false;
        cells.push_back({"A", "floor (routing off)", a});

        auto b = makeBase();
        cells.push_back({"B", "binary Phase P (knobs off)", b});

        auto c = makeBase();
        c.metaPathUseEdgeWeights = true;
        c.metaPathMinSeedSimilarity = 0.3f;
        cells.push_back({"C", "edgeWeights + minSeedSim=0.3", c});

        auto d = makeBase();
        d.metaPathUseEdgeWeights = true;
        d.metaPathMinSeedSimilarity = 0.3f;
        d.metaPathReciprocalOnly = true;
        cells.push_back({"D", "C + reciprocalOnly", d});
    }

    struct Accum {
        double ndcg = 0.0;
        double recall = 0.0;
        double boostedDocs = 0.0; // mean per query
        double seedDocs = 0.0;
        std::size_t queries = 0;
    };
    std::vector<Accum> acc(cells.size());

    // --- Diagnostic: probe seed search behavior on first query ---
    {
        const auto& firstQ = queryMap.begin()->second;
        auto qe = gen->generateEmbedding(firstQ.query);
        float topCos = -2.0f;
        std::string topId;
        for (std::size_t i = 0; i < docIds.size(); ++i) {
            float s = dotCosine(qe, docEmb[i]);
            if (s > topCos) { topCos = s; topId = docIds[i]; }
        }
        vector::VectorSearchParams p0;
        p0.k = 32;
        p0.similarity_threshold = 0.0f;
        auto recs0 = vectorDb->search(qe, p0);
        std::size_t docLvl = 0;
        float bestScore = -2.0f;
        for (const auto& r : recs0) {
            if (r.level == vector::EmbeddingLevel::DOCUMENT) {
                ++docLvl;
                bestScore = std::max(bestScore, r.relevance_score);
            }
        }
        auto nodeProbe = kgStore->getNodeByKey("doc:" + topId);
        std::cout << "[diag] top corpus cosine=" << topCos << " (doc " << topId << ")"
                  << " | search@thr0 returned " << recs0.size() << " recs, " << docLvl
                  << " DOCUMENT, bestScore=" << bestScore
                  << " | getNodeByKey(doc:" << topId << ")="
                  << ((nodeProbe && nodeProbe.value().has_value()) ? "OK" : "MISS") << "\n";
        vector::VectorSearchParams p7; // router's effective default threshold
        p7.k = 32;
        auto recs7 = vectorDb->search(qe, p7);
        std::cout << "[diag] search@thr0.7 (router default) returned " << recs7.size() << " recs\n";
    }

    std::cout << "Evaluating " << queryMap.size() << " queries x " << cells.size() << " cells...\n";
    std::size_t qn = 0;
    for (const auto& [qid, tq] : queryMap) {
        auto qEmb = gen->generateEmbedding(tq.query);
        if (qEmb.size() != dim)
            continue;

        // Base dense ranking (shared by all cells).
        std::vector<std::pair<float, std::size_t>> base;
        base.reserve(docIds.size());
        for (std::size_t i = 0; i < docIds.size(); ++i)
            base.emplace_back(dotCosine(qEmb, docEmb[i]), i);
        std::sort(base.begin(), base.end(),
                  [](const auto& a, const auto& b) { return a.first > b.first; });

        for (std::size_t ci = 0; ci < cells.size(); ++ci) {
            const auto& cell = cells[ci];
            auto mp = search::computeMetaPathBoosts(qEmb, vectorDb, kgStore, cell.config);

            std::vector<std::pair<float, std::size_t>> ranked = base;
            std::size_t boosted = 0;
            if (!mp.docBoost.empty()) {
                for (auto& [score, idx] : ranked) {
                    auto it = mp.docBoost.find(docIds[idx]);
                    if (it == mp.docBoost.end())
                        continue;
                    const float factor = 1.0f + cell.config.metaPathBoostAlpha * it->second;
                    if (factor != 1.0f) {
                        score *= factor;
                        ++boosted;
                    }
                }
                std::stable_sort(ranked.begin(), ranked.end(),
                                 [](const auto& a, const auto& b) { return a.first > b.first; });
            }

            std::vector<std::string> rankedIds;
            rankedIds.reserve(std::min<std::size_t>(ranked.size(), 64));
            for (std::size_t i = 0; i < ranked.size() && i < 64; ++i)
                rankedIds.push_back(docIds[ranked[i].second]);

            auto sample = yams::bench::scoreRankedDocIds(tq, rankedIds, kMetricK);
            acc[ci].ndcg += sample.ndcgAtK;
            acc[ci].recall += sample.recallAtK;
            acc[ci].boostedDocs += static_cast<double>(boosted);
            acc[ci].seedDocs += static_cast<double>(mp.seedDocCount);
            acc[ci].queries++;
        }
        if (++qn % 100 == 0)
            std::cout << "  ..." << qn << " queries\n";
    }

    // --- Report ---
    std::cout << "\n=== meta-path A/B on scifact (nDCG@10 / Recall@10, " << queryMap.size()
              << " queries, edge k=" << kEdgeTopK << ") ===\n";
    std::cout << std::left << std::setw(4) << "Cell" << std::setw(34) << "Config" << std::right
              << std::setw(10) << "nDCG@10" << std::setw(11) << "Recall@10" << std::setw(11)
              << "boost/q" << std::setw(9) << "seed/q" << "\n";
    double floorNdcg = 0.0;
    json out = json::array();
    for (std::size_t ci = 0; ci < cells.size(); ++ci) {
        const auto& a = acc[ci];
        const double q = a.queries ? static_cast<double>(a.queries) : 1.0;
        const double ndcg = a.ndcg / q;
        const double recall = a.recall / q;
        const double bpq = a.boostedDocs / q;
        const double spq = a.seedDocs / q;
        if (ci == 0)
            floorNdcg = ndcg;
        std::cout << std::left << std::setw(4) << cells[ci].id << std::setw(34) << cells[ci].label
                  << std::right << std::fixed << std::setprecision(4) << std::setw(10) << ndcg
                  << std::setw(11) << recall << std::setprecision(2) << std::setw(11) << bpq
                  << std::setw(9) << spq << "  (Δ=" << std::showpos << std::setprecision(4)
                  << (ndcg - floorNdcg) << std::noshowpos << ")\n";
        out.push_back({{"cell", cells[ci].id},
                       {"label", cells[ci].label},
                       {"dataset", "scifact"},
                       {"queries", a.queries},
                       {"edge_topk", kEdgeTopK},
                       {"ndcg_at_10", ndcg},
                       {"recall_at_10", recall},
                       {"ndcg_delta_vs_floor", ndcg - floorNdcg},
                       {"mean_boosted_docs", bpq},
                       {"mean_seed_docs", spq}});
    }

    fs::path outDir = "bench_results";
    fs::create_directories(outDir);
    fs::path outPath = outDir / "metapath_ab_scifact.jsonl";
    std::ofstream os(outPath);
    for (const auto& row : out)
        os << row.dump() << "\n";
    os.close();
    std::cout << "\nWrote " << outPath.string() << "\n";

    vectorDb.reset();
    kgStore.reset();
    std::error_code ec;
    fs::remove_all(tmp, ec);
    return 0;
}
