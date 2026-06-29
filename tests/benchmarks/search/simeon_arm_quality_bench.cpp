// simeon bandit-arm retrieval-quality bench — scores each SimeonLexicalBackend
// arm directly over a judged corpus and reports nDCG@k / Recall@k / MRR per arm.
// Deterministic (no bandit exploration), no live daemon. Decides whether the
// strategy-router arms (keyphrase / lead_field) and the promoted geom arm earn
// their place vs the SAB / RM3 baselines.
//
//   ./simeon_arm_quality_bench [corpus_root] [queries.json] [k]
//
// corpus_root default: the yams repo (the judged ground-truth is yams paths).
// Relevance is path-substring (mirrors live_relevance_ab.py).

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/metadata/metadata_repository.h>
#include <yams/search/simeon_lexical_backend.h>

namespace fs = std::filesystem;
using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

namespace {

struct Query {
    std::string text;
    std::vector<std::string> relevant; // path substrings
};

bool is_text_ext(const std::string& e) {
    static const std::set<std::string> ok = {".cpp", ".cc",  ".h",   ".hpp", ".c",    ".md",
                                             ".txt", ".py",  ".json", ".toml", ".build", ".cmake",
                                             ".sh",  ".rs",  ".ts",  ".js",  ".yml",  ".yaml"};
    return ok.count(e) > 0;
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in)
        return {};
    std::string s((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    return s;
}

// Index every text file under root/{src,include,tests,docs} into a temp repo.
struct Corpus {
    fs::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    std::vector<std::int64_t> docIds;
    std::vector<std::string> docPaths; // aligned with docIds
};

bool build_corpus(Corpus& c, const fs::path& root) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? fs::path(t) : fs::temp_directory_path();
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    c.dbPath = base / ("simeon_arm_bench_" + std::to_string(ts) + ".db");
    std::error_code ec;
    fs::remove(c.dbPath, ec);

    ConnectionPoolConfig pcfg;
    c.pool = std::make_unique<ConnectionPool>(c.dbPath.string(), pcfg);
    if (!c.pool->initialize().has_value())
        return false;
    c.repo = std::make_shared<MetadataRepository>(*c.pool);

    std::size_t n = 0;
    for (const char* sub : {"src", "include", "tests", "docs"}) {
        const fs::path dir = root / sub;
        if (!fs::exists(dir))
            continue;
        for (auto it = fs::recursive_directory_iterator(
                 dir, fs::directory_options::skip_permission_denied);
             it != fs::recursive_directory_iterator(); ++it) {
            const auto& p = it->path();
            // Skip build trees / vendored simeon to keep the corpus the judged scope.
            const std::string ps = p.string();
            if (ps.find("/build") != std::string::npos || ps.find("/.git") != std::string::npos)
                continue;
            if (!it->is_regular_file() || !is_text_ext(p.extension().string()))
                continue;
            std::error_code sec;
            if (fs::file_size(p, sec) > std::uintmax_t{512} * 1024 || sec)
                continue;
            std::string content = read_file(p);
            if (content.empty())
                continue;
            const std::string hash = "armbench" + std::to_string(n);
            DocumentInfo d;
            d.filePath = ps;
            d.fileName = p.filename().string();
            d.fileExtension = p.extension().string();
            d.fileSize = static_cast<int64_t>(content.size());
            d.sha256Hash = hash;
            d.mimeType = "text/plain";
            d.createdTime = std::chrono::floor<std::chrono::seconds>(
                std::chrono::system_clock::now());
            d.modifiedTime = d.createdTime;
            d.indexedTime = d.createdTime;
            auto did = c.repo->insertDocument(d);
            if (!did.has_value())
                continue;
            if (!c.repo->indexDocumentContent(did.value(), hash, content, "text/plain").has_value())
                continue;
            DocumentContent dc;
            dc.documentId = did.value();
            dc.contentText = content;
            dc.contentLength = static_cast<int64_t>(content.size());
            dc.extractionMethod = "bench";
            dc.language = "en";
            if (!c.repo->insertContent(dc).has_value())
                continue;
            c.docIds.push_back(did.value());
            c.docPaths.push_back(ps);
            ++n;
        }
    }
    return n > 0;
}

bool is_relevant(const std::string& path, const std::vector<std::string>& subs) {
    for (const auto& s : subs)
        if (path.find(s) != std::string::npos)
            return true;
    return false;
}

struct Metrics {
    double ndcg = 0.0, recall = 0.0, rr = 0.0;
    int found = 0;
};

// nDCG@k / Recall@k / MRR for one ranked path list (mirrors live_relevance_ab.py).
Metrics score_query(const std::vector<std::string>& ranked, const std::vector<std::string>& rel,
                    std::size_t k) {
    Metrics m;
    double dcg = 0.0;
    int hits = 0;
    bool first = true;
    for (std::size_t rank = 0; rank < std::min(k, ranked.size()); ++rank) {
        if (is_relevant(ranked[rank], rel)) {
            dcg += 1.0 / std::log2(static_cast<double>(rank) + 2.0);
            ++hits;
            if (first) {
                m.rr = 1.0 / (static_cast<double>(rank) + 1.0);
                first = false;
            }
        }
    }
    const std::size_t ideal_n = std::min(rel.size(), k);
    double idcg = 0.0;
    for (std::size_t r = 0; r < ideal_n; ++r)
        idcg += 1.0 / std::log2(static_cast<double>(r) + 2.0);
    m.ndcg = idcg > 0.0 ? dcg / idcg : 0.0;
    m.recall = rel.empty() ? 0.0 : static_cast<double>(hits) / static_cast<double>(rel.size());
    m.found = hits > 0 ? 1 : 0;
    return m;
}

} // namespace

int main(int argc, char** argv) {
    const fs::path root = argc > 1 ? fs::path(argv[1]) : fs::path("/Users/trevon/work/tools/yams");
    const fs::path qpath = argc > 2
                               ? fs::path(argv[2])
                               : root / "tests/benchmarks/relevance/yams_corpus_queries.json";
    const std::size_t k = argc > 3 ? std::stoul(argv[3]) : 10;

    // Load judged queries.
    std::vector<Query> queries;
    {
        std::ifstream in(qpath);
        if (!in) {
            std::fprintf(stderr, "cannot open queries: %s\n", qpath.string().c_str());
            return 1;
        }
        nlohmann::json j;
        in >> j;
        for (const auto& q : j["queries"]) {
            Query qq;
            qq.text = q["query"].get<std::string>();
            for (const auto& r : q["relevant"])
                qq.relevant.push_back(r.get<std::string>());
            queries.push_back(std::move(qq));
        }
    }

    std::printf("indexing corpus under %s ...\n", root.string().c_str());
    Corpus corpus;
    const auto t0 = std::chrono::steady_clock::now();
    if (!build_corpus(corpus, root)) {
        std::fprintf(stderr, "corpus build failed\n");
        return 1;
    }
    const double build_s =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    std::printf("indexed %zu docs in %.1fs; building backend ...\n", corpus.docIds.size(), build_s);

    // All arms available: strategy router (keyphrase/lead_field) + fragment geom.
    SimeonLexicalBackend::Config cfg;
    cfg.strategy_router_enabled = true;
    cfg.fragment_geometry_enabled = false; // geometry retired; arm removed
    cfg.concept_mining_enabled = true;
    SimeonLexicalBackend backend(cfg);
    if (!backend.buildAsync(corpus.repo).has_value()) {
        std::fprintf(stderr, "buildAsync failed\n");
        return 1;
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(600);
    while (!backend.ready() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    if (!backend.ready()) {
        std::fprintf(stderr, "backend not ready\n");
        return 1;
    }
    std::printf("backend ready: docs=%zu strategy_router=%d fragment_geom=%d\n\n",
                backend.doc_count(), backend.hasStrategyRouter() ? 1 : 0,
                backend.fragmentGeometryReady() ? 1 : 0);

    const std::vector<std::string> arms = {"sab_smooth", "keyphrase", "lead_field"};

    // Debug: dump top-5 paths per arm for the first query (verify geom isn't a bug).
    if (std::getenv("SIMEON_ARM_DEBUG") && !queries.empty()) {
        const auto& q = queries[0];
        std::printf("DEBUG query[0]: \"%s\"\n  relevant:", q.text.c_str());
        for (const auto& r : q.relevant)
            std::printf(" %s", r.c_str());
        std::printf("\n");
        for (const auto& arm : arms) {
            auto dec = backend.scoreBanditRouted(q.text, arm, corpus.docIds);
            if (!dec)
                continue;
            const auto& scores = dec.value().scores;
            std::vector<std::size_t> ord(scores.size());
            for (std::size_t i = 0; i < ord.size(); ++i)
                ord[i] = i;
            std::partial_sort(ord.begin(), ord.begin() + std::min<std::size_t>(5, ord.size()),
                              ord.end(),
                              [&](std::size_t a, std::size_t b) { return scores[a] > scores[b]; });
            std::printf("  [%s]\n", arm.c_str());
            for (std::size_t i = 0; i < std::min<std::size_t>(5, ord.size()); ++i) {
                const bool rel = is_relevant(corpus.docPaths[ord[i]], q.relevant);
                std::printf("    %.4f %s %s\n", scores[ord[i]], rel ? "*" : " ",
                            corpus.docPaths[ord[i]].c_str());
            }
        }
        std::printf("\n");
    }

    std::printf("%-26s %8s %8s %8s %6s %9s %8s %8s\n", "arm", "nDCG@10", "Recall", "MRR", "found",
                "lat_usμ", "p50", "p90");
    for (const auto& arm : arms) {
        Metrics tot;
        int nq = 0;
        std::vector<double> lat; // per-query scoreBanditRouted wall-clock (µs)
        lat.reserve(queries.size());
        for (const auto& q : queries) {
            const auto t0 = std::chrono::steady_clock::now();
            auto dec = backend.scoreBanditRouted(q.text, arm, corpus.docIds);
            lat.push_back(
                std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0)
                    .count());
            if (!dec)
                continue;
            const auto& scores = dec.value().scores;
            std::vector<std::size_t> ord(scores.size());
            for (std::size_t i = 0; i < ord.size(); ++i)
                ord[i] = i;
            std::partial_sort(ord.begin(), ord.begin() + std::min(k, ord.size()), ord.end(),
                              [&](std::size_t a, std::size_t b) { return scores[a] > scores[b]; });
            std::vector<std::string> ranked;
            for (std::size_t i = 0; i < std::min(k, ord.size()); ++i)
                ranked.push_back(corpus.docPaths[ord[i]]);
            Metrics m = score_query(ranked, q.relevant, k);
            tot.ndcg += m.ndcg;
            tot.recall += m.recall;
            tot.rr += m.rr;
            tot.found += m.found;
            ++nq;
        }
        const double d = nq > 0 ? nq : 1;
        std::sort(lat.begin(), lat.end());
        double lat_sum = 0.0;
        for (double v : lat)
            lat_sum += v;
        const double lat_mean = lat.empty() ? 0.0 : lat_sum / static_cast<double>(lat.size());
        const double p50 = lat.empty() ? 0.0 : lat[lat.size() / 2];
        const double p90 = lat.empty() ? 0.0 : lat[static_cast<std::size_t>(lat.size() * 0.9)];
        std::printf("%-26s %8.4f %8.4f %8.4f %4d/%d %9.1f %8.1f %8.1f\n", arm.c_str(), tot.ndcg / d,
                    tot.recall / d, tot.rr / d, tot.found, nq, lat_mean, p50, p90);
    }

    std::error_code ec;
    fs::remove(corpus.dbPath, ec);
    return 0;
}
