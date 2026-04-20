#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "../integration/daemon/test_async_helpers.h"
#include "../integration/daemon/test_daemon_harness.h"
#include "beir_loader.h"
#include "topology_topic_vocab.h"

#include <yams/crypto/hasher.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/metric_keys.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_metadata_store.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace std::chrono_literals;
using yams::daemon::AddDocumentRequest;
using yams::daemon::ClientConfig;
using yams::daemon::DaemonClient;
using yams::daemon::SearchRequest;
using yams::test::DaemonHarness;
using yams::test::DaemonHarnessOptions;
namespace irm = yams::daemon::ir_metrics;

namespace {

constexpr auto kStartTimeout = std::chrono::seconds(60);
constexpr int kTopK = 10;
constexpr int kTopK100 = 100;

struct LabeledQuery {
    std::string query;
    std::set<std::string> relevantFiles;
    std::map<std::string, int> relevanceGrades;
};

struct DocSpec {
    std::string filename;
    std::string content;
    std::string topic;
    int grade = 0;
};

struct Fixture {
    fs::path corpusDir;
    std::vector<DocSpec> docs;
    std::vector<LabeledQuery> queries;
    std::vector<std::string> topics;
    std::unordered_map<std::string, std::vector<std::string>> topicToCoreFiles;
    std::unordered_map<std::string, std::vector<std::string>> topicToBridgeFiles;
    std::unordered_map<std::string, std::vector<std::string>> topicToThinAnchorFiles;
};

fs::path resolveOutputPath() {
    if (const char* override = std::getenv("YAMS_BENCH_OUTPUT"); override && *override) {
        return fs::path(override);
    }
    return fs::path("bench_results/topology_ablation_quality.jsonl");
}

void emitRecord(const fs::path& outputPath, const json& record) {
    json enriched = record;
    enriched["schema_version"] = std::string(irm::kIRSchemaVersion);
    if (const char* v = std::getenv("YAMS_BENCH_RUN_ID"); v && *v) {
        enriched["run_id"] = v;
    }
    if (const char* v = std::getenv("YAMS_BENCH_PHASE"); v && *v) {
        enriched["run_phase"] = v;
    }
    fs::create_directories(outputPath.parent_path());
    std::ofstream ofs(outputPath, std::ios::app);
    ofs << enriched.dump() << '\n';
}

struct TopicSpec {
    std::string name;
    std::vector<std::string> primaryTerms;
    std::string queryPhrase;
};

const std::vector<TopicSpec>& topicCatalog() {
    static const std::vector<TopicSpec> kTopics = {
        {"authentication",
         {"user", "password", "session", "token", "login", "credential"},
         "user login session credential"},
        {"database",
         {"table", "row", "schema", "transaction", "index", "query"},
         "transaction schema row index"},
        {"network",
         {"socket", "packet", "tcp", "latency", "bandwidth", "endpoint"},
         "socket packet endpoint latency"},
        {"encryption",
         {"cipher", "key", "salt", "hash", "aes", "decrypt"},
         "cipher key salt decrypt"},
        {"logging",
         {"trace", "span", "metric", "verbose", "rotation", "appender"},
         "trace span verbose appender"},
        {"storage",
         {"chunk", "block", "object", "manifest", "blob", "shard"},
         "chunk block manifest shard"},
    };
    return kTopics;
}

std::vector<TopicSpec> resolveTopicCatalog(int desiredCount) {
    std::vector<TopicSpec> out;
    const auto& curated = topicCatalog();
    out.reserve(static_cast<std::size_t>(desiredCount));
    for (const auto& t : curated) {
        if (static_cast<int>(out.size()) >= desiredCount) {
            return out;
        }
        out.push_back(t);
    }
    const auto& vocab = yams::bench::extendedTopicVocab();
    for (const auto& v : vocab) {
        if (static_cast<int>(out.size()) >= desiredCount) {
            break;
        }
        TopicSpec spec;
        spec.name = v.name;
        spec.queryPhrase = v.queryPhrase;
        spec.primaryTerms.assign(v.primaryTerms.begin(), v.primaryTerms.end());
        out.push_back(std::move(spec));
    }
    return out;
}

enum class FixtureMode { Synthetic, Beir };

struct FixtureConfig {
    FixtureMode mode = FixtureMode::Synthetic;
    int topicCount = 6;
    int corePerTopic = 6;
    int bridgePerTopic = 3;
    int tangentialPerTopic = 3;
    int noiseDocs = 12;
    std::string beirDataset = "nfcorpus";
    fs::path beirCacheDir;
    int beirMaxQueries = 0;
    bool useSimeonEmbeddings = false;
};

inline bool configUsesRealEmbeddings(const FixtureConfig& cfg) {
    return cfg.useSimeonEmbeddings || cfg.mode == FixtureMode::Beir;
}

int readEnvInt(const char* key, int fallback) {
    if (const char* raw = std::getenv(key); raw && *raw) {
        try {
            return std::max(0, std::stoi(raw));
        } catch (const std::exception& e) {
            spdlog::warn("Bench: bad {}='{}': {}", key, raw, e.what());
        }
    }
    return fallback;
}

FixtureConfig fixtureConfigFromEnv() {
    FixtureConfig cfg;
    if (const char* raw = std::getenv("YAMS_BENCH_FIXTURE"); raw && *raw) {
        std::string s(raw);
        if (s == "beir") {
            cfg.mode = FixtureMode::Beir;
        }
    }
    cfg.topicCount = readEnvInt("YAMS_BENCH_TOPICS", cfg.topicCount);
    if (const char* raw = std::getenv("YAMS_BENCH_DOCS_PER_TOPIC"); raw && *raw) {
        try {
            const int base = std::max(2, std::stoi(raw));
            cfg.corePerTopic = std::max(1, base / 2);
            cfg.bridgePerTopic = std::max(1, base / 4);
            cfg.tangentialPerTopic = std::max(1, base / 4);
        } catch (const std::exception& e) {
            spdlog::warn("Bench: bad YAMS_BENCH_DOCS_PER_TOPIC='{}': {}", raw, e.what());
        }
    }
    cfg.noiseDocs = readEnvInt("YAMS_BENCH_NOISE_DOCS", cfg.noiseDocs);
    if (const char* raw = std::getenv("YAMS_BENCH_BEIR_DATASET"); raw && *raw) {
        cfg.beirDataset = raw;
    }
    if (const char* raw = std::getenv("YAMS_BENCH_BEIR_CACHE_DIR"); raw && *raw) {
        cfg.beirCacheDir = raw;
    } else if (const char* home = std::getenv("HOME"); home && *home) {
        cfg.beirCacheDir = fs::path(home) / ".cache" / "yams" / "benchmarks" / cfg.beirDataset;
    }
    cfg.beirMaxQueries = readEnvInt("YAMS_BENCH_BEIR_MAX_QUERIES", cfg.beirMaxQueries);
    const auto& vocab = yams::bench::extendedTopicVocab();
    const int maxTopics = static_cast<int>(topicCatalog().size() + vocab.size());
    if (cfg.topicCount > maxTopics) {
        spdlog::warn("Bench: requested {} topics but only {} available; clamping", cfg.topicCount,
                     maxTopics);
        cfg.topicCount = maxTopics;
    }
    if (cfg.topicCount < 1) {
        cfg.topicCount = 1;
    }
    return cfg;
}

std::string makeContent(const TopicSpec& primary, std::mt19937& rng,
                        const TopicSpec* neighbor = nullptr, double neighborMixRatio = 0.0) {
    std::ostringstream oss;
    std::uniform_int_distribution<int> primDist(0,
                                                static_cast<int>(primary.primaryTerms.size()) - 1);
    const int primaryWords = 60;
    const int neighborWords = static_cast<int>(primaryWords * neighborMixRatio);
    oss << "Document body. ";
    for (int i = 0; i < primaryWords; ++i) {
        oss << primary.primaryTerms[primDist(rng)] << ' ';
    }
    if (neighbor) {
        std::uniform_int_distribution<int> nDist(
            0, static_cast<int>(neighbor->primaryTerms.size()) - 1);
        for (int i = 0; i < neighborWords; ++i) {
            oss << neighbor->primaryTerms[nDist(rng)] << ' ';
        }
    }
    oss << "\nMore filler content goes here to add length and natural variation.\n";
    return oss.str();
}

std::string makeThinAnchorContent(const TopicSpec& primary, std::mt19937& rng) {
    static const std::array<const char*, 24> noiseWords = {
        "alpha", "beta",  "gamma",  "delta",   "epsilon", "zeta", "eta",     "theta",
        "iota",  "kappa", "lambda", "mu",      "nu",      "xi",   "omicron", "pi",
        "rho",   "sigma", "tau",    "upsilon", "phi",     "chi",  "psi",     "omega",
    };
    std::ostringstream oss;
    std::uniform_int_distribution<int> noiseDist(0, static_cast<int>(noiseWords.size()) - 1);
    std::uniform_int_distribution<int> termDist(0,
                                                static_cast<int>(primary.primaryTerms.size()) - 1);
    oss << "Filler content with sparse anchor. ";
    for (int i = 0; i < 80; ++i) {
        oss << noiseWords[noiseDist(rng)] << ' ';
    }
    oss << primary.primaryTerms[termDist(rng)] << ' ';
    for (int i = 0; i < 80; ++i) {
        oss << noiseWords[noiseDist(rng)] << ' ';
    }
    return oss.str();
}

std::string makeNoiseContent(std::mt19937& rng) {
    static const std::array<const char*, 24> noiseWords = {
        "alpha", "beta",  "gamma",  "delta",   "epsilon", "zeta", "eta",     "theta",
        "iota",  "kappa", "lambda", "mu",      "nu",      "xi",   "omicron", "pi",
        "rho",   "sigma", "tau",    "upsilon", "phi",     "chi",  "psi",     "omega",
    };
    std::ostringstream oss;
    std::uniform_int_distribution<int> dist(0, static_cast<int>(noiseWords.size()) - 1);
    oss << "Generic content with no domain anchor. ";
    for (int i = 0; i < 80; ++i) {
        oss << noiseWords[dist(rng)] << ' ';
    }
    return oss.str();
}

Fixture buildFixture(const fs::path& root, const FixtureConfig& cfg) {
    Fixture fx;
    fx.corpusDir = root / "corpus";
    fs::create_directories(fx.corpusDir);

    const auto catalog = resolveTopicCatalog(cfg.topicCount);
    for (const auto& t : catalog) {
        fx.topics.push_back(t.name);
    }

    std::mt19937 rng(0xC01DA11A);

    auto addDoc = [&](const std::string& filename, const std::string& topic, int grade,
                      const std::string& content) {
        std::ofstream(fx.corpusDir / filename) << content;
        fx.docs.push_back({filename, content, topic, grade});
    };

    for (std::size_t ti = 0; ti < catalog.size(); ++ti) {
        const auto& topic = catalog[ti];
        const auto& neighbor = catalog[(ti + 1) % catalog.size()];
        const auto& farNeighbor = catalog[(ti + 2) % catalog.size()];

        for (int i = 0; i < cfg.corePerTopic; ++i) {
            std::string fn = "core_" + topic.name + "_" + std::to_string(i) + ".txt";
            addDoc(fn, topic.name, 3, makeContent(topic, rng));
            fx.topicToCoreFiles[topic.name].push_back(fn);
        }
        for (int i = 0; i < cfg.bridgePerTopic; ++i) {
            std::string fn = "bridge_" + topic.name + "_" + std::to_string(i) + ".txt";
            addDoc(fn, topic.name, 2, makeContent(topic, rng, &neighbor, 0.45));
            fx.topicToBridgeFiles[topic.name].push_back(fn);
        }
        for (int i = 0; i < cfg.tangentialPerTopic; ++i) {
            std::string fn = "tangent_" + topic.name + "_" + std::to_string(i) + ".txt";
            addDoc(fn, topic.name, 1, makeContent(farNeighbor, rng, &topic, 0.20));
        }
        std::string thinFn = "thin_" + topic.name + ".txt";
        addDoc(thinFn, topic.name, 1, makeThinAnchorContent(topic, rng));
        fx.topicToThinAnchorFiles[topic.name].push_back(thinFn);
    }
    for (int i = 0; i < cfg.noiseDocs; ++i) {
        std::string fn = "noise_" + std::to_string(i) + ".txt";
        addDoc(fn, "", 0, makeNoiseContent(rng));
    }

    for (const auto& t : catalog) {
        LabeledQuery q;
        q.query = t.queryPhrase;
        for (const auto& d : fx.docs) {
            if (d.topic == t.name && d.grade > 0) {
                q.relevantFiles.insert(d.filename);
                q.relevanceGrades[d.filename] = d.grade;
            }
        }
        fx.queries.push_back(std::move(q));
    }
    return fx;
}

std::string sanitizeBeirId(const std::string& id) {
    std::string out;
    out.reserve(id.size());
    for (char c : id) {
        if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_') {
            out.push_back(c);
        } else {
            out.push_back('_');
        }
    }
    if (out.empty()) {
        out = "doc";
    }
    return out;
}

yams::Result<Fixture> buildBeirFixture(const fs::path& root, const FixtureConfig& cfg) {
    Fixture fx;
    fx.corpusDir = root / "corpus";
    fs::create_directories(fx.corpusDir);

    auto loaded = yams::bench::loadBEIRDataset(cfg.beirDataset, cfg.beirCacheDir);
    if (!loaded) {
        return loaded.error();
    }
    const auto& ds = loaded.value();

    std::unordered_map<std::string, std::string> docIdToFilename;
    docIdToFilename.reserve(ds.documents.size());
    for (const auto& [docId, doc] : ds.documents) {
        std::string base = sanitizeBeirId(docId);
        std::string fn = base + ".txt";
        int suffix = 1;
        while (docIdToFilename.count(docId) == 0 && fs::exists(fx.corpusDir / fn)) {
            fn = base + "_" + std::to_string(suffix++) + ".txt";
        }
        std::string content;
        if (!doc.title.empty()) {
            content = doc.title;
            content.append("\n\n");
        }
        content.append(doc.text);
        std::ofstream(fx.corpusDir / fn) << content;
        fx.docs.push_back({fn, std::move(content), std::string{}, 0});
        docIdToFilename.emplace(docId, std::move(fn));
    }

    std::vector<std::string> queryIds;
    queryIds.reserve(ds.queries.size());
    for (const auto& [qid, _] : ds.queries) {
        if (ds.qrels.find(qid) != ds.qrels.end()) {
            queryIds.push_back(qid);
        }
    }
    if (cfg.beirMaxQueries > 0 && static_cast<int>(queryIds.size()) > cfg.beirMaxQueries) {
        queryIds.resize(static_cast<std::size_t>(cfg.beirMaxQueries));
    }

    for (const auto& qid : queryIds) {
        const auto qIt = ds.queries.find(qid);
        if (qIt == ds.queries.end()) {
            continue;
        }
        LabeledQuery q;
        q.query = qIt->second.text;
        auto range = ds.qrels.equal_range(qid);
        bool hasRelevant = false;
        for (auto it = range.first; it != range.second; ++it) {
            const auto& [docId, grade] = it->second;
            if (grade <= 0) {
                continue;
            }
            auto fnIt = docIdToFilename.find(docId);
            if (fnIt == docIdToFilename.end()) {
                continue;
            }
            q.relevantFiles.insert(fnIt->second);
            q.relevanceGrades[fnIt->second] = grade;
            hasRelevant = true;
        }
        if (hasRelevant) {
            fx.queries.push_back(std::move(q));
        }
    }

    spdlog::info("BEIR fixture: {} -> {} docs written, {} queries with qrels", cfg.beirDataset,
                 fx.docs.size(), fx.queries.size());
    return fx;
}

double computeDCG(const std::vector<int>& grades, int k) {
    double dcg = 0.0;
    const int bound = std::min(k, static_cast<int>(grades.size()));
    for (int i = 0; i < bound; ++i) {
        dcg += (std::pow(2.0, grades[i]) - 1.0) / std::log2(i + 2.0);
    }
    return dcg;
}

double computeIDCG(std::vector<int> grades, int k) {
    std::sort(grades.begin(), grades.end(), std::greater<int>());
    return computeDCG(grades, k);
}

struct QualityMetrics {
    double ndcgAtK = 0.0;
    double mrrAtK = 0.0;
    double map = 0.0;
    double recallAtK = 0.0;
    double recallAt100 = 0.0;
    int numQueries = 0;
    std::uint64_t routedSeedCountTotal = 0;
    std::uint64_t routedSeedCountQueries = 0;
    std::string tier1FingerprintJoined;
};

fs::path benchFixtureDir() {
    if (const char* v = std::getenv("YAMS_BENCH_FIXTURE_DIR"); v && *v) {
        return fs::path(v);
    }
    return {};
}

std::string fixtureKey(const FixtureConfig& cfg) {
    std::string corpus = cfg.mode == FixtureMode::Beir ? cfg.beirDataset : "synthetic";
    std::string backend = configUsesRealEmbeddings(cfg) ? "simeon" : "mock";
    return corpus + "_" + backend;
}

bool copyRecursive(const fs::path& src, const fs::path& dst) {
    std::error_code ec;
    fs::create_directories(dst, ec);
    if (ec) {
        spdlog::warn("copyRecursive: create_directories({}) failed: {}", dst.string(),
                     ec.message());
        return false;
    }
    for (auto it = fs::recursive_directory_iterator(src, ec);
         !ec && it != fs::recursive_directory_iterator(); ++it) {
        const auto& entry = *it;
        auto rel = fs::relative(entry.path(), src, ec);
        if (ec) {
            return false;
        }
        fs::path target = dst / rel;
        if (entry.is_directory(ec)) {
            fs::create_directories(target, ec);
        } else if (entry.is_regular_file(ec)) {
            fs::create_directories(target.parent_path(), ec);
            fs::copy_file(entry.path(), target, fs::copy_options::overwrite_existing, ec);
        }
        if (ec) {
            spdlog::warn("copyRecursive: copy {} -> {} failed: {}", entry.path().string(),
                         target.string(), ec.message());
            return false;
        }
    }
    return !ec;
}

bool tryLoadBenchFixture(const FixtureConfig& cfg, const fs::path& targetDataDir) {
    auto fxDir = benchFixtureDir();
    if (fxDir.empty()) {
        return false;
    }
    fs::path fxPath = fxDir / fixtureKey(cfg);
    std::error_code ec;
    if (!fs::exists(fxPath / "yams.db", ec)) {
        return false;
    }
    if (!copyRecursive(fxPath, targetDataDir)) {
        spdlog::warn("Bench fixture: copy {} -> {} failed; falling back to live ingest",
                     fxPath.string(), targetDataDir.string());
        return false;
    }
    fs::remove(targetDataDir / ".yams-lock", ec);
    fs::remove(targetDataDir / "vectors.db.lock", ec);
    fs::remove(targetDataDir / "vectors.lock", ec);
    spdlog::info("Bench fixture loaded from {}", fxPath.string());
    return true;
}

void saveBenchFixture(const FixtureConfig& cfg, const fs::path& sourceDataDir) {
    auto fxDir = benchFixtureDir();
    if (fxDir.empty()) {
        return;
    }
    fs::path fxPath = fxDir / fixtureKey(cfg);
    std::error_code ec;
    if (fs::exists(fxPath / "yams.db", ec)) {
        return;
    }
    fs::create_directories(fxPath, ec);
    static const std::unordered_set<std::string> kExcludeEntries = {".yams-lock", "vectors.db.lock",
                                                                    "vectors.lock"};
    for (auto it = fs::recursive_directory_iterator(sourceDataDir, ec);
         !ec && it != fs::recursive_directory_iterator(); ++it) {
        const auto& entry = *it;
        auto rel = fs::relative(entry.path(), sourceDataDir, ec);
        if (ec) {
            break;
        }
        if (kExcludeEntries.contains(rel.filename().string())) {
            continue;
        }
        fs::path target = fxPath / rel;
        if (entry.is_directory(ec)) {
            fs::create_directories(target, ec);
        } else if (entry.is_regular_file(ec)) {
            fs::create_directories(target.parent_path(), ec);
            fs::copy_file(entry.path(), target, fs::copy_options::overwrite_existing, ec);
        }
    }
    spdlog::info("Bench fixture saved to {}", fxPath.string());
}

bool ingestFixtureMode(DaemonClient& client, const Fixture& fx, bool withEmbeddings) {
    for (const auto& d : fx.docs) {
        AddDocumentRequest req;
        req.name = d.filename;
        req.content = d.content;
        req.tags = {"topology-ablation-quality"};
        req.noEmbeddings = !withEmbeddings;

        auto res = yams::cli::run_sync(client.streamingAddDocument(req), 60s);
        if (!res) {
            spdlog::warn("Ingest failed for {}: {}", d.filename, res.error().message);
            return false;
        }
    }
    return true;
}

bool seedTopologyArtifacts(DaemonHarness& harness, const Fixture& fx) {
    auto* daemon = harness.daemon();
    auto* sm = daemon ? daemon->getServiceManager() : nullptr;
    auto repo = sm ? sm->getMetadataRepo() : nullptr;
    auto kg = repo ? repo->getKnowledgeGraphStore() : nullptr;
    if (!repo || !kg) {
        spdlog::warn("Topology seeding skipped: metadata or KG store unavailable");
        return false;
    }

    auto allTaggedRes = repo->findDocumentsByTags({"topology-ablation-quality"}, false);
    if (!allTaggedRes) {
        spdlog::warn("Topology seeding: findDocumentsByTags failed: {}",
                     allTaggedRes.error().message);
        return false;
    }
    std::unordered_map<std::string, yams::metadata::DocumentInfo> byFilename;
    for (const auto& info : allTaggedRes.value()) {
        byFilename.emplace(info.fileName, info);
    }

    std::unordered_map<std::string, std::string> filenameToHash;
    std::unordered_map<std::string, std::int64_t> filenameToNodeId;
    for (const auto& d : fx.docs) {
        auto it = byFilename.find(d.filename);
        if (it == byFilename.end()) {
            spdlog::warn("Topology seeding: no metadata row for {}", d.filename);
            continue;
        }
        const auto& info = it->second;
        filenameToHash[d.filename] = info.sha256Hash;
        auto nodeRes = kg->ensureDocumentNode(info.sha256Hash, info.fileName);
        if (!nodeRes) {
            spdlog::warn("Topology seeding: ensureDocumentNode failed for {}: {}", d.filename,
                         nodeRes.error().message);
            continue;
        }
        filenameToNodeId[d.filename] = nodeRes.value();
    }

    std::vector<yams::metadata::KGEdge> edges;
    auto addPair = [&](std::int64_t a, std::int64_t b, const std::string& rel, float weight) {
        yams::metadata::KGEdge fwd;
        fwd.srcNodeId = a;
        fwd.dstNodeId = b;
        fwd.relation = rel;
        fwd.weight = weight;
        edges.push_back(fwd);
        yams::metadata::KGEdge rev = fwd;
        rev.srcNodeId = b;
        rev.dstNodeId = a;
        edges.push_back(rev);
    };

    for (const auto& topic : fx.topics) {
        const auto& cores = fx.topicToCoreFiles.at(topic);
        if (cores.size() < 2) {
            continue;
        }
        const auto medoidIt = filenameToNodeId.find(cores.front());
        if (medoidIt == filenameToNodeId.end()) {
            continue;
        }
        for (std::size_t i = 1; i < cores.size(); ++i) {
            auto it = filenameToNodeId.find(cores[i]);
            if (it == filenameToNodeId.end())
                continue;
            addPair(medoidIt->second, it->second, "semantic_neighbor", 0.85f);
        }
        const auto& bridges = fx.topicToBridgeFiles.at(topic);
        for (const auto& b : bridges) {
            auto it = filenameToNodeId.find(b);
            if (it == filenameToNodeId.end())
                continue;
            addPair(medoidIt->second, it->second, "semantic_neighbor", 0.55f);
        }
    }

    if (!edges.empty()) {
        auto er = kg->addEdgesUnique(edges);
        if (!er) {
            spdlog::warn("Topology seeding: addEdgesUnique failed: {}", er.error().message);
        }
    }

    yams::topology::TopologyArtifactBatch batch;
    batch.snapshotId = "ablation_quality_synth";
    batch.algorithm = "synthetic_quality_v1";
    batch.generatedAtUnixSeconds =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count());
    batch.topologyEpoch = 1;

    for (std::size_t ti = 0; ti < fx.topics.size(); ++ti) {
        const auto& topic = fx.topics[ti];
        const auto& cores = fx.topicToCoreFiles.at(topic);
        const auto& bridges = fx.topicToBridgeFiles.at(topic);
        if (cores.empty())
            continue;
        const auto medoidHashIt = filenameToHash.find(cores.front());
        if (medoidHashIt == filenameToHash.end())
            continue;

        yams::topology::ClusterArtifact cluster;
        cluster.clusterId = "cluster_" + topic;
        cluster.level = 0;
        cluster.persistenceScore = 0.85;
        cluster.cohesionScore = 0.70;
        cluster.bridgeMass = static_cast<double>(bridges.size()) /
                             static_cast<double>(cores.size() + bridges.size());

        cluster.medoid = yams::topology::ClusterRepresentative{
            cluster.clusterId, medoidHashIt->second, cores.front(), 1.0};

        for (std::size_t i = 0; i < cores.size(); ++i) {
            auto hashIt = filenameToHash.find(cores[i]);
            if (hashIt == filenameToHash.end())
                continue;
            cluster.memberDocumentHashes.push_back(hashIt->second);
            yams::topology::DocumentClusterMembership m;
            m.documentHash = hashIt->second;
            m.clusterId = cluster.clusterId;
            m.clusterLevel = 0;
            m.persistenceScore = cluster.persistenceScore;
            m.cohesionScore = cluster.cohesionScore;
            m.bridgeScore = 0.05;
            m.role = (i == 0) ? yams::topology::DocumentTopologyRole::Medoid
                              : yams::topology::DocumentTopologyRole::Core;
            batch.memberships.push_back(std::move(m));
        }
        for (const auto& b : bridges) {
            auto hashIt = filenameToHash.find(b);
            if (hashIt == filenameToHash.end())
                continue;
            cluster.memberDocumentHashes.push_back(hashIt->second);
            yams::topology::DocumentClusterMembership m;
            m.documentHash = hashIt->second;
            m.clusterId = cluster.clusterId;
            m.clusterLevel = 0;
            m.persistenceScore = cluster.persistenceScore;
            m.cohesionScore = cluster.cohesionScore;
            m.bridgeScore = 0.80;
            m.role = yams::topology::DocumentTopologyRole::Bridge;
            batch.memberships.push_back(std::move(m));
        }
        auto thinIt = fx.topicToThinAnchorFiles.find(topic);
        if (thinIt != fx.topicToThinAnchorFiles.end()) {
            const auto medoidNodeIt = filenameToNodeId.find(cores.front());
            for (const auto& tf : thinIt->second) {
                auto hashIt = filenameToHash.find(tf);
                if (hashIt == filenameToHash.end())
                    continue;
                cluster.memberDocumentHashes.push_back(hashIt->second);
                yams::topology::DocumentClusterMembership mm;
                mm.documentHash = hashIt->second;
                mm.clusterId = cluster.clusterId;
                mm.clusterLevel = 0;
                mm.persistenceScore = cluster.persistenceScore;
                mm.cohesionScore = cluster.cohesionScore;
                mm.bridgeScore = 0.10;
                mm.role = yams::topology::DocumentTopologyRole::Outlier;
                batch.memberships.push_back(std::move(mm));
                auto nodeIt = filenameToNodeId.find(tf);
                if (nodeIt != filenameToNodeId.end() && medoidNodeIt != filenameToNodeId.end()) {
                    yams::metadata::KGEdge fwd;
                    fwd.srcNodeId = medoidNodeIt->second;
                    fwd.dstNodeId = nodeIt->second;
                    fwd.relation = "semantic_neighbor";
                    fwd.weight = 0.40f;
                    auto er = kg->addEdgesUnique({fwd});
                    if (!er) {
                        spdlog::warn("Topology seeding (thin anchor): addEdgesUnique failed: {}",
                                     er.error().message);
                    }
                }
            }
        }
        cluster.memberCount = cluster.memberDocumentHashes.size();
        batch.clusters.push_back(std::move(cluster));
    }

    auto metadataIface = std::static_pointer_cast<yams::metadata::IMetadataRepository>(repo);
    yams::topology::MetadataKgTopologyArtifactStore store(metadataIface, kg);
    auto sr = store.storeBatch(batch);
    if (!sr) {
        spdlog::warn("Topology seeding: storeBatch failed: {}", sr.error().message);
        return false;
    }

    std::vector<std::string> overlayHashes;
    overlayHashes.reserve(filenameToHash.size());
    for (const auto& [fname, hash] : filenameToHash) {
        overlayHashes.push_back(hash);
    }
    sm->getTopologyManager().markDirtyBatch(overlayHashes);
    sm->getTopologyManager().setPublishedEpoch(batch.topologyEpoch);
    return true;
}

double evaluateAtK(DaemonClient& client, const LabeledQuery& q, int k,
                   const std::string& searchType, QualityMetrics& acc, bool accumulateTopK) {
    SearchRequest req;
    req.query = q.query;
    req.searchType = searchType;
    req.limit = static_cast<std::size_t>(k);
    req.timeout = 30s;

    auto res = yams::cli::run_sync(client.search(req), 45s);
    if (!res) {
        return 0.0;
    }
    const auto& results = res.value().results;
    if (accumulateTopK) {
        const auto& stats = res.value().searchStats;
        if (auto it = stats.find("routed_seed_count"); it != stats.end()) {
            try {
                acc.routedSeedCountTotal += std::stoull(it->second);
                ++acc.routedSeedCountQueries;
            } catch (...) {
            }
        }
        if (auto it = stats.find("tier1_hash_fingerprint"); it != stats.end()) {
            if (!acc.tier1FingerprintJoined.empty()) {
                acc.tier1FingerprintJoined.push_back('|');
            }
            acc.tier1FingerprintJoined.append(it->second);
        }
    }

    int firstRelevantRank = -1;
    int numRelevantInTopK = 0;
    int numRelevantSeen = 0;
    double avgPrecision = 0.0;
    std::vector<int> retrievedGrades;
    std::unordered_set<std::string> seenTopKFilenames;

    const std::size_t bound = std::min(static_cast<std::size_t>(k), results.size());
    for (std::size_t i = 0; i < bound; ++i) {
        std::string filename = fs::path(results[i].path).filename().string();
        const bool isDuplicate = !seenTopKFilenames.insert(filename).second;
        const bool isRelevant = q.relevantFiles.count(filename) > 0 && !isDuplicate;

        auto gradeIt = q.relevanceGrades.find(filename);
        int grade = (!isDuplicate && gradeIt != q.relevanceGrades.end()) ? gradeIt->second : 0;
        retrievedGrades.push_back(grade);

        if (isRelevant) {
            ++numRelevantInTopK;
            if (firstRelevantRank < 0) {
                firstRelevantRank = static_cast<int>(i) + 1;
            }
            ++numRelevantSeen;
            avgPrecision += static_cast<double>(numRelevantSeen) / static_cast<double>(i + 1);
        }
    }

    double recallHere = 0.0;
    if (!q.relevantFiles.empty()) {
        recallHere =
            static_cast<double>(numRelevantInTopK) / static_cast<double>(q.relevantFiles.size());
    }

    if (accumulateTopK) {
        if (firstRelevantRank > 0) {
            acc.mrrAtK += 1.0 / firstRelevantRank;
        }
        acc.recallAtK += recallHere;
        if (numRelevantSeen > 0) {
            acc.map += avgPrecision / numRelevantSeen;
        }
        std::vector<int> allGrades;
        for (const auto& [_, g] : q.relevanceGrades) {
            allGrades.push_back(g);
        }
        double dcg = computeDCG(retrievedGrades, k);
        double idcg = computeIDCG(allGrades, k);
        acc.ndcgAtK += (idcg > 0.0) ? dcg / idcg : 0.0;
    }
    return recallHere;
}

QualityMetrics evaluate(DaemonClient& client, const Fixture& fx, const std::string& searchType) {
    QualityMetrics m;
    m.numQueries = static_cast<int>(fx.queries.size());
    if (m.numQueries == 0) {
        return m;
    }
    for (const auto& q : fx.queries) {
        (void)evaluateAtK(client, q, kTopK, searchType, m, true);
    }
    for (const auto& q : fx.queries) {
        QualityMetrics scratch;
        const double recall100 = evaluateAtK(client, q, kTopK100, searchType, scratch, false);
        m.recallAt100 += recall100;
    }
    m.ndcgAtK /= m.numQueries;
    m.mrrAtK /= m.numQueries;
    m.map /= m.numQueries;
    m.recallAtK /= m.numQueries;
    m.recallAt100 /= m.numQueries;
    if (!m.tier1FingerprintJoined.empty()) {
        const auto& joined = m.tier1FingerprintJoined;
        m.tier1FingerprintJoined =
            yams::crypto::SHA256Hasher::hash(
                std::as_bytes(std::span<const char>(joined.data(), joined.size())))
                .substr(0, 16);
    }
    return m;
}

void setAxisEnv(std::size_t maxClusters, std::size_t maxDocs, float medoidBoost, float bridgeBoost,
                const char* variant = nullptr) {
    setenv("YAMS_SEARCH_ENABLE_TOPOLOGY_WEAK_ROUTING", "1", 1);
    setenv("YAMS_SEARCH_TOPOLOGY_MAX_CLUSTERS", std::to_string(maxClusters).c_str(), 1);
    setenv("YAMS_SEARCH_TOPOLOGY_MAX_DOCS", std::to_string(maxDocs).c_str(), 1);
    std::ostringstream mb, bb;
    mb << medoidBoost;
    bb << bridgeBoost;
    setenv("YAMS_SEARCH_TOPOLOGY_MEDOID_BOOST", mb.str().c_str(), 1);
    setenv("YAMS_SEARCH_TOPOLOGY_BRIDGE_BOOST", bb.str().c_str(), 1);
    setenv("YAMS_SEARCH_ADAPTIVE_MIN_TEXT_HITS", "999", 1);
    setenv("YAMS_SEARCH_ADAPTIVE_MIN_TOP_TEXT_SCORE", "0.99", 1);
    setenv("YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "1", 1);
    setenv("YAMS_SEARCH_BYPASS_CORPUS_WARMING_GATE", "1", 1);
    setenv("YAMS_ENABLE_ENV_OVERRIDES", "1", 1);
    if (variant) {
        setenv("YAMS_SEARCH_TOPOLOGY_ROUTING_VARIANT", variant, 1);
    } else {
        unsetenv("YAMS_SEARCH_TOPOLOGY_ROUTING_VARIANT");
    }
}

DaemonHarnessOptions quietHarnessOptions() {
    DaemonHarnessOptions opts;
    opts.enableModelProvider = false;
    opts.useMockModelProvider = true;
    opts.autoLoadPlugins = false;
    opts.enableAutoRepair = true;
    opts.isolateState = true;
    return opts;
}

DaemonHarnessOptions harnessOptionsFor(const FixtureConfig& cfg) {
    DaemonHarnessOptions opts = quietHarnessOptions();
    if (configUsesRealEmbeddings(cfg)) {
        opts.enableModelProvider = true;
        opts.useMockModelProvider = false;
        opts.autoLoadPlugins = false;
    }
    return opts;
}

struct CellOutcome {
    bool ok = false;
    QualityMetrics metrics{};
    double hybridEvalMs = -1.0;
    std::string fixtureStatus = "ok";
    bool topologySeeded = false;
    std::uint64_t clusterCount = 0;
    std::uint64_t documentsProcessed = 0;
    std::uint64_t documentsMissingEmbeddings = 0;
    std::string cellIdentityExpected;
    std::string cellIdentityObserved;
    std::string algorithm;
    std::uint64_t clusterSizeMax = 0;
    std::uint64_t clusterSizeP50 = 0;
    std::uint64_t clusterSizeP90 = 0;
    std::uint64_t singletonCount = 0;
    std::uint64_t orphanDocCount = 0;
    double singletonRatio = 0.0;
    double giantClusterRatio = 0.0;
    double clusterSizeGini = 0.0;
    double avgIntraEdgeWeight = 0.0;
    double routedSeedCountMean = 0.0;
    std::uint64_t routedSeedCountTotal = 0;
    std::uint64_t routedSeedCountQueries = 0;
    std::string tier1HashFingerprint;
};

std::string computeCellIdentityFromEnv() {
    auto envOr = [](const char* name, const char* fallback) -> std::string {
        const char* raw = std::getenv(name);
        return (raw != nullptr && *raw != '\0') ? std::string{raw} : std::string{fallback};
    };
    std::string s;
    s.reserve(128);
    s.append("topk=");
    s.append(envOr("YAMS_GRAPH_SEMANTIC_TOPK", "default"));
    s.append(";thr=");
    s.append(envOr("YAMS_GRAPH_SEMANTIC_THRESHOLD", "default"));
    s.append(";engine=");
    s.append(envOr("YAMS_TOPOLOGY_ENGINE", "default"));
    s.append(";reciprocal=");
    s.append(envOr("YAMS_TOPOLOGY_RECIPROCAL_ONLY", "default"));
    s.append(";min_edge=");
    s.append(envOr("YAMS_TOPOLOGY_MIN_EDGE_SCORE", "default"));
    s.append(";max_neighbors=");
    s.append(envOr("YAMS_TOPOLOGY_MAX_NEIGHBORS", "default"));
    return s;
}

struct TopologyConfigOverride {
    std::optional<std::string> integration;
    std::optional<int> recallExpandPerCluster;
    std::optional<int> rrfK;
    std::optional<std::string> routeScoring;
    std::optional<std::string> engine;
    std::optional<int> kmeansK;

    [[nodiscard]] bool empty() const {
        return !integration.has_value() && !recallExpandPerCluster.has_value() &&
               !rrfK.has_value() && !routeScoring.has_value() && !engine.has_value() &&
               !kmeansK.has_value();
    }
};

fs::path writeTopologyConfigToml(const TopologyConfigOverride& ovr) {
    static std::atomic<std::uint64_t> counter{0};
    const auto nonce = counter.fetch_add(1, std::memory_order_relaxed);
    auto path =
        fs::temp_directory_path() /
        ("yams_topology_bench_" + std::to_string(nonce) + "_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".toml");
    std::ofstream out(path);
    out << "[search.topology]\n";
    if (ovr.integration.has_value()) {
        out << "integration = \"" << *ovr.integration << "\"\n";
    }
    if (ovr.recallExpandPerCluster.has_value()) {
        out << "recall_expand_per_cluster = " << *ovr.recallExpandPerCluster << "\n";
    }
    if (ovr.rrfK.has_value()) {
        out << "rrf_k = " << *ovr.rrfK << "\n";
    }
    if (ovr.routeScoring.has_value()) {
        out << "route_scoring = \"" << *ovr.routeScoring << "\"\n";
    }
    if (ovr.engine.has_value() || ovr.kmeansK.has_value()) {
        out << "\n[topology]\n";
        if (ovr.engine.has_value()) {
            out << "engine = \"" << *ovr.engine << "\"\n";
        }
        if (ovr.kmeansK.has_value()) {
            out << "kmeans_k = " << *ovr.kmeansK << "\n";
        }
    }
    return path;
}

bool runSyncTopologyRebuild(yams::daemon::ServiceManager* sm, const std::string& cellId,
                            CellOutcome& outcome) {
    if (sm == nullptr) {
        return false;
    }
    auto& tm = sm->getTopologyManager();
    tm.setAutoRebuildEnabled(false);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    while (tm.isRebuildInProgress() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // Go through ServiceManager so tuningConfig_.topologyAlgorithm (seeded from
    // [topology].engine via ConfigResolver) is honored. Calling
    // tm.rebuildArtifacts(..., "") here would always fall back to "connected".
    auto result = sm->rebuildTopologyArtifacts("axis-cell/" + cellId, false, {});
    tm.setAutoRebuildEnabled(true);
    if (!result) {
        spdlog::warn("runSyncTopologyRebuild failed: {}", result.error().message);
        return false;
    }
    const auto& stats = result.value();
    outcome.clusterCount = stats.clustersBuilt;
    outcome.documentsProcessed = stats.documentsProcessed;
    outcome.documentsMissingEmbeddings = stats.documentsMissingEmbeddings;
    outcome.cellIdentityObserved = stats.cellIdentity;
    outcome.algorithm = stats.algorithm;
    outcome.clusterSizeMax = stats.clusterSizeMax;
    outcome.clusterSizeP50 = stats.clusterSizeP50;
    outcome.clusterSizeP90 = stats.clusterSizeP90;
    outcome.singletonCount = stats.singletonCount;
    outcome.orphanDocCount = stats.orphanDocCount;
    outcome.singletonRatio = stats.singletonRatio;
    outcome.giantClusterRatio = stats.giantClusterRatio;
    outcome.clusterSizeGini = stats.clusterSizeGini;
    outcome.avgIntraEdgeWeight = stats.avgIntraEdgeWeight;
    return stats.stored && !stats.skipped;
}

CellOutcome runCell(std::size_t maxClusters, std::size_t maxDocs, float medoidBoost,
                    float bridgeBoost, const char* variant = nullptr,
                    const FixtureConfig& cfg = FixtureConfig{},
                    const TopologyConfigOverride& topologyConfig = TopologyConfigOverride{}) {
    setAxisEnv(maxClusters, maxDocs, medoidBoost, bridgeBoost, variant);
    if (configUsesRealEmbeddings(cfg)) {
        setenv("YAMS_BENCH_INGEST_NO_EMBEDDINGS", "0", 1);
        setenv("YAMS_EMBED_BACKEND", "simeon", 1);
    } else {
        unsetenv("YAMS_EMBED_BACKEND");
    }
    if (const char* lv = std::getenv("YAMS_BENCH_DAEMON_DEBUG"); lv && std::string(lv) == "1") {
        spdlog::set_level(spdlog::level::debug);
    }

    fs::path configTomlPath;
    if (!topologyConfig.empty()) {
        configTomlPath = writeTopologyConfigToml(topologyConfig);
    }
    struct CleanupTomlOnReturn {
        fs::path path;
        ~CleanupTomlOnReturn() {
            if (!path.empty()) {
                std::error_code ec;
                fs::remove(path, ec);
            }
        }
    } cleanupToml{configTomlPath};

    CellOutcome outcome;
    try {
        DaemonHarnessOptions harnessOpts = harnessOptionsFor(cfg);
        if (!configTomlPath.empty()) {
            harnessOpts.configPath = configTomlPath;
        }
        DaemonHarness harness(harnessOpts);
        const fs::path dataDir = harness.rootDir() / "data";
        std::error_code ec_fx;
        fs::create_directories(dataDir, ec_fx);
        const bool fixtureLoaded = tryLoadBenchFixture(cfg, dataDir);
        if (!harness.start(kStartTimeout)) {
            outcome.fixtureStatus = "missing";
            return outcome;
        }

        Fixture fx;
        if (cfg.mode == FixtureMode::Beir) {
            auto fxRes = buildBeirFixture(harness.rootDir(), cfg);
            if (!fxRes) {
                spdlog::warn("BEIR fixture build failed: {}", fxRes.error().message);
                outcome.fixtureStatus = "missing";
                return outcome;
            }
            fx = std::move(fxRes.value());
        } else {
            fx = buildFixture(harness.rootDir(), cfg);
        }

        ClientConfig ccfg;
        ccfg.socketPath = harness.socketPath();
        ccfg.autoStart = false;
        ccfg.requestTimeout = 30s;
        DaemonClient client(ccfg);

        const bool ingestWithEmbeddings = configUsesRealEmbeddings(cfg);
        if (!fixtureLoaded) {
            if (!ingestFixtureMode(client, fx, ingestWithEmbeddings)) {
                outcome.fixtureStatus = "missing";
                return outcome;
            }
        } else {
            spdlog::info("Bench: skipping ingest (fixture loaded for {})", fixtureKey(cfg));
        }

        outcome.cellIdentityExpected = computeCellIdentityFromEnv();
        if (cfg.mode == FixtureMode::Beir) {
            auto* daemon = harness.daemon();
            auto* sm = daemon ? daemon->getServiceManager() : nullptr;
            if (sm && !fixtureLoaded) {
                auto rebuilt = sm->rebuildSemanticNeighborGraph("bench_corpus_complete");
                if (!rebuilt) {
                    spdlog::warn("Bench: corpus-wide semantic neighbor rebuild failed: {}",
                                 rebuilt.error().message);
                } else {
                    spdlog::info("Bench: corpus-wide semantic neighbor rebuild produced {} edges",
                                 rebuilt.value());
                }
            }
            outcome.topologySeeded =
                runSyncTopologyRebuild(sm, outcome.cellIdentityExpected, outcome);
        } else {
            if (!fixtureLoaded) {
                outcome.topologySeeded = seedTopologyArtifacts(harness, fx);
            } else {
                outcome.topologySeeded = true;
            }
            const bool requiresRebuild = cfg.useSimeonEmbeddings || !topologyConfig.empty();
            if (requiresRebuild) {
                auto* daemon = harness.daemon();
                auto* sm = daemon ? daemon->getServiceManager() : nullptr;
                (void)runSyncTopologyRebuild(sm, outcome.cellIdentityExpected, outcome);
            }
        }

        const std::string searchType = std::getenv("YAMS_BENCH_SEARCH_TYPE")
                                           ? std::getenv("YAMS_BENCH_SEARCH_TYPE")
                                           : "hybrid";

        const auto start = std::chrono::steady_clock::now();
        outcome.metrics = evaluate(client, fx, searchType);
        const auto end = std::chrono::steady_clock::now();
        outcome.hybridEvalMs = std::chrono::duration<double, std::milli>(end - start).count();
        outcome.ok = outcome.metrics.numQueries > 0;
        outcome.routedSeedCountTotal = outcome.metrics.routedSeedCountTotal;
        outcome.routedSeedCountQueries = outcome.metrics.routedSeedCountQueries;
        outcome.routedSeedCountMean =
            outcome.metrics.routedSeedCountQueries > 0
                ? static_cast<double>(outcome.metrics.routedSeedCountTotal) /
                      static_cast<double>(outcome.metrics.routedSeedCountQueries)
                : 0.0;
        outcome.tier1HashFingerprint = outcome.metrics.tier1FingerprintJoined;

        if (!fixtureLoaded) {
            harness.stop();
            saveBenchFixture(cfg, dataDir);
        }
    } catch (const std::exception& e) {
        spdlog::warn("Cell threw: {}", e.what());
        outcome.fixtureStatus = "missing";
    } catch (...) {
        outcome.fixtureStatus = "missing";
    }
    return outcome;
}

json toCellJson(const CellOutcome& outcome) {
    json j;
    j[std::string(irm::kIRFixtureStatus)] = outcome.fixtureStatus;
    j["topology_seeded"] = outcome.topologySeeded;
    j["cluster_count"] = outcome.clusterCount;
    j["documents_processed"] = outcome.documentsProcessed;
    j["documents_missing_embeddings"] = outcome.documentsMissingEmbeddings;
    j["cell_identity_expected"] = outcome.cellIdentityExpected;
    j["cell_identity_observed"] = outcome.cellIdentityObserved;
    j["cell_identity_match"] = !outcome.cellIdentityExpected.empty() &&
                               outcome.cellIdentityExpected == outcome.cellIdentityObserved;
    j["cluster_size_max"] = outcome.clusterSizeMax;
    j["cluster_size_p50"] = outcome.clusterSizeP50;
    j["cluster_size_p90"] = outcome.clusterSizeP90;
    j["singleton_count"] = outcome.singletonCount;
    j["singleton_ratio"] = outcome.singletonRatio;
    j["giant_cluster_ratio"] = outcome.giantClusterRatio;
    j["cluster_size_gini"] = outcome.clusterSizeGini;
    j["avg_intra_edge_weight"] = outcome.avgIntraEdgeWeight;
    j["routed_seed_count_total"] = outcome.routedSeedCountTotal;
    j["routed_seed_count_queries"] = outcome.routedSeedCountQueries;
    j["routed_seed_count_mean"] = outcome.routedSeedCountMean;
    j["tier1_hash_fingerprint"] = outcome.tier1HashFingerprint;
    j["orphan_doc_count"] = outcome.orphanDocCount;
    j["topology_algorithm"] = outcome.algorithm;
    if (outcome.ok) {
        j[std::string(irm::kIRNdcgAtK)] = outcome.metrics.ndcgAtK;
        j[std::string(irm::kIRMrrAtK)] = outcome.metrics.mrrAtK;
        j[std::string(irm::kIRMap)] = outcome.metrics.map;
        j[std::string(irm::kIRRecallAtK)] = outcome.metrics.recallAtK;
        j[std::string(irm::kIRRecallAt100)] = outcome.metrics.recallAt100;
        j["num_queries"] = outcome.metrics.numQueries;
        j["hybrid_eval_ms"] = outcome.hybridEvalMs;
    } else {
        j[std::string(irm::kIRNdcgAtK)] = -1.0;
        j[std::string(irm::kIRMrrAtK)] = -1.0;
        j[std::string(irm::kIRMap)] = -1.0;
        j[std::string(irm::kIRRecallAtK)] = -1.0;
        j[std::string(irm::kIRRecallAt100)] = -1.0;
        j["num_queries"] = 0;
        j["hybrid_eval_ms"] = -1.0;
    }
    return j;
}

} // namespace

TEST_CASE("[!benchmark][topology-ablation-quality] axis-1 routing depth",
          "[topology-ablation-quality]") {
    const auto outputPath = resolveOutputPath();
    struct Point {
        std::size_t maxClusters;
        std::size_t maxDocs;
    };
    const std::vector<Point> grid = {
        {1, 32}, {1, 64}, {1, 128}, {2, 32}, {2, 64}, {2, 128}, {4, 32}, {4, 64}, {4, 128},
    };
    constexpr float kMidMedoidBoost = 0.05f;
    constexpr float kMidBridgeBoost = 0.03f;

    for (const auto& p : grid) {
        auto outcome = runCell(p.maxClusters, p.maxDocs, kMidMedoidBoost, kMidBridgeBoost);
        json record = {
            {std::string(irm::kIRTestKey), std::string(irm::kIRTestName)},
            {std::string(irm::kIRAxisKey), "routing_depth"},
            {std::string(irm::kIRAxisIdKey), 1},
            {"max_clusters", p.maxClusters},
            {"max_docs_cap", p.maxDocs},
            {"medoid_boost", kMidMedoidBoost},
            {"bridge_boost", kMidBridgeBoost},
        };
        record.update(toCellJson(outcome));
        emitRecord(outputPath, record);
    }
}

TEST_CASE("[!benchmark][topology-ablation-quality] axis-2 role-score weights",
          "[topology-ablation-quality]") {
    const auto outputPath = resolveOutputPath();
    const std::vector<float> medoidGrid = {0.0f, 0.025f, 0.05f, 0.075f, 0.10f};
    const std::vector<float> bridgeGrid = {0.0f, 0.015f, 0.03f, 0.045f, 0.06f};
    constexpr std::size_t kMidClusters = 2;
    constexpr std::size_t kMidDocs = 64;

    for (const auto m : medoidGrid) {
        for (const auto b : bridgeGrid) {
            auto outcome = runCell(kMidClusters, kMidDocs, m, b);
            json record = {
                {std::string(irm::kIRTestKey), std::string(irm::kIRTestName)},
                {std::string(irm::kIRAxisKey), "role_score_weights"},
                {std::string(irm::kIRAxisIdKey), 2},
                {"max_clusters", kMidClusters},
                {"max_docs_cap", kMidDocs},
                {"medoid_boost", m},
                {"bridge_boost", b},
            };
            record.update(toCellJson(outcome));
            emitRecord(outputPath, record);
        }
    }
}

TEST_CASE("[!benchmark][topology-ablation-quality] axis-3 routing variants",
          "[topology-ablation-quality]") {
    const auto outputPath = resolveOutputPath();
    constexpr std::size_t kMidClusters = 2;
    constexpr std::size_t kMidDocs = 64;
    constexpr float kMidMedoidBoost = 0.05f;
    constexpr float kMidBridgeBoost = 0.03f;
    const std::array<const char*, 5> variants = {
        "baseline", "vector_seed", "kg_walk", "score_replace", "medoid_promote",
    };

    for (const char* v : variants) {
        auto outcome = runCell(kMidClusters, kMidDocs, kMidMedoidBoost, kMidBridgeBoost, v);
        json record = {
            {std::string(irm::kIRTestKey), std::string(irm::kIRTestName)},
            {std::string(irm::kIRAxisKey), "routing_variants"},
            {std::string(irm::kIRAxisIdKey), 3},
            {"max_clusters", kMidClusters},
            {"max_docs_cap", kMidDocs},
            {"medoid_boost", kMidMedoidBoost},
            {"bridge_boost", kMidBridgeBoost},
            {"variant", std::string(v)},
        };
        record.update(toCellJson(outcome));
        emitRecord(outputPath, record);
    }
}

namespace {

void applyGraphBuildEnv(int topK, const std::string& thresholdLabel) {
    setenv("YAMS_GRAPH_SEMANTIC_TOPK", std::to_string(topK).c_str(), 1);
    if (thresholdLabel == "adaptive") {
        unsetenv("YAMS_GRAPH_SEMANTIC_THRESHOLD");
    } else {
        setenv("YAMS_GRAPH_SEMANTIC_THRESHOLD", thresholdLabel.c_str(), 1);
    }
}

void clearGraphBuildEnv() {
    unsetenv("YAMS_GRAPH_SEMANTIC_TOPK");
    unsetenv("YAMS_GRAPH_SEMANTIC_THRESHOLD");
}

} // namespace

TEST_CASE("[!benchmark][topology-ablation-quality] axis-4 graph build sweep",
          "[topology-ablation-quality]") {
    const auto outputPath = resolveOutputPath();
    constexpr std::size_t kMidClusters = 2;
    constexpr std::size_t kMidDocs = 64;
    constexpr float kMidMedoidBoost = 0.05f;
    constexpr float kMidBridgeBoost = 0.03f;

    const std::vector<int> topKGrid = {4, 16, 32, 64, 128};
    const std::vector<std::string> thresholdGrid = {"adaptive", "0.3", "0.5", "0.7"};

    const std::string tier = std::getenv("YAMS_BENCH_TIER") ? std::getenv("YAMS_BENCH_TIER") : "S";

    FixtureConfig cfg = fixtureConfigFromEnv();
    if (tier == "XL" || tier == "XXL") {
        cfg.mode = FixtureMode::Beir;
        cfg.useSimeonEmbeddings = true;
        if (const char* raw = std::getenv("YAMS_BENCH_BEIR_DATASET"); !(raw && *raw)) {
            cfg.beirDataset = (tier == "XL") ? "nfcorpus" : "scifact";
        }
        if (cfg.beirCacheDir.empty() || cfg.beirCacheDir.filename().string() != cfg.beirDataset) {
            if (const char* home = std::getenv("HOME"); home && *home) {
                cfg.beirCacheDir =
                    fs::path(home) / ".cache" / "yams" / "benchmarks" / cfg.beirDataset;
            }
        }
    } else if (tier == "L+") {
        cfg.useSimeonEmbeddings = true;
    }

    const std::size_t expectedCorpusSize = [&]() -> std::size_t {
        if (cfg.mode == FixtureMode::Beir) {
            return 0;
        }
        const int perTopic = cfg.corePerTopic + cfg.bridgePerTopic + cfg.tangentialPerTopic + 1;
        return static_cast<std::size_t>(cfg.topicCount) * static_cast<std::size_t>(perTopic) +
               static_cast<std::size_t>(cfg.noiseDocs);
    }();

    const std::string topKFilter = std::getenv("YAMS_BENCH_GRAPH_TOPK_FILTER")
                                       ? std::getenv("YAMS_BENCH_GRAPH_TOPK_FILTER")
                                       : "";
    const std::string thresholdFilter = std::getenv("YAMS_BENCH_GRAPH_THRESHOLD_FILTER")
                                            ? std::getenv("YAMS_BENCH_GRAPH_THRESHOLD_FILTER")
                                            : "";
    const int cellCooldownMs = readEnvInt("YAMS_BENCH_VARIANT_COOLDOWN_MS", 0);
    bool firstEmitted = false;

    for (const int topK : topKGrid) {
        if (!topKFilter.empty() && topKFilter.find(std::to_string(topK)) == std::string::npos) {
            continue;
        }
        for (const auto& threshold : thresholdGrid) {
            if (!thresholdFilter.empty() && thresholdFilter.find(threshold) == std::string::npos) {
                continue;
            }
            if (firstEmitted && cellCooldownMs > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(cellCooldownMs));
            }
            firstEmitted = true;

            applyGraphBuildEnv(topK, threshold);
            auto outcome =
                runCell(kMidClusters, kMidDocs, kMidMedoidBoost, kMidBridgeBoost, nullptr, cfg);
            clearGraphBuildEnv();

            json record = {
                {std::string(irm::kIRTestKey), std::string(irm::kIRTestName)},
                {std::string(irm::kIRAxisKey), "graph_build_sweep"},
                {std::string(irm::kIRAxisIdKey), 4},
                {"tier", tier},
                {"fixture_mode", (cfg.mode == FixtureMode::Beir) ? "beir" : "synthetic"},
                {"embedding_backend", configUsesRealEmbeddings(cfg) ? "simeon" : "mock"},
                {"topic_count", cfg.topicCount},
                {"docs_per_topic_core", cfg.corePerTopic},
                {"docs_per_topic_bridge", cfg.bridgePerTopic},
                {"docs_per_topic_tangent", cfg.tangentialPerTopic},
                {"noise_docs", cfg.noiseDocs},
                {"corpus_size_expected", expectedCorpusSize},
                {"beir_dataset", (cfg.mode == FixtureMode::Beir) ? cfg.beirDataset : ""},
                {"max_clusters", kMidClusters},
                {"max_docs_cap", kMidDocs},
                {"medoid_boost", kMidMedoidBoost},
                {"bridge_boost", kMidBridgeBoost},
                {"graph_topk", topK},
                {"graph_threshold", threshold},
            };
            record.update(toCellJson(outcome));
            emitRecord(outputPath, record);
        }
    }
}

TEST_CASE("[!benchmark][topology-ablation-quality] axis-5 integration pattern sweep",
          "[topology-ablation-quality]") {
    const auto outputPath = resolveOutputPath();
    constexpr std::size_t kMidClusters = 2;
    constexpr std::size_t kMidDocs = 64;
    constexpr float kMidMedoidBoost = 0.05f;
    constexpr float kMidBridgeBoost = 0.03f;
    constexpr int kDefaultRrfK = 60;

    const std::vector<std::string> integrationGrid = {"boost", "recall_expand", "rrf", "both"};
    const std::vector<int> recallGrid = {0, 4, 16};

    const std::string tier = std::getenv("YAMS_BENCH_TIER") ? std::getenv("YAMS_BENCH_TIER") : "S";

    FixtureConfig cfg = fixtureConfigFromEnv();
    if (tier == "XL" || tier == "XXL") {
        cfg.mode = FixtureMode::Beir;
        cfg.useSimeonEmbeddings = true;
        if (const char* raw = std::getenv("YAMS_BENCH_BEIR_DATASET"); !(raw && *raw)) {
            cfg.beirDataset = (tier == "XL") ? "nfcorpus" : "scifact";
        }
        if (cfg.beirCacheDir.empty() || cfg.beirCacheDir.filename().string() != cfg.beirDataset) {
            if (const char* home = std::getenv("HOME"); home && *home) {
                cfg.beirCacheDir =
                    fs::path(home) / ".cache" / "yams" / "benchmarks" / cfg.beirDataset;
            }
        }
    } else if (tier == "L+") {
        cfg.useSimeonEmbeddings = true;
    }

    const std::size_t expectedCorpusSize = [&]() -> std::size_t {
        if (cfg.mode == FixtureMode::Beir) {
            return 0;
        }
        const int perTopic = cfg.corePerTopic + cfg.bridgePerTopic + cfg.tangentialPerTopic + 1;
        return static_cast<std::size_t>(cfg.topicCount) * static_cast<std::size_t>(perTopic) +
               static_cast<std::size_t>(cfg.noiseDocs);
    }();

    const std::string integrationFilter = std::getenv("YAMS_BENCH_INTEGRATION_FILTER")
                                              ? std::getenv("YAMS_BENCH_INTEGRATION_FILTER")
                                              : "";
    const std::string recallFilter = std::getenv("YAMS_BENCH_RECALL_EXPAND_FILTER")
                                         ? std::getenv("YAMS_BENCH_RECALL_EXPAND_FILTER")
                                         : "";
    const int cellCooldownMs = readEnvInt("YAMS_BENCH_VARIANT_COOLDOWN_MS", 0);
    bool firstEmitted = false;

    for (const auto& integration : integrationGrid) {
        if (!integrationFilter.empty() &&
            integrationFilter.find(integration) == std::string::npos) {
            continue;
        }
        for (const int recallN : recallGrid) {
            if (!recallFilter.empty() &&
                recallFilter.find(std::to_string(recallN)) == std::string::npos) {
                continue;
            }
            if (firstEmitted && cellCooldownMs > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(cellCooldownMs));
            }
            firstEmitted = true;

            const int rrfK = (integration == "rrf" || integration == "both") ? kDefaultRrfK : 0;
            TopologyConfigOverride override;
            override.integration = integration;
            if (recallN > 0) {
                override.recallExpandPerCluster = recallN;
            }
            if (rrfK > 0) {
                override.rrfK = rrfK;
            }
            auto outcome = runCell(kMidClusters, kMidDocs, kMidMedoidBoost, kMidBridgeBoost,
                                   nullptr, cfg, override);

            json record = {
                {std::string(irm::kIRTestKey), std::string(irm::kIRTestName)},
                {std::string(irm::kIRAxisKey), "integration_pattern_sweep"},
                {std::string(irm::kIRAxisIdKey), 5},
                {"tier", tier},
                {"fixture_mode", (cfg.mode == FixtureMode::Beir) ? "beir" : "synthetic"},
                {"embedding_backend", configUsesRealEmbeddings(cfg) ? "simeon" : "mock"},
                {"topic_count", cfg.topicCount},
                {"docs_per_topic_core", cfg.corePerTopic},
                {"docs_per_topic_bridge", cfg.bridgePerTopic},
                {"docs_per_topic_tangent", cfg.tangentialPerTopic},
                {"noise_docs", cfg.noiseDocs},
                {"corpus_size_expected", expectedCorpusSize},
                {"beir_dataset", (cfg.mode == FixtureMode::Beir) ? cfg.beirDataset : ""},
                {"max_clusters", kMidClusters},
                {"max_docs_cap", kMidDocs},
                {"medoid_boost", kMidMedoidBoost},
                {"bridge_boost", kMidBridgeBoost},
                {"integration_pattern", integration},
                {"recall_expand_n", recallN},
                {"rrf_k", rrfK},
            };
            record.update(toCellJson(outcome));
            emitRecord(outputPath, record);
        }
    }
}

TEST_CASE("[!benchmark][topology-ablation-quality] axis-7 cluster route sweep",
          "[topology-ablation-quality]") {
    const auto outputPath = resolveOutputPath();
    constexpr float kMidMedoidBoost = 0.05f;
    constexpr float kMidBridgeBoost = 0.03f;

    const std::vector<std::size_t> routeKGrid = {1, 2, 4, 8, 16};
    const std::vector<std::size_t> maxDocsGrid = {64, 128, 256};
    const std::vector<std::string> scoringGrid = {"current", "size_weighted", "seed_coverage"};

    const std::string tier = std::getenv("YAMS_BENCH_TIER") ? std::getenv("YAMS_BENCH_TIER") : "S";

    FixtureConfig cfg = fixtureConfigFromEnv();
    if (tier == "XL" || tier == "XXL") {
        cfg.mode = FixtureMode::Beir;
        cfg.useSimeonEmbeddings = true;
        if (const char* raw = std::getenv("YAMS_BENCH_BEIR_DATASET"); !(raw && *raw)) {
            cfg.beirDataset = (tier == "XL") ? "nfcorpus" : "scifact";
        }
        if (cfg.beirCacheDir.empty() || cfg.beirCacheDir.filename().string() != cfg.beirDataset) {
            if (const char* home = std::getenv("HOME"); home && *home) {
                cfg.beirCacheDir =
                    fs::path(home) / ".cache" / "yams" / "benchmarks" / cfg.beirDataset;
            }
        }
    } else if (tier == "L+") {
        cfg.useSimeonEmbeddings = true;
    }

    const std::size_t expectedCorpusSize = [&]() -> std::size_t {
        if (cfg.mode == FixtureMode::Beir) {
            return 0;
        }
        const int perTopic = cfg.corePerTopic + cfg.bridgePerTopic + cfg.tangentialPerTopic + 1;
        return static_cast<std::size_t>(cfg.topicCount) * static_cast<std::size_t>(perTopic) +
               static_cast<std::size_t>(cfg.noiseDocs);
    }();

    const std::string routeKFilter =
        std::getenv("YAMS_BENCH_ROUTE_K_FILTER") ? std::getenv("YAMS_BENCH_ROUTE_K_FILTER") : "";
    const std::string maxDocsFilter = std::getenv("YAMS_BENCH_ROUTE_MAX_DOCS_FILTER")
                                          ? std::getenv("YAMS_BENCH_ROUTE_MAX_DOCS_FILTER")
                                          : "";
    const std::string scoringFilter = std::getenv("YAMS_BENCH_ROUTE_SCORING_FILTER")
                                          ? std::getenv("YAMS_BENCH_ROUTE_SCORING_FILTER")
                                          : "";
    const int cellCooldownMs = readEnvInt("YAMS_BENCH_VARIANT_COOLDOWN_MS", 0);
    bool firstEmitted = false;

    for (const auto routeK : routeKGrid) {
        if (!routeKFilter.empty() &&
            routeKFilter.find(std::to_string(routeK)) == std::string::npos) {
            continue;
        }
        for (const auto maxDocs : maxDocsGrid) {
            if (!maxDocsFilter.empty() &&
                maxDocsFilter.find(std::to_string(maxDocs)) == std::string::npos) {
                continue;
            }
            for (const auto& scoring : scoringGrid) {
                if (!scoringFilter.empty() && scoringFilter.find(scoring) == std::string::npos) {
                    continue;
                }
                if (firstEmitted && cellCooldownMs > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(cellCooldownMs));
                }
                firstEmitted = true;

                TopologyConfigOverride override;
                override.routeScoring = scoring;
                auto outcome = runCell(routeK, maxDocs, kMidMedoidBoost, kMidBridgeBoost, nullptr,
                                       cfg, override);

                json record = {
                    {std::string(irm::kIRTestKey), std::string(irm::kIRTestName)},
                    {std::string(irm::kIRAxisKey), "cluster_route_sweep"},
                    {std::string(irm::kIRAxisIdKey), 7},
                    {"tier", tier},
                    {"fixture_mode", (cfg.mode == FixtureMode::Beir) ? "beir" : "synthetic"},
                    {"embedding_backend", configUsesRealEmbeddings(cfg) ? "simeon" : "mock"},
                    {"topic_count", cfg.topicCount},
                    {"docs_per_topic_core", cfg.corePerTopic},
                    {"docs_per_topic_bridge", cfg.bridgePerTopic},
                    {"docs_per_topic_tangent", cfg.tangentialPerTopic},
                    {"noise_docs", cfg.noiseDocs},
                    {"corpus_size_expected", expectedCorpusSize},
                    {"beir_dataset", (cfg.mode == FixtureMode::Beir) ? cfg.beirDataset : ""},
                    {"route_k", routeK},
                    {"max_docs_cap", maxDocs},
                    {"route_scoring", scoring},
                    {"medoid_boost", kMidMedoidBoost},
                    {"bridge_boost", kMidBridgeBoost},
                };
                record.update(toCellJson(outcome));
                emitRecord(outputPath, record);
            }
        }
    }
}

TEST_CASE("[!benchmark][topology-ablation-quality] axis-8 cluster engine sweep",
          "[topology-ablation-quality]") {
    const auto outputPath = resolveOutputPath();
    constexpr float kMidMedoidBoost = 0.05f;
    constexpr float kMidBridgeBoost = 0.03f;

    struct EngineCell {
        std::string engine;
        std::optional<int> kmeansK; // only meaningful for kmeans_embedding
        std::string label;
    };
    const std::vector<EngineCell> engineGrid = {
        {"connected", std::nullopt, "connected"},
        {"louvain", std::nullopt, "louvain"},
        {"label_propagation", std::nullopt, "label_propagation"},
        {"kmeans_embedding", 0, "kmeans_embedding_auto"},
        {"kmeans_embedding", 8, "kmeans_embedding_k8"},
        {"kmeans_embedding", 32, "kmeans_embedding_k32"},
    };

    const auto envInt = [](const char* name, int fallback) {
        if (const char* raw = std::getenv(name); raw && *raw) {
            try {
                return std::stoi(raw);
            } catch (...) {
            }
        }
        return fallback;
    };
    const auto envStr = [](const char* name, const char* fallback) -> std::string {
        if (const char* raw = std::getenv(name); raw && *raw) {
            return std::string{raw};
        }
        return std::string{fallback};
    };
    const auto routeK = static_cast<std::size_t>(envInt("YAMS_BENCH_AXIS8_ROUTE_K", 4));
    const auto maxDocs = static_cast<std::size_t>(envInt("YAMS_BENCH_AXIS8_MAX_DOCS", 128));
    const std::string routeScoring = envStr("YAMS_BENCH_AXIS8_ROUTE_SCORING", "current");

    const std::string tier = std::getenv("YAMS_BENCH_TIER") ? std::getenv("YAMS_BENCH_TIER") : "S";

    FixtureConfig cfg = fixtureConfigFromEnv();
    if (tier == "XL" || tier == "XXL") {
        cfg.mode = FixtureMode::Beir;
        cfg.useSimeonEmbeddings = true;
        if (const char* raw = std::getenv("YAMS_BENCH_BEIR_DATASET"); !(raw && *raw)) {
            cfg.beirDataset = (tier == "XL") ? "nfcorpus" : "scifact";
        }
        if (cfg.beirCacheDir.empty() || cfg.beirCacheDir.filename().string() != cfg.beirDataset) {
            if (const char* home = std::getenv("HOME"); home && *home) {
                cfg.beirCacheDir =
                    fs::path(home) / ".cache" / "yams" / "benchmarks" / cfg.beirDataset;
            }
        }
    } else if (tier == "L+") {
        cfg.useSimeonEmbeddings = true;
    }

    const std::size_t expectedCorpusSize = [&]() -> std::size_t {
        if (cfg.mode == FixtureMode::Beir) {
            return 0;
        }
        const int perTopic = cfg.corePerTopic + cfg.bridgePerTopic + cfg.tangentialPerTopic + 1;
        return static_cast<std::size_t>(cfg.topicCount) * static_cast<std::size_t>(perTopic) +
               static_cast<std::size_t>(cfg.noiseDocs);
    }();

    const std::string engineFilter =
        std::getenv("YAMS_BENCH_ENGINE_FILTER") ? std::getenv("YAMS_BENCH_ENGINE_FILTER") : "";
    const int cellCooldownMs = readEnvInt("YAMS_BENCH_VARIANT_COOLDOWN_MS", 0);
    bool firstEmitted = false;

    for (const auto& cell : engineGrid) {
        if (!engineFilter.empty() && engineFilter.find(cell.label) == std::string::npos) {
            continue;
        }
        if (firstEmitted && cellCooldownMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(cellCooldownMs));
        }
        firstEmitted = true;

        TopologyConfigOverride override;
        override.engine = cell.engine;
        override.kmeansK = cell.kmeansK;
        override.routeScoring = routeScoring;
        auto outcome =
            runCell(routeK, maxDocs, kMidMedoidBoost, kMidBridgeBoost, nullptr, cfg, override);

        json record = {
            {std::string(irm::kIRTestKey), std::string(irm::kIRTestName)},
            {std::string(irm::kIRAxisKey), "cluster_engine_sweep"},
            {std::string(irm::kIRAxisIdKey), 8},
            {"tier", tier},
            {"fixture_mode", (cfg.mode == FixtureMode::Beir) ? "beir" : "synthetic"},
            {"embedding_backend", configUsesRealEmbeddings(cfg) ? "simeon" : "mock"},
            {"topic_count", cfg.topicCount},
            {"docs_per_topic_core", cfg.corePerTopic},
            {"docs_per_topic_bridge", cfg.bridgePerTopic},
            {"docs_per_topic_tangent", cfg.tangentialPerTopic},
            {"noise_docs", cfg.noiseDocs},
            {"corpus_size_expected", expectedCorpusSize},
            {"beir_dataset", (cfg.mode == FixtureMode::Beir) ? cfg.beirDataset : ""},
            {"topology_engine_requested", cell.engine},
            {"kmeans_k_requested", cell.kmeansK.value_or(-1)},
            {"route_k", routeK},
            {"max_docs_cap", maxDocs},
            {"route_scoring", routeScoring},
            {"medoid_boost", kMidMedoidBoost},
            {"bridge_boost", kMidBridgeBoost},
        };
        record.update(toCellJson(outcome));
        emitRecord(outputPath, record);
    }
}

TEST_CASE("[!benchmark][topology-ablation-quality] axis-3 routing variants on corpus ladder",
          "[topology-ablation-quality]") {
    const auto outputPath = resolveOutputPath();
    constexpr std::size_t kMidClusters = 2;
    constexpr std::size_t kMidDocs = 64;
    constexpr float kMidMedoidBoost = 0.05f;
    constexpr float kMidBridgeBoost = 0.03f;
    const std::array<const char*, 5> variants = {
        "baseline", "vector_seed", "kg_walk", "score_replace", "medoid_promote",
    };

    const std::string tier = std::getenv("YAMS_BENCH_TIER") ? std::getenv("YAMS_BENCH_TIER") : "S";

    FixtureConfig cfg = fixtureConfigFromEnv();
    if (tier == "XL" || tier == "XXL") {
        cfg.mode = FixtureMode::Beir;
        cfg.useSimeonEmbeddings = true;
        if (const char* raw = std::getenv("YAMS_BENCH_BEIR_DATASET"); !(raw && *raw)) {
            cfg.beirDataset = (tier == "XL") ? "nfcorpus" : "scifact";
        }
        if (cfg.beirCacheDir.empty() || cfg.beirCacheDir.filename().string() != cfg.beirDataset) {
            if (const char* home = std::getenv("HOME"); home && *home) {
                cfg.beirCacheDir =
                    fs::path(home) / ".cache" / "yams" / "benchmarks" / cfg.beirDataset;
            }
        }
    } else if (tier == "L+") {
        cfg.useSimeonEmbeddings = true;
    }

    const std::size_t expectedCorpusSize = [&]() -> std::size_t {
        if (cfg.mode == FixtureMode::Beir) {
            return 0;
        }
        const int perTopic = cfg.corePerTopic + cfg.bridgePerTopic + cfg.tangentialPerTopic + 1;
        return static_cast<std::size_t>(cfg.topicCount) * static_cast<std::size_t>(perTopic) +
               static_cast<std::size_t>(cfg.noiseDocs);
    }();

    const std::string variantFilter =
        std::getenv("YAMS_BENCH_VARIANT_FILTER") ? std::getenv("YAMS_BENCH_VARIANT_FILTER") : "";
    const int variantCooldownMs = readEnvInt("YAMS_BENCH_VARIANT_COOLDOWN_MS", 0);
    bool firstEmitted = false;
    for (const char* v : variants) {
        if (!variantFilter.empty() && variantFilter.find(v) == std::string::npos) {
            continue;
        }
        if (firstEmitted && variantCooldownMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(variantCooldownMs));
        }
        firstEmitted = true;
        auto outcome = runCell(kMidClusters, kMidDocs, kMidMedoidBoost, kMidBridgeBoost, v, cfg);
        json record = {
            {std::string(irm::kIRTestKey), std::string(irm::kIRTestName)},
            {std::string(irm::kIRAxisKey), "routing_variants_ladder"},
            {std::string(irm::kIRAxisIdKey), 3},
            {"tier", tier},
            {"fixture_mode", (cfg.mode == FixtureMode::Beir) ? "beir" : "synthetic"},
            {"embedding_backend", configUsesRealEmbeddings(cfg) ? "simeon" : "mock"},
            {"topic_count", cfg.topicCount},
            {"docs_per_topic_core", cfg.corePerTopic},
            {"docs_per_topic_bridge", cfg.bridgePerTopic},
            {"docs_per_topic_tangent", cfg.tangentialPerTopic},
            {"noise_docs", cfg.noiseDocs},
            {"corpus_size_expected", expectedCorpusSize},
            {"beir_dataset", (cfg.mode == FixtureMode::Beir) ? cfg.beirDataset : ""},
            {"max_clusters", kMidClusters},
            {"max_docs_cap", kMidDocs},
            {"medoid_boost", kMidMedoidBoost},
            {"bridge_boost", kMidBridgeBoost},
            {"variant", std::string(v)},
        };
        record.update(toCellJson(outcome));
        emitRecord(outputPath, record);
    }
}
