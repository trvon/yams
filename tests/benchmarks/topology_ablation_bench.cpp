// Topology ablation bench — isolated parameter sweep over the topology
// subsystem. Exercises StableClusterTopologyRouter::route and the
// role-score boost application independent of the full search engine,
// daemon, or corpus.
//
// Paired with docs/benchmarks/topology_ablation_matrix.md. Sweeps:
//   - Axis 1: router limit (topologyWeakQueryMaxClusters)
//            × max docs (topologyWeakQueryMaxDocs)
//   - Axis 2: medoid boost × bridge boost
//
// Axis 5 (KG rerank) is skipped in v1; it requires a populated KG
// store. See matrix doc appendix for the follow-up hooks.
//
// Environment:
//   YAMS_BENCH_OUTPUT            - JSONL output path (default:
//                                 bench_results/topology_ablation.jsonl)
//   YAMS_BENCH_TOPO_CLUSTERS     - synthetic cluster count (default 32)
//   YAMS_BENCH_TOPO_DOCS_PER    - members per cluster (default 16)
//   YAMS_BENCH_TOPO_BRIDGE_RATIO - bridge fraction per cluster (0..1, default 0.1)
//   YAMS_BENCH_RUN_ID            - stamped into every record
//
// Run:
//   YAMS_BENCH_OUTPUT=/tmp/topology_ablation.jsonl \
//     ./build/debug/tests/benchmarks/topology_ablation_bench \
//     "[!benchmark][topology-ablation]" --allow-running-no-tests

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using yams::topology::ClusterArtifact;
using yams::topology::ClusterRepresentative;
using yams::topology::DocumentClusterMembership;
using yams::topology::DocumentTopologyRole;
using yams::topology::StableClusterTopologyRouter;
using yams::topology::TopologyArtifactBatch;
using yams::topology::TopologyRouteRequest;

namespace {

fs::path resolveOutputPath() {
    if (const char* override = std::getenv("YAMS_BENCH_OUTPUT"); override && *override) {
        return fs::path(override);
    }
    return fs::path("bench_results/topology_ablation.jsonl");
}

std::size_t envSize(const char* name, std::size_t fallback) {
    if (const char* v = std::getenv(name); v && *v) {
        try {
            auto parsed = std::stoull(v);
            return static_cast<std::size_t>(parsed);
        } catch (...) {
            return fallback;
        }
    }
    return fallback;
}

double envDouble(const char* name, double fallback) {
    if (const char* v = std::getenv(name); v && *v) {
        try {
            return std::stod(v);
        } catch (...) {
            return fallback;
        }
    }
    return fallback;
}

std::string makeHash(std::size_t clusterIdx, std::size_t docIdx) {
    std::ostringstream oss;
    oss << "sha256_" << std::setfill('0') << std::setw(4) << clusterIdx << "_" << std::setw(6)
        << docIdx;
    return oss.str();
}

TopologyArtifactBatch synthesizeBatch(std::size_t numClusters, std::size_t docsPerCluster,
                                      double bridgeRatio) {
    TopologyArtifactBatch batch;
    batch.snapshotId = "ablation_synth";
    batch.algorithm = "synthetic_ablation_v1";
    batch.generatedAtUnixSeconds =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count());
    batch.topologyEpoch = 1;
    batch.clusters.reserve(numClusters);
    batch.memberships.reserve(numClusters * docsPerCluster);

    std::mt19937 rng(0xA11BA7);
    std::uniform_real_distribution<double> persistenceDist(0.3, 1.0);
    std::uniform_real_distribution<double> cohesionDist(0.2, 1.0);

    for (std::size_t c = 0; c < numClusters; ++c) {
        ClusterArtifact cluster;
        cluster.clusterId = "cluster_" + std::to_string(c);
        cluster.level = 0;
        cluster.memberCount = docsPerCluster;
        cluster.persistenceScore = persistenceDist(rng);
        cluster.cohesionScore = cohesionDist(rng);
        cluster.bridgeMass = bridgeRatio;

        const auto medoidHash = makeHash(c, 0);
        cluster.medoid = ClusterRepresentative{.clusterId = cluster.clusterId,
                                               .documentHash = medoidHash,
                                               .filePath = "/synth/" + medoidHash,
                                               .representativeScore = 1.0};

        const auto bridgeCount =
            static_cast<std::size_t>(static_cast<double>(docsPerCluster) * bridgeRatio);

        for (std::size_t d = 0; d < docsPerCluster; ++d) {
            const auto hash = makeHash(c, d);
            cluster.memberDocumentHashes.push_back(hash);

            DocumentClusterMembership m;
            m.documentHash = hash;
            m.clusterId = cluster.clusterId;
            m.clusterLevel = 0;
            m.persistenceScore = cluster.persistenceScore;
            m.cohesionScore = cluster.cohesionScore;
            m.bridgeScore = (d > 0 && d <= bridgeCount) ? 0.8 : 0.1;
            if (d == 0) {
                m.role = DocumentTopologyRole::Medoid;
            } else if (d <= bridgeCount) {
                m.role = DocumentTopologyRole::Bridge;
            } else {
                m.role = DocumentTopologyRole::Core;
            }
            batch.memberships.push_back(std::move(m));
        }
        batch.clusters.push_back(std::move(cluster));
    }

    return batch;
}

void emitRecord(const fs::path& outputPath, const json& record) {
    json enriched = record;
    enriched["schema_version"] = "topology_ablation_v1";
    if (const char* v = std::getenv("YAMS_BENCH_RUN_ID"); v && *v) {
        enriched["run_id"] = v;
    }
    if (const char* v = std::getenv("YAMS_BENCH_PHASE"); v && *v) {
        enriched["run_phase"] = v;
    }
    if (const char* v = std::getenv("YAMS_BENCH_CASE"); v && *v) {
        enriched["run_case"] = v;
    }
    fs::create_directories(outputPath.parent_path());
    std::ofstream ofs(outputPath, std::ios::app);
    ofs << enriched.dump() << '\n';
}

struct LatencySummary {
    double p50Us{0.0};
    double p95Us{0.0};
    double p99Us{0.0};
    double meanUs{0.0};
    std::size_t samples{0};
};

LatencySummary summarizeUs(std::vector<double>& samples) {
    LatencySummary s;
    s.samples = samples.size();
    if (samples.empty()) {
        return s;
    }
    std::sort(samples.begin(), samples.end());
    auto pick = [&](double q) {
        const auto idx = static_cast<std::size_t>(q * (samples.size() - 1));
        return samples[std::min(idx, samples.size() - 1)];
    };
    s.p50Us = pick(0.50);
    s.p95Us = pick(0.95);
    s.p99Us = pick(0.99);
    s.meanUs =
        std::accumulate(samples.begin(), samples.end(), 0.0) / static_cast<double>(samples.size());
    return s;
}

} // namespace

// ─────────────────────────────────────────────────────────────────────────────
// Axis 1 — routing depth grid
// ─────────────────────────────────────────────────────────────────────────────

TEST_CASE("[!benchmark][topology-ablation] axis-1 routing depth sweep", "[topology-ablation]") {
    const auto numClusters = envSize("YAMS_BENCH_TOPO_CLUSTERS", 32);
    const auto docsPerCluster = envSize("YAMS_BENCH_TOPO_DOCS_PER", 16);
    const auto bridgeRatio = envDouble("YAMS_BENCH_TOPO_BRIDGE_RATIO", 0.1);
    const auto batch = synthesizeBatch(numClusters, docsPerCluster, bridgeRatio);
    const auto outputPath = resolveOutputPath();

    // Seed with first-cluster medoid hashes so the router has an anchor.
    std::vector<std::string> seeds;
    for (std::size_t c = 0; c < std::min<std::size_t>(4, numClusters); ++c) {
        seeds.push_back(makeHash(c, 0));
    }

    struct Point {
        std::size_t limit;
        std::size_t maxDocs;
    };
    const std::vector<Point> grid = {
        {1, 32}, {1, 64}, {1, 128}, {2, 32}, {2, 64}, {2, 128}, {4, 32}, {4, 64}, {4, 128},
    };

    StableClusterTopologyRouter router;
    constexpr std::size_t kIterations = 200;

    for (const auto& point : grid) {
        std::vector<double> latencyUs;
        latencyUs.reserve(kIterations);

        std::size_t totalRoutes = 0;
        std::size_t totalRoutedDocs = 0;
        for (std::size_t iter = 0; iter < kIterations; ++iter) {
            TopologyRouteRequest request;
            request.queryText = "synthetic";
            request.seedDocumentHashes = seeds;
            request.limit = point.limit;
            request.weakQueryOnly = true;

            const auto start = std::chrono::steady_clock::now();
            auto result = router.route(request, batch);
            const auto end = std::chrono::steady_clock::now();
            REQUIRE(result.has_value());
            latencyUs.push_back(std::chrono::duration<double, std::micro>(end - start).count());
            totalRoutes += result.value().size();
            for (const auto& r : result.value()) {
                totalRoutedDocs += std::min(r.memberCount, point.maxDocs);
            }
        }

        const auto summary = summarizeUs(latencyUs);

        json record = {
            {"test", "topology_ablation"},
            {"axis", "routing_depth"},
            {"axis_id", 1},
            {"max_clusters", point.limit},
            {"max_docs_cap", point.maxDocs},
            {"synthetic_clusters", numClusters},
            {"synthetic_docs_per_cluster", docsPerCluster},
            {"synthetic_bridge_ratio", bridgeRatio},
            {"iterations", kIterations},
            {"route_latency_us",
             {{"p50", summary.p50Us},
              {"p95", summary.p95Us},
              {"p99", summary.p99Us},
              {"mean", summary.meanUs},
              {"samples", summary.samples}}},
            {"avg_routes_per_call",
             static_cast<double>(totalRoutes) / static_cast<double>(kIterations)},
            {"avg_routed_docs_per_call",
             static_cast<double>(totalRoutedDocs) / static_cast<double>(kIterations)},
        };
        emitRecord(outputPath, record);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Axis 2 — role-score weight sweep
// ─────────────────────────────────────────────────────────────────────────────

TEST_CASE("[!benchmark][topology-ablation] axis-2 role-score weights", "[topology-ablation]") {
    const auto numClusters = envSize("YAMS_BENCH_TOPO_CLUSTERS", 32);
    const auto docsPerCluster = envSize("YAMS_BENCH_TOPO_DOCS_PER", 16);
    const auto bridgeRatio = envDouble("YAMS_BENCH_TOPO_BRIDGE_RATIO", 0.1);
    const auto batch = synthesizeBatch(numClusters, docsPerCluster, bridgeRatio);
    const auto outputPath = resolveOutputPath();

    // Base score that would come from graphExpansionFtsPenalty * topologyRoutedBaseMultiplier.
    constexpr float kBaseScore = 0.50f;

    const std::vector<float> medoidGrid = {0.0f, 0.025f, 0.05f, 0.075f, 0.10f};
    const std::vector<float> bridgeGrid = {0.0f, 0.015f, 0.03f, 0.045f, 0.06f};

    for (const auto medoidBoost : medoidGrid) {
        for (const auto bridgeBoost : bridgeGrid) {
            double medoidScoreSum = 0.0;
            double bridgeScoreSum = 0.0;
            double coreScoreSum = 0.0;
            std::size_t medoidCount = 0;
            std::size_t bridgeCount = 0;
            std::size_t coreCount = 0;
            float maxScore = 0.0f;
            float minScore = 1.0f;

            for (const auto& m : batch.memberships) {
                float score = kBaseScore;
                switch (m.role) {
                    case DocumentTopologyRole::Medoid:
                        score = std::clamp(score + medoidBoost, 0.0f, 1.0f);
                        medoidScoreSum += score;
                        ++medoidCount;
                        break;
                    case DocumentTopologyRole::Bridge:
                        score = std::clamp(score + bridgeBoost, 0.0f, 1.0f);
                        bridgeScoreSum += score;
                        ++bridgeCount;
                        break;
                    case DocumentTopologyRole::Core:
                        coreScoreSum += score;
                        ++coreCount;
                        break;
                    case DocumentTopologyRole::Outlier:
                        break;
                }
                maxScore = std::max(maxScore, score);
                minScore = std::min(minScore, score);
            }

            auto safeAvg = [](double sum, std::size_t n) {
                return n == 0 ? 0.0 : sum / static_cast<double>(n);
            };

            const double medoidAvg = safeAvg(medoidScoreSum, medoidCount);
            const double bridgeAvg = safeAvg(bridgeScoreSum, bridgeCount);
            const double coreAvg = safeAvg(coreScoreSum, coreCount);

            json record = {
                {"test", "topology_ablation"},
                {"axis", "role_score_weights"},
                {"axis_id", 2},
                {"medoid_boost", medoidBoost},
                {"bridge_boost", bridgeBoost},
                {"base_score", kBaseScore},
                {"counts", {{"medoid", medoidCount}, {"bridge", bridgeCount}, {"core", coreCount}}},
                {"avg_score", {{"medoid", medoidAvg}, {"bridge", bridgeAvg}, {"core", coreAvg}}},
                {"score_spread",
                 {{"min", minScore},
                  {"max", maxScore},
                  {"medoid_minus_core", medoidAvg - coreAvg},
                  {"bridge_minus_core", bridgeAvg - coreAvg}}},
            };
            emitRecord(outputPath, record);
        }
    }
}
