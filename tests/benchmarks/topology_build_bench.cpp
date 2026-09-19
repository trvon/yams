// Focused benchmark for ConnectedComponentTopologyEngine::buildArtifacts() construction cost.
//
// This deliberately excludes database extraction, artifact persistence, topology routing, and
// retrieval quality. Profile this executable to attribute construction work without daemon noise.
//
// Production-safe construction controls are encoded explicitly rather than read from product
// environment variables: minEdgeScore=0.25, maxComponentDocs=64, reciprocalOnly=true.
// Harness-only controls:
//   YAMS_BENCH_TOPO_BUILD_DOCS     largest document scale (default 4096)
//   YAMS_BENCH_TOPO_BUILD_REPEATS measured builds per point (default 30)
//   YAMS_BENCH_TOPO_BUILD_DIMENSIONS embedding dimensions (default 1024)
//   YAMS_BENCH_TOPO_BUILD_NEIGHBORS neighbors per document (default 32)
//   YAMS_BENCH_TOPO_BUILD_SHAPE   optional fragmented_singletons,
//                                 clustered_low_score_noise, giant_component, or
//                                 many_oversized_components filter
//   YAMS_BENCH_TOPO_BUILD_CONFIG  optional safe_defaults/unbounded_control filter
//   YAMS_BENCH_TOPO_BUILD_ONLY_MAX run only the largest scale when set to 1
//   YAMS_BENCH_OUTPUT              optional JSONL output path
//   YAMS_BENCH_RUN_ID              optional run identifier

#include <yams/topology/topology_baseline.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;
using json = nlohmann::json;
using yams::topology::ConnectedComponentTopologyEngine;
using yams::topology::TopologyArtifactBatch;
using yams::topology::TopologyBuildConfig;
using yams::topology::TopologyBuildMode;
using yams::topology::TopologyDocumentInput;
using yams::topology::TopologyInputKind;
using yams::topology::TopologyNeighbor;

namespace {

constexpr std::size_t kComponentSize = 32;
constexpr std::size_t kOversizedComponentSize = 65;
constexpr std::size_t kFragmentedSingletonsPerCycle = 22;
constexpr std::size_t kFragmentedConnectedDocumentsPerCycle = 19;
constexpr std::size_t kFragmentedCycleDocuments =
    kFragmentedSingletonsPerCycle + kFragmentedConnectedDocumentsPerCycle;
constexpr std::size_t kDefaultNeighborsPerDocument = 32;
constexpr std::size_t kDefaultEmbeddingDimensions = 1024;
constexpr std::size_t kSafeMaxComponentDocuments = 64;
constexpr double kSafeMinimumEdgeScore = 0.25;

std::size_t envSize(std::string_view name, std::size_t fallback) {
    const std::string key{name};
    const char* raw = std::getenv(key.c_str());
    if (raw == nullptr || *raw == '\0') {
        return fallback;
    }
    char* end = nullptr;
    const auto parsed = std::strtoull(raw, &end, 10);
    if (end == raw || *end != '\0') {
        return fallback;
    }
    return static_cast<std::size_t>(parsed);
}

std::string envString(std::string_view name, std::string_view fallback = {}) {
    const std::string key{name};
    const char* raw = std::getenv(key.c_str());
    return raw == nullptr ? std::string{fallback} : std::string{raw};
}

std::string makeHash(std::size_t component, std::size_t member) {
    return "doc-" + std::to_string(component) + '-' + std::to_string(member);
}

enum class GraphShape : std::uint8_t {
    FragmentedSingletons,
    ClusteredLowScoreNoise,
    GiantComponent,
    ManyOversizedComponents
};

std::string_view shapeName(GraphShape shape) {
    switch (shape) {
        case GraphShape::FragmentedSingletons:
            return "fragmented_singletons";
        case GraphShape::ClusteredLowScoreNoise:
            return "clustered_low_score_noise";
        case GraphShape::GiantComponent:
            return "giant_component";
        case GraphShape::ManyOversizedComponents:
            return "many_oversized_components";
    }
    return "unknown";
}

struct WorkloadConfig {
    std::size_t embeddingDimensions{kDefaultEmbeddingDimensions};
    std::size_t neighborsPerDocument{kDefaultNeighborsPerDocument};
};

std::vector<TopologyDocumentInput> synthesizeDocuments(std::size_t documentCount, GraphShape shape,
                                                       const WorkloadConfig& workload) {
    std::vector<TopologyDocumentInput> documents(documentCount);
    for (std::size_t i = 0; i < documentCount; ++i) {
        const std::size_t componentSize =
            shape == GraphShape::ManyOversizedComponents ? kOversizedComponentSize : kComponentSize;
        const std::size_t component = i / componentSize;
        const std::size_t member = i % componentSize;
        const std::size_t componentStart = component * componentSize;
        const std::size_t componentEnd = std::min(componentStart + componentSize, documentCount);
        const std::size_t componentDocumentCount = componentEnd - componentStart;

        auto& document = documents[i];
        document.documentHash = makeHash(i / kComponentSize, i % kComponentSize);
        document.filePath = "/synth/topology-build/" + document.documentHash;
        document.embedding.assign(workload.embeddingDimensions, 0.0F);
        document.embedding[component % workload.embeddingDimensions] = 1.0F;
        document.embedding[i % workload.embeddingDimensions] += 0.05F;
        document.neighbors.reserve(workload.neighborsPerDocument + 1);

        // Twenty-two singleton clusters plus one 19-document cluster per cycle models the
        // persisted corpus's 95.7% singleton-cluster ratio and roughly 55% singleton documents.
        std::size_t neighborGroupStart = componentStart;
        std::size_t neighborGroupMember = member;
        std::size_t neighborGroupDocumentCount = componentDocumentCount;
        bool connectedComponent = true;
        if (shape == GraphShape::FragmentedSingletons) {
            const std::size_t cycleStart =
                (i / kFragmentedCycleDocuments) * kFragmentedCycleDocuments;
            const std::size_t cycleMember = i - cycleStart;
            connectedComponent = cycleMember >= kFragmentedSingletonsPerCycle;
            if (connectedComponent) {
                neighborGroupStart = cycleStart + kFragmentedSingletonsPerCycle;
                neighborGroupMember = i - neighborGroupStart;
                neighborGroupDocumentCount =
                    std::min(cycleStart + kFragmentedCycleDocuments, documentCount) -
                    neighborGroupStart;
            }
        }
        const std::size_t localNeighborCount =
            connectedComponent
                ? std::min(workload.neighborsPerDocument, neighborGroupDocumentCount > 0
                                                              ? neighborGroupDocumentCount - 1
                                                              : std::size_t{0})
                : 0;
        for (std::size_t offset = 1; offset <= localNeighborCount; ++offset) {
            const std::size_t neighbor =
                neighborGroupStart + ((neighborGroupMember + offset) % neighborGroupDocumentCount);
            document.neighbors.push_back(TopologyNeighbor{
                .documentHash = makeHash(neighbor / kComponentSize, neighbor % kComponentSize),
                .score = 0.90F - (static_cast<float>(offset) * 0.01F),
                .reciprocal = true,
            });
        }

        const bool componentBoundary = (i + 1 == componentEnd) && componentEnd < documentCount;
        const bool connectComponents =
            shape == GraphShape::ClusteredLowScoreNoise || shape == GraphShape::GiantComponent;
        if (componentBoundary && connectComponents) {
            document.neighbors.push_back(TopologyNeighbor{
                .documentHash =
                    makeHash(componentEnd / kComponentSize, componentEnd % kComponentSize),
                .score = shape == GraphShape::GiantComponent ? 0.90F : 0.10F,
                .reciprocal = true,
            });
        }
    }
    return documents;
}

class ArtifactFingerprintBuilder {
public:
    void appendInteger(std::uint64_t value) noexcept {
        for (std::size_t byte = 0; byte < sizeof(value); ++byte) {
            fingerprint_ ^= static_cast<unsigned char>(value >> (byte * 8U));
            fingerprint_ *= kFnvPrime;
        }
    }

    void appendString(std::string_view value) noexcept {
        appendInteger(value.size());
        for (const unsigned char byte : value) {
            fingerprint_ ^= byte;
            fingerprint_ *= kFnvPrime;
        }
    }

    void appendOptionalString(const std::optional<std::string>& value) noexcept {
        appendInteger(value.has_value() ? 1U : 0U);
        if (value.has_value()) {
            appendString(*value);
        }
    }

    void appendFloat(float value) noexcept { appendInteger(std::bit_cast<std::uint32_t>(value)); }

    void appendDouble(double value) noexcept { appendInteger(std::bit_cast<std::uint64_t>(value)); }

    [[nodiscard]] std::uint64_t value() const noexcept { return fingerprint_; }

private:
    static constexpr std::uint64_t kFnvOffset = 1469598103934665603ULL;
    static constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
    std::uint64_t fingerprint_{kFnvOffset};
};

std::uint64_t artifactFingerprint(const TopologyArtifactBatch& batch) {
    ArtifactFingerprintBuilder fingerprint;
    // Snapshot ids, wall-clock timestamps, and publication epochs are intentionally excluded.
    fingerprint.appendString(batch.algorithm);
    fingerprint.appendInteger(static_cast<std::uint8_t>(batch.inputKind));
    fingerprint.appendString(batch.embeddingSpaceIdentity);
    fingerprint.appendString(batch.protectedRelationIdentity);
    fingerprint.appendInteger(batch.clusters.size());
    for (const auto& cluster : batch.clusters) {
        fingerprint.appendString(cluster.clusterId);
        fingerprint.appendOptionalString(cluster.parentClusterId);
        fingerprint.appendInteger(cluster.level);
        fingerprint.appendInteger(cluster.memberCount);
        fingerprint.appendDouble(cluster.persistenceScore);
        fingerprint.appendDouble(cluster.cohesionScore);
        fingerprint.appendDouble(cluster.densityScore);
        fingerprint.appendDouble(cluster.bridgeMass);
        fingerprint.appendInteger(cluster.protectedPairCount);
        fingerprint.appendInteger(cluster.preservedProtectedPairCount);
        fingerprint.appendInteger(cluster.medoid.has_value() ? 1U : 0U);
        if (cluster.medoid.has_value()) {
            fingerprint.appendString(cluster.medoid->clusterId);
            fingerprint.appendString(cluster.medoid->documentHash);
            fingerprint.appendString(cluster.medoid->filePath);
            fingerprint.appendDouble(cluster.medoid->representativeScore);
        }
        fingerprint.appendInteger(cluster.memberDocumentHashes.size());
        for (const auto& hash : cluster.memberDocumentHashes) {
            fingerprint.appendString(hash);
        }
        fingerprint.appendInteger(cluster.overlapClusterIds.size());
        for (const auto& clusterId : cluster.overlapClusterIds) {
            fingerprint.appendString(clusterId);
        }
        fingerprint.appendInteger(cluster.centroidEmbedding.size());
        for (const float value : cluster.centroidEmbedding) {
            fingerprint.appendFloat(value);
        }
        fingerprint.appendInteger(cluster.routingRepresentatives.size());
        for (const auto& representative : cluster.routingRepresentatives) {
            fingerprint.appendString(representative.documentHash);
            fingerprint.appendInteger(representative.embedding.size());
            for (const float value : representative.embedding) {
                fingerprint.appendFloat(value);
            }
        }
    }
    fingerprint.appendInteger(batch.memberships.size());
    for (const auto& membership : batch.memberships) {
        fingerprint.appendString(membership.documentHash);
        fingerprint.appendString(membership.clusterId);
        fingerprint.appendOptionalString(membership.parentClusterId);
        fingerprint.appendInteger(membership.clusterLevel);
        fingerprint.appendDouble(membership.persistenceScore);
        fingerprint.appendDouble(membership.cohesionScore);
        fingerprint.appendDouble(membership.bridgeScore);
        fingerprint.appendInteger(static_cast<std::uint8_t>(membership.role));
        fingerprint.appendInteger(membership.overlapClusterIds.size());
        for (const auto& clusterId : membership.overlapClusterIds) {
            fingerprint.appendString(clusterId);
        }
    }
    return fingerprint.value();
}

struct LatencySummary {
    double p50Us{0.0};
    double p95Us{0.0};
    double p99Us{0.0};
    double meanUs{0.0};
    std::size_t samples{0};
};

LatencySummary summarizeLatency(std::vector<double>& samples) {
    LatencySummary summary;
    summary.samples = samples.size();
    if (samples.empty()) {
        return summary;
    }
    std::ranges::sort(samples);
    const auto percentile = [&](double quantile) {
        const double position = quantile * static_cast<double>(samples.size() - 1);
        const auto lower = static_cast<std::size_t>(position);
        const auto upper = std::min(lower + 1, samples.size() - 1);
        const double fraction = position - static_cast<double>(lower);
        return samples[lower] + ((samples[upper] - samples[lower]) * fraction);
    };
    summary.p50Us = percentile(0.50);
    summary.p95Us = percentile(0.95);
    summary.p99Us = percentile(0.99);
    summary.meanUs =
        std::accumulate(samples.begin(), samples.end(), 0.0) / static_cast<double>(samples.size());
    return summary;
}

struct BuildGeometry {
    std::size_t clusters{0};
    std::size_t memberships{0};
    std::size_t maxClusterSize{0};
    std::size_t singletonClusters{0};
    std::size_t protectedPairs{0};
    std::size_t preservedProtectedPairs{0};
    double giantClusterRatio{0.0};
};

BuildGeometry summarizeGeometry(const TopologyArtifactBatch& batch) {
    BuildGeometry geometry;
    geometry.clusters = batch.clusters.size();
    geometry.memberships = batch.memberships.size();
    std::size_t totalMembers = 0;
    for (const auto& cluster : batch.clusters) {
        geometry.maxClusterSize = std::max(geometry.maxClusterSize, cluster.memberCount);
        geometry.singletonClusters += cluster.memberCount == 1 ? 1 : 0;
        geometry.protectedPairs += cluster.protectedPairCount;
        geometry.preservedProtectedPairs += cluster.preservedProtectedPairCount;
        totalMembers += cluster.memberCount;
    }
    if (totalMembers > 0) {
        geometry.giantClusterRatio =
            static_cast<double>(geometry.maxClusterSize) / static_cast<double>(totalMembers);
    }
    return geometry;
}

struct ConfigPoint {
    std::string_view name;
    double minEdgeScore;
    std::size_t maxComponentDocuments;
    bool safeDefaults;
};

std::vector<GraphShape> selectShapes(std::string_view filter) {
    if (filter.empty() || filter == "all") {
        return {GraphShape::FragmentedSingletons, GraphShape::ClusteredLowScoreNoise,
                GraphShape::GiantComponent, GraphShape::ManyOversizedComponents};
    }
    if (filter == shapeName(GraphShape::FragmentedSingletons)) {
        return {GraphShape::FragmentedSingletons};
    }
    if (filter == shapeName(GraphShape::ClusteredLowScoreNoise)) {
        return {GraphShape::ClusteredLowScoreNoise};
    }
    if (filter == shapeName(GraphShape::GiantComponent)) {
        return {GraphShape::GiantComponent};
    }
    if (filter == shapeName(GraphShape::ManyOversizedComponents)) {
        return {GraphShape::ManyOversizedComponents};
    }
    throw std::runtime_error{"unknown topology build shape filter: " + std::string{filter}};
}

std::vector<ConfigPoint> selectConfigurations(std::string_view filter) {
    const ConfigPoint safe{
        .name = "safe_defaults",
        .minEdgeScore = kSafeMinimumEdgeScore,
        .maxComponentDocuments = kSafeMaxComponentDocuments,
        .safeDefaults = true,
    };
    const ConfigPoint control{
        .name = "unbounded_control",
        .minEdgeScore = 0.0,
        .maxComponentDocuments = 0,
        .safeDefaults = false,
    };
    if (filter.empty() || filter == "all") {
        return {safe, control};
    }
    if (filter == safe.name) {
        return {safe};
    }
    if (filter == control.name) {
        return {control};
    }
    throw std::runtime_error{"unknown topology build configuration filter: " + std::string{filter}};
}

json runPoint(std::span<const TopologyDocumentInput> documents, GraphShape shape,
              const ConfigPoint& point, const WorkloadConfig& workload, std::size_t repetitions,
              std::string_view runId) {
    TopologyBuildConfig config;
    config.mode = TopologyBuildMode::Approximate;
    config.inputKind = TopologyInputKind::Hybrid;
    config.minEdgeScore = point.minEdgeScore;
    config.maxComponentDocs = point.maxComponentDocuments;
    config.maxNeighborsPerDocument = workload.neighborsPerDocument;
    config.reciprocalOnly = true;
    config.allowOverlap = false;
    config.routingRepresentativeCount = 1;

    ConnectedComponentTopologyEngine engine;
    auto warmup = engine.buildArtifacts(documents, config);
    if (!warmup) {
        throw std::runtime_error{"topology build warmup failed"};
    }

    std::vector<double> latencyUs;
    latencyUs.reserve(repetitions);
    std::optional<std::uint64_t> expectedFingerprint;
    BuildGeometry geometry;
    for (std::size_t iteration = 0; iteration < repetitions; ++iteration) {
        const auto start = std::chrono::steady_clock::now();
        auto result = engine.buildArtifacts(documents, config);
        const auto end = std::chrono::steady_clock::now();
        if (!result) {
            throw std::runtime_error{"topology build failed"};
        }
        latencyUs.push_back(std::chrono::duration<double, std::micro>(end - start).count());

        const auto fingerprint = artifactFingerprint(result.value());
        if (!expectedFingerprint.has_value()) {
            expectedFingerprint = fingerprint;
            geometry = summarizeGeometry(result.value());
        } else if (fingerprint != *expectedFingerprint) {
            throw std::runtime_error{"topology artifact content changed between repetitions"};
        }
    }

    if (geometry.memberships != documents.size()) {
        throw std::runtime_error{"topology build did not emit one primary membership per document"};
    }
    if (point.safeDefaults && geometry.maxClusterSize > kSafeMaxComponentDocuments) {
        throw std::runtime_error{"safe construction cap was not preserved"};
    }
    if (!point.safeDefaults &&
        (shape == GraphShape::ClusteredLowScoreNoise || shape == GraphShape::GiantComponent) &&
        documents.size() > kComponentSize && geometry.clusters != 1) {
        throw std::runtime_error{"control graph no longer forms the expected connected component"};
    }

    const auto directedNeighborEntries = std::accumulate(
        documents.begin(), documents.end(), std::size_t{0},
        [](std::size_t total, const auto& document) { return total + document.neighbors.size(); });
    const auto summary = summarizeLatency(latencyUs);
    const double documentsPerSecond =
        summary.meanUs > 0.0 ? static_cast<double>(documents.size()) * 1'000'000.0 / summary.meanUs
                             : 0.0;

    return json{
        {"schema", "yams_topology_build_v1"},
        {"run_id", runId},
        {"configuration", point.name},
        {"safe_defaults", point.safeDefaults},
        {"shape", shapeName(shape)},
        {"documents", documents.size()},
        {"directed_neighbor_entries", directedNeighborEntries},
        {"repetitions", repetitions},
        {"construction",
         {{"mode", "approximate"},
          {"input_kind", "hybrid"},
          {"min_edge_score", point.minEdgeScore},
          {"max_component_docs", point.maxComponentDocuments},
          {"max_neighbors_per_document", workload.neighborsPerDocument},
          {"embedding_dimensions", workload.embeddingDimensions},
          {"reciprocal_only", true},
          {"allow_overlap", false},
          {"routing_representative_count", 1}}},
        {"build_latency_us",
         {{"p50", summary.p50Us},
          {"p95", summary.p95Us},
          {"p99", summary.p99Us},
          {"mean", summary.meanUs},
          {"samples", summary.samples}}},
        {"documents_per_second", documentsPerSecond},
        {"geometry",
         {{"clusters", geometry.clusters},
          {"memberships", geometry.memberships},
          {"max_cluster_size", geometry.maxClusterSize},
          {"singleton_clusters", geometry.singletonClusters},
          {"giant_cluster_ratio", geometry.giantClusterRatio},
          {"protected_pairs", geometry.protectedPairs},
          {"preserved_protected_pairs", geometry.preservedProtectedPairs}}},
        {"artifact_fingerprint", expectedFingerprint.value_or(0)},
    };
}

std::vector<std::size_t> documentScales(std::size_t maximum) {
    std::vector<std::size_t> scales{256, 1024};
    std::erase_if(scales, [maximum](std::size_t value) { return value > maximum; });
    if (scales.empty() || scales.back() != maximum) {
        scales.push_back(maximum);
    }
    return scales;
}

void emitRecord(const std::optional<fs::path>& outputPath, const json& record) {
    if (!outputPath.has_value()) {
        std::cout << record.dump() << '\n';
        return;
    }
    if (!outputPath->parent_path().empty()) {
        fs::create_directories(outputPath->parent_path());
    }
    std::ofstream output(*outputPath, std::ios::app);
    if (!output) {
        throw std::runtime_error{"failed to open benchmark output: " + outputPath->string()};
    }
    output << record.dump() << '\n';
}

} // namespace

int main() {
    try {
        const auto maximumDocuments =
            std::max<std::size_t>(1, envSize("YAMS_BENCH_TOPO_BUILD_DOCS", 4096));
        const auto repetitions =
            std::max<std::size_t>(1, envSize("YAMS_BENCH_TOPO_BUILD_REPEATS", 30));
        const auto runId = envString("YAMS_BENCH_RUN_ID", "local");
        const WorkloadConfig workload{
            .embeddingDimensions = std::max<std::size_t>(
                1, envSize("YAMS_BENCH_TOPO_BUILD_DIMENSIONS", kDefaultEmbeddingDimensions)),
            .neighborsPerDocument =
                envSize("YAMS_BENCH_TOPO_BUILD_NEIGHBORS", kDefaultNeighborsPerDocument),
        };
        const auto outputValue = envString("YAMS_BENCH_OUTPUT");
        const std::optional<fs::path> outputPath =
            outputValue.empty() ? std::nullopt : std::optional<fs::path>{outputValue};
        const auto configurations = selectConfigurations(envString("YAMS_BENCH_TOPO_BUILD_CONFIG"));
        const auto shapes = selectShapes(envString("YAMS_BENCH_TOPO_BUILD_SHAPE"));
        auto scales = documentScales(maximumDocuments);
        if (envString("YAMS_BENCH_TOPO_BUILD_ONLY_MAX") == "1") {
            scales = {maximumDocuments};
        }

        for (const auto documentCount : scales) {
            for (const auto shape : shapes) {
                const auto documents = synthesizeDocuments(documentCount, shape, workload);
                for (const auto& configuration : configurations) {
                    const auto record =
                        runPoint(documents, shape, configuration, workload, repetitions, runId);
                    emitRecord(outputPath, record);
                    std::cerr << configuration.name << ' ' << shapeName(shape)
                              << " docs=" << documentCount
                              << " p50_us=" << record["build_latency_us"]["p50"] << '\n';
                }
            }
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "topology_build_bench: " << error.what() << '\n';
        return 1;
    }
}
