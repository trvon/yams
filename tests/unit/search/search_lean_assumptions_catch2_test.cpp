// SPDX-License-Identifier: GPL-3.0-or-later
//
// Checks the hypotheses the Lean search contracts (formal/topology/Yams/Topology/SearchEngine.lean,
// TopologyInfluence.lean, SelectiveRouting.lean, AllowedSetRetrieval.lean) take about the C++
// search path. Each case names the Lean definition or theorem it stands in for.
//
// Cases tagged [!shouldfail] pin a place where the C++ behaviour does not satisfy the Lean
// definition as written; each one says why. When the code or the model changes so that the
// definition holds, Catch2 reports the case as failed and the tag has to be removed.

#include <catch2/catch_test_macros.hpp>

#include <yams/search/evidence_search_pipeline.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>
#include <yams/search/search_observability.h>
#include <yams/search/topology_routing_session.h>
#include <yams/search/tuning_pipeline.h>
#include <yams/vector/vector_database.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include "tests/unit/search/topology_search_test_support.h"

using yams::search::ComponentResult;
using yams::search::EvidenceSearchPipeline;
using yams::search::resolveQueryPolicy;
using yams::search::SearchEngineConfig;
using yams::search::SearchObservability;
using yams::search::SearchResponse;
using yams::search::seedTunedParamsFromConfig;
using yams::search::selectTopologyRoutesForNarrowing;
using yams::search::TopologyCandidateEvidence;
using yams::search::TopologyEvidenceByCandidate;
using yams::search::TopologyProofObligationStatus;
using yams::search::TopologyRouteAction;
using yams::search::TopologyRouteCertificate;
using yams::search::topologyRoutingMayExpand;
using yams::search::topologyRoutingMayLoad;

namespace {

constexpr std::uint32_t kSeed = 0x5ea7u;
constexpr int kIterations = 200;

constexpr std::array kSources{
    ComponentResult::Source::Text,
    ComponentResult::Source::SimeonText,
    ComponentResult::Source::GraphText,
    ComponentResult::Source::PathTree,
    ComponentResult::Source::KnowledgeGraph,
    ComponentResult::Source::Vector,
    ComponentResult::Source::GraphVector,
    ComponentResult::Source::EntityVector,
    ComponentResult::Source::Tag,
    ComponentResult::Source::Metadata,
};

std::string candidateId(const yams::metadata::SearchResult& result) {
    return result.document.sha256Hash.empty() ? result.document.filePath
                                              : result.document.sha256Hash;
}

std::vector<ComponentResult> randomComponents(std::mt19937& rng) {
    std::uniform_int_distribution<std::size_t> count(0, 24);
    std::uniform_int_distribution<std::size_t> doc(0, 9);
    std::uniform_int_distribution<std::size_t> source(0, kSources.size() - 1);
    std::uniform_int_distribution<std::size_t> rank(0, 20);
    std::uniform_real_distribution<float> score(-0.5F, 1.5F);
    std::uniform_real_distribution<float> unit(0.0F, 1.0F);

    std::vector<ComponentResult> components(count(rng));
    for (auto& component : components) {
        const auto d = doc(rng);
        // Ids come from the hash, from the path when the hash is empty, or are absent.
        const float shape = unit(rng);
        if (shape < 0.8F) {
            component.documentHash = "h" + std::to_string(d);
            component.filePath = "/p" + std::to_string(d);
        } else if (shape < 0.95F) {
            component.filePath = "/only-path-" + std::to_string(d);
        }
        component.source = kSources[source(rng)];
        component.rank = rank(rng);
        component.score = score(rng);
    }
    return components;
}

std::unordered_set<std::string> componentIds(const std::vector<ComponentResult>& components) {
    std::unordered_set<std::string> ids;
    for (const auto& component : components) {
        if (!component.documentHash.empty()) {
            ids.insert(component.documentHash);
        } else if (!component.filePath.empty()) {
            ids.insert(component.filePath);
        }
    }
    return ids;
}

} // namespace

TEST_CASE("Lean contracts: fusion and top-k cannot invent, duplicate or exceed the limit",
          "[unit][search][lean][contract][evidence-pipeline][property]") {
    // Lean SearchEngine: fuse_noInvent, graphRerank_noInvent, topK_subset, topK_respectsLimit,
    // runSearch_resultsComeFromEnabledCandidates, runSearch_respectsLimit; TopologyInfluence:
    // topology evidence may change scores but not the candidate set.
    std::mt19937 rng(kSeed);
    std::uniform_int_distribution<std::size_t> limit(0, 12);
    std::uniform_int_distribution<std::size_t> maxResults(1, 12);
    std::uniform_real_distribution<float> unit(0.0F, 1.0F);
    std::uniform_real_distribution<float> adjustment(-2.0F, 2.0F);
    const EvidenceSearchPipeline pipeline;

    for (int iteration = 0; iteration < kIterations; ++iteration) {
        const auto components = randomComponents(rng);
        const auto inputIds = componentIds(components);
        SearchEngineConfig config;
        config.maxResults = maxResults(rng);
        config.lexicalFloorBoost = unit(rng) < 0.5F ? 0.0F : unit(rng);
        const std::size_t requested = limit(rng);
        const std::size_t effectiveLimit = requested > 0 ? requested : config.maxResults;

        TopologyEvidenceByCandidate topology;
        for (std::size_t d = 0; d < 12; ++d) {
            if (unit(rng) < 0.3F) {
                // Includes ids that no component produced.
                topology.emplace("h" + std::to_string(d),
                                 TopologyCandidateEvidence{.scoreAdjustment = adjustment(rng),
                                                           .confidence = adjustment(rng)});
            }
        }
        CAPTURE(iteration, components.size(), requested, config.maxResults, topology.size());

        const auto bounded = pipeline.execute(components, topology, config, requested);
        REQUIRE(bounded.has_value());
        const auto& results = bounded.value().results;
        CHECK(results.size() <= effectiveLimit);
        CHECK(results.size() == std::min(effectiveLimit, inputIds.size()));

        std::unordered_set<std::string> seen;
        for (std::size_t i = 0; i < results.size(); ++i) {
            const auto id = candidateId(results[i]);
            CAPTURE(id);
            CHECK(inputIds.contains(id));
            CHECK(seen.insert(id).second);
            if (i > 0) {
                CHECK(results[i - 1].score >= results[i].score);
            }
        }

        // Top-k is a prefix of the unbounded ranking.
        const auto unbounded = pipeline.execute(components, topology, config, 1000);
        REQUIRE(unbounded.has_value());
        const auto& all = unbounded.value().results;
        REQUIRE(all.size() >= results.size());
        for (std::size_t i = 0; i < results.size(); ++i) {
            CHECK(candidateId(all[i]) == candidateId(results[i]));
        }

        // Topology evidence changes scores only: the unbounded candidate set is the same without
        // it.
        const auto withoutTopology = pipeline.execute(components, {}, config, 1000);
        REQUIRE(withoutTopology.has_value());
        std::unordered_set<std::string> idsWith;
        std::unordered_set<std::string> idsWithout;
        for (const auto& result : all) {
            idsWith.insert(candidateId(result));
        }
        for (const auto& result : withoutTopology.value().results) {
            idsWithout.insert(candidateId(result));
        }
        CHECK(idsWith == idsWithout);
        CHECK(idsWith == inputIds);
    }
}

TEST_CASE("Lean contracts: topology load and expand gates match TopologyInfluence",
          "[unit][search][lean][contract][topology]") {
    // Lean TopologyInfluence.topologyMayLoad / topologyMayExpand. C++ has no rerankOnly mode,
    // the one Lean mode where the two gates differ.
    using Mode = SearchEngineConfig::TopologyRoutingMode;
    for (const bool weak : {false, true}) {
        CAPTURE(weak);
        CHECK_FALSE(topologyRoutingMayLoad(Mode::Disabled, weak));
        CHECK_FALSE(topologyRoutingMayExpand(Mode::Disabled, weak));
        CHECK(topologyRoutingMayLoad(Mode::WeakQueryOnly, weak) == weak);
        CHECK(topologyRoutingMayExpand(Mode::WeakQueryOnly, weak) == weak);
        CHECK(topologyRoutingMayLoad(Mode::HybridAssist, weak));
        CHECK(topologyRoutingMayExpand(Mode::HybridAssist, weak));
    }
}

TEST_CASE("Lean contracts: a response is degraded exactly when a component failed or timed out",
          "[unit][search][lean][contract][observability]") {
    // Lean SearchEngine.runSearch_degradedFlag; skipped components are not degradation.
    for (int mask = 0; mask < 8; ++mask) {
        const bool failed = (mask & 1) != 0;
        const bool timedOut = (mask & 2) != 0;
        const bool skipped = (mask & 4) != 0;
        CAPTURE(failed, timedOut, skipped);
        SearchObservability observability;
        if (failed) {
            observability.failedComponents = {"vector"};
        }
        if (timedOut) {
            observability.timedOutComponents = {"text"};
        }
        if (skipped) {
            observability.skippedComponents = {"kg"};
        }
        SearchResponse response;
        response.isDegraded = !(failed || timedOut);
        observability.applyComponentStatus(response);
        CHECK(response.isDegraded == (failed || timedOut));
    }
}

TEST_CASE("Lean contracts: only a fully admitted, useful, bound certificate narrows",
          "[unit][search][lean][contract][topology][property]") {
    // Lean SelectiveRouting.selectRoutingAction: narrow iff admissible, global iff the allowed
    // set is empty, otherwise augment. C++ additionally needs a non-empty allowed set and a
    // construction-bound certificate before it narrows, which only removes narrow cases.
    using Status = TopologyProofObligationStatus;
    constexpr std::array kStatuses{Status::Unavailable, Status::Satisfied, Status::Violated};
    std::size_t narrowCount = 0;
    for (std::size_t code = 0; code < 2187; ++code) { // 3^7 obligation assignments
        std::array<Status, 7> statuses{};
        std::size_t rest = code;
        for (auto& status : statuses) {
            status = kStatuses[rest % 3];
            rest /= 3;
        }
        const bool allSatisfied = std::ranges::all_of(
            statuses, [](Status status) { return status == Status::Satisfied; });
        for (int extra = 0; extra < 16; ++extra) {
            const bool useful = (extra & 1) != 0;
            const bool bound = (extra & 2) != 0;
            const bool selectedPositive = (extra & 4) != 0;
            const bool materializedMatches = (extra & 8) != 0;

            TopologyRouteCertificate certificate;
            auto& admission = certificate.admission;
            admission.coordinateSpaceAlignment = statuses[0];
            admission.protectedRelationCoverage = statuses[1];
            admission.protectedFibersRepresented = statuses[2];
            admission.certificateSaturatesProtectedFibers = statuses[3];
            admission.routeRisk = statuses[4];
            admission.work = statuses[5];
            admission.coverMaterialization = statuses[6];
            admission.selectedCoverDocuments = selectedPositive ? 2 : 0;
            admission.materializedCoverDocuments =
                materializedMatches ? admission.selectedCoverDocuments : 1;
            if (useful) {
                certificate.allowedDocumentHashes.insert("doc");
            }
            if (bound) {
                certificate.constructionFingerprint = "construction";
                certificate.selectedCoverIds = {"cover"};
                certificate.selectedProtectedRelationFiberIds = {"fiber"};
            }

            const bool admissible =
                allSatisfied && selectedPositive && admission.materializedCoverDocuments == 2;
            const auto action = certificate.action();
            CAPTURE(code, extra);
            CHECK((admission.eligibleForNarrowing() == admissible));
            if (!useful) {
                CHECK((action == TopologyRouteAction::Global));
            } else if (admissible && bound) {
                CHECK((action == TopologyRouteAction::Narrow));
                ++narrowCount;
            } else {
                CHECK((action == TopologyRouteAction::Augment));
            }
        }
    }
    CHECK(narrowCount == 1U); // all seven satisfied, useful, bound, selected, materialized
}

TEST_CASE("Lean contracts: route selection for narrowing is a bounded prefix",
          "[unit][search][lean][contract][topology][property]") {
    // Lean TopologyInfluence.selectedTopologyClusters (= clusters.take maxClusters) and
    // selectedTopologyClusters_respectsClusterCap. C++ adds an adaptive prefix and abstention.
    std::mt19937 rng(kSeed + 1);
    std::uniform_int_distribution<std::size_t> count(0, 8);
    std::uniform_int_distribution<std::size_t> bound(0, 5);
    std::uniform_real_distribution<float> unit(0.0F, 1.0F);
    for (int iteration = 0; iteration < kIterations; ++iteration) {
        std::vector<yams::topology::ClusterRoute> routes(count(rng));
        for (std::size_t i = 0; i < routes.size(); ++i) {
            routes[i].clusterId = "c" + std::to_string(i);
            routes[i].routeScore = unit(rng);
        }
        std::ranges::sort(routes,
                          [](const auto& a, const auto& b) { return a.routeScore > b.routeScore; });
        const std::size_t minClusters = bound(rng);
        const std::size_t maxClusters = bound(rng);
        const float gap = unit(rng) < 0.5F ? 0.0F : unit(rng) * 0.3F;
        const float margin = unit(rng) < 0.5F ? 0.0F : unit(rng) * 0.3F;
        CAPTURE(iteration, routes.size(), minClusters, maxClusters, gap, margin);

        const auto selection =
            selectTopologyRoutesForNarrowing(routes, minClusters, maxClusters, gap, margin);
        CHECK(selection.availableRoutes == routes.size());
        REQUIRE(selection.routes.size() <= routes.size());
        for (std::size_t i = 0; i < selection.routes.size(); ++i) {
            CHECK(selection.routes[i].clusterId == routes[i].clusterId);
        }
        if (maxClusters > 0) {
            CHECK(selection.routes.size() <= maxClusters);
        }
        if (!routes.empty()) {
            CHECK_FALSE(selection.routes.empty());
        }
        if (margin <= 0.0F) {
            CHECK_FALSE(selection.abstained);
        }
    }
}

TEST_CASE("Lean contracts: semantic mode keeps every graph-text source out",
          "[unit][search][lean][contract][mode]") {
    // Lean SearchEngine.semanticMode_noLexicalOnlyCandidates for the graphText source: C++
    // schedules a component only when its weight is positive.
    SearchEngineConfig config;
    config.enableIntentAdaptiveWeighting = false;
    config.textWeight = 0.50F;
    config.vectorWeight = 0.30F;
    config.graphTextWeight = 0.20F;
    const auto resolution = resolveQueryPolicy("how do embeddings work", config,
                                               seedTunedParamsFromConfig(config), std::nullopt,
                                               /*semanticOnly=*/true);
    CHECK(resolution.config.graphTextWeight <= 0.0F);
    CHECK(resolution.config.vectorWeight > 0.0F);
}

TEST_CASE("Lean contracts: semantic mode schedules no lexical-only component",
          "[unit][search][lean][contract][mode][!shouldfail]") {
    // Lean SearchEngine.semanticMode_noLexicalOnlyCandidates: semantic mode admits no text,
    // path, tag, metadata or graphText candidate. SearchService maps type=semantic to
    // SearchParams::semanticOnly, and applySemanticOnlyLayer only caps the text weight at 0.20
    // (simeonText at 0.05) and leaves path, tag and metadata weights alone, so their components
    // still run and their candidates reach fusion. In C++ "semantic" means vector-dominant
    // hybrid, not vector-only; the Lean source gate does not describe it.
    SearchEngineConfig config;
    config.enableIntentAdaptiveWeighting = false;
    config.textWeight = 0.50F;
    config.vectorWeight = 0.30F;
    config.pathTreeWeight = 0.08F;
    config.tagWeight = 0.05F;
    config.metadataWeight = 0.05F;
    const auto resolution = resolveQueryPolicy("how do embeddings work", config,
                                               seedTunedParamsFromConfig(config), std::nullopt,
                                               /*semanticOnly=*/true);
    CHECK(resolution.config.textWeight <= 0.0F);
    CHECK(resolution.config.pathTreeWeight <= 0.0F);
    CHECK(resolution.config.tagWeight <= 0.0F);
    CHECK(resolution.config.metadataWeight <= 0.0F);
}

TEST_CASE("Lean contracts: allowed-set vector retrieval never leaves the allowed set",
          "[unit][search][lean][contract][vector][property]") {
    // Lean AllowedSetRetrieval.RetrieverRespectsAllowedSet, the premise of
    // retrieveRoutes_noInvent: every hit of a routed request names an allowed document.
    using yams::vector::CandidateFilterMode;
    using yams::vector::VectorDatabase;
    using yams::vector::VectorDatabaseConfig;
    using yams::vector::VectorRecord;
    using yams::vector::VectorSearchEngine;
    using yams::vector::VectorSearchParams;

    std::mt19937 rng(kSeed + 2);
    std::uniform_real_distribution<float> coord(-1.0F, 1.0F);
    std::uniform_real_distribution<float> unit(0.0F, 1.0F);
    std::uniform_int_distribution<std::size_t> k(1, 6);
    constexpr std::size_t kDocs = 6;
    constexpr std::size_t kChunksPerDoc = 3;

    for (const auto engine : {VectorSearchEngine::ExactScan, VectorSearchEngine::Vec0L2,
                              VectorSearchEngine::SimeonPqAdc}) {
        CAPTURE(yams::vector::vectorSearchEngineName(engine));
        VectorDatabaseConfig config;
        config.database_path = ":memory:";
        config.embedding_dim = 4;
        config.use_in_memory = true;
        config.search_engine = engine;
        config.simeon_pq_subquantizers = 2;
        config.simeon_pq_centroids = 4;
        config.simeon_pq_train_limit = 32;
        config.simeon_pq_rerank_factor = 1;
        VectorDatabase db(config);
        REQUIRE(db.initializeChecked().has_value());
        for (std::size_t d = 0; d < kDocs; ++d) {
            for (std::size_t c = 0; c < kChunksPerDoc; ++c) {
                VectorRecord record;
                record.chunk_id = "doc" + std::to_string(d) + "_chunk" + std::to_string(c);
                record.document_hash = "doc" + std::to_string(d);
                record.embedding = {coord(rng), coord(rng), coord(rng), coord(rng)};
                REQUIRE(db.insertVectorChecked(record).has_value());
            }
        }
        REQUIRE(db.buildIndexChecked().has_value());

        for (const auto mode :
             {CandidateFilterMode::BackendDefault, CandidateFilterMode::Exact,
              CandidateFilterMode::ExactDocumentComplete, CandidateFilterMode::DocumentTopK}) {
            for (int iteration = 0; iteration < 25; ++iteration) {
                VectorSearchParams params;
                params.k = k(rng);
                params.similarity_threshold = -1.0F;
                params.candidate_filter_mode = mode;
                for (std::size_t d = 0; d <= kDocs; ++d) {
                    if (unit(rng) < 0.4F) {
                        // d == kDocs names a document with no vectors.
                        params.candidate_hashes.insert("doc" + std::to_string(d));
                    }
                }
                if (params.candidate_hashes.empty()) {
                    params.candidate_hashes.insert("doc0");
                }
                CAPTURE(static_cast<int>(mode), iteration, params.k,
                        params.candidate_hashes.size());

                const auto hits = db.searchSimilarChecked(
                    {coord(rng), coord(rng), coord(rng), coord(rng)}, params);
                if (mode == CandidateFilterMode::DocumentTopK &&
                    engine != VectorSearchEngine::SimeonPqAdc) {
                    // Only the Simeon PQ backend implements per-document top-k; the others
                    // refuse the request instead of returning anything.
                    REQUIRE_FALSE(hits.has_value());
                    CHECK(hits.error().code == yams::ErrorCode::NotSupported);
                    continue;
                }
                REQUIRE(hits.has_value());
                // ExactDocumentComplete returns every row of the allowed documents by design; the
                // caller truncates at the document boundary (Lean's `take documentBudget`).
                if (mode != CandidateFilterMode::ExactDocumentComplete) {
                    CHECK(hits.value().size() <= params.k);
                }
                for (const auto& hit : hits.value()) {
                    CAPTURE(hit.chunk_id, hit.document_hash);
                    CHECK(params.candidate_hashes.contains(hit.document_hash));
                }
            }
        }
    }
}

TEST_CASE("Lean contracts: end-to-end results are stored documents within the limit",
          "[unit][search][lean][contract][engine]") {
    // Lean SearchEngine.runSearch_respectsLimit and SystemContract.StorageToSearchContract
    // (every result is a document whose metadata is visible) through SearchEngine.
    namespace support = yams::search::topology_test_support;
    support::TopologySearchFixture fix;
    support::seedTopologyDocuments(fix);
    const auto generator = support::makeFixedGenerator({1.0F, 0.0F});
    const auto config = support::topologyRoutingTestConfig(false);
    const std::unordered_set<std::string> stored{"x1", "x2", "y1", "y2"};

    for (const std::size_t limit : {1U, 2U, 3U, 10U}) {
        for (const char* query : {"alpha", "omega one", "unmatched query"}) {
            CAPTURE(limit, query);
            const auto response = support::runTopologySearch(fix, generator, config, limit, query);
            REQUIRE(response.has_value());
            CHECK(response.value().results.size() <= limit);
            for (const auto& result : response.value().results) {
                CAPTURE(result.document.sha256Hash);
                CHECK(stored.contains(result.document.sha256Hash));
            }
        }
    }
}

TEST_CASE("Lean contracts: every disabled source is reported as skipped",
          "[unit][search][lean][contract][engine][!shouldfail]") {
    // Lean SearchEngine.disabledSource_isSkipped: a source whose gate is off appears in the
    // response's skipped list. SearchEngine leaves a zero-weight component unscheduled without
    // recording it; skippedComponents only names stages skipped at run time (kg under the
    // semantic budget guard, graph_rerank, cross_rerank). topologyRoutingTestConfig sets the path,
    // tag, metadata and kg weights to zero.
    namespace support = yams::search::topology_test_support;
    support::TopologySearchFixture fix;
    support::seedTopologyDocuments(fix);
    const auto generator = support::makeFixedGenerator({1.0F, 0.0F});
    const auto config = support::topologyRoutingTestConfig(false);
    REQUIRE(config.pathTreeWeight <= 0.0F);
    REQUIRE(config.metadataWeight <= 0.0F);

    const auto response = support::runTopologySearch(fix, generator, config, 4, "alpha");
    REQUIRE(response.has_value());
    const auto& skipped = response.value().skippedComponents;
    CAPTURE(skipped);
    CHECK(std::ranges::find(skipped, "path") != skipped.end());
    CHECK(std::ranges::find(skipped, "metadata") != skipped.end());
}
