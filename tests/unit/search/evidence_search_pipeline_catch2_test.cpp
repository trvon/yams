#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/search/evidence_search_pipeline.h>

#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>

using yams::ErrorCode;
using yams::Result;
using yams::search::ComponentResult;
using yams::search::deriveTopologyCandidateEvidence;
using yams::search::EvidenceSearchPipeline;
using yams::search::SearchEngineConfig;
using yams::search::TopologyCandidateEvidence;
using yams::search::TopologyCandidateStructureEvidence;
using yams::search::TopologyEvidenceByCandidate;
using yams::search::TopologyRescuePriorityByIdentity;
using yams::search::TopologyRoutingSessionResult;

namespace {

ComponentResult makeComponent(std::string id, float score, ComponentResult::Source source,
                              std::size_t rank) {
    ComponentResult component;
    component.documentHash = id;
    component.filePath = id;
    component.score = score;
    component.source = source;
    component.rank = rank;
    return component;
}

std::unordered_set<std::string>
resultIds(const std::vector<yams::metadata::SearchResult>& results) {
    std::unordered_set<std::string> ids;
    for (const auto& result : results) {
        ids.insert(result.document.sha256Hash);
    }
    return ids;
}

} // namespace

TEST_CASE("Topology membership becomes bounded candidate-only evidence",
          "[search][evidence_pipeline][topology][catch2]") {
    TopologyRoutingSessionResult route;
    route.artifactAdmitted = true;
    route.bestRouteScore = 0.8F;
    route.meanAcceptedRouteScore = 0.6F;
    route.routeBoundaryScoreMargin = 0.4F;
    route.seedCount = 2;
    route.seedsInRoutedClusters = 2;
    route.certificate.allowedDocumentHashes = {"member", "medoid"};
    route.routedCandidateHashes = {"medoid"};
    route.medoidHashes = {"medoid"};
    route.candidateStructureEvidence.emplace(
        "member", TopologyCandidateStructureEvidence{.scaleAgreement = 0.75F,
                                                     .overlapSupport = 0.50F,
                                                     .persistenceSupport = 0.80F,
                                                     .cohesionSupport = 0.70F,
                                                     .bridgeSupport = 0.40F,
                                                     .densitySupport = 0.60F});

    const auto evidence = deriveTopologyCandidateEvidence(route, 0.02F);

    REQUIRE(evidence.size() == 2);
    CHECK(evidence.at("member").confidence >= 0.0F);
    CHECK(evidence.at("member").confidence <= 1.0F);
    CHECK(evidence.at("member").scoreAdjustment <= 0.02F);
    CHECK(evidence.at("medoid").scoreAdjustment > evidence.at("member").scoreAdjustment);
    CHECK(evidence.at("member").scaleAgreement == Catch::Approx(0.75F));
    CHECK(evidence.at("member").overlapSupport == Catch::Approx(0.50F));
    CHECK(evidence.at("member").persistenceSupport == Catch::Approx(0.80F));
    CHECK(evidence.at("member").cohesionSupport == Catch::Approx(0.70F));
    CHECK(evidence.at("member").bridgeSupport == Catch::Approx(0.40F));
    CHECK(evidence.at("member").densitySupport == Catch::Approx(0.60F));
    CHECK(evidence.at("member").densityPenalty == Catch::Approx(0.40F));

    route.confidenceAbstained = true;
    CHECK(deriveTopologyCandidateEvidence(route, 0.02F).empty());
}

TEST_CASE("EvidenceSearchPipeline aggregates typed source evidence without changing candidates",
          "[search][evidence_pipeline][catch2]") {
    SearchEngineConfig config;
    config.maxResults = 10;
    config.rrfK = 1.0F;
    config.textWeight = 1.0F;
    config.vectorWeight = 1.0F;

    std::vector<ComponentResult> components{
        makeComponent("doc-a", 0.8F, ComponentResult::Source::Text, 0),
        makeComponent("doc-a", 0.9F, ComponentResult::Source::Vector, 3),
        makeComponent("doc-b", 0.7F, ComponentResult::Source::Vector, 0),
    };

    EvidenceSearchPipeline pipeline;
    auto run = pipeline.execute(components, {}, config, 10);

    REQUIRE(run);
    CHECK(run.value().trace.inputComponents == 3);
    CHECK(run.value().trace.aggregatedCandidates == 2);
    CHECK(run.value().trace.fusedCandidates == 2);
    CHECK(run.value().trace.finalCandidates == 2);
    CHECK(resultIds(run.value().results) == std::unordered_set<std::string>{"doc-a", "doc-b"});

    const auto evidence = std::ranges::find_if(run.value().evidence, [](const auto& candidate) {
        return candidate.documentHash == "doc-a";
    });
    REQUIRE(evidence != run.value().evidence.end());
    CHECK(evidence->hasLexicalAnchor);
    CHECK(evidence->hasVectorEvidence);
    CHECK(evidence->sources.size() == 2);
}

TEST_CASE("EvidenceSearchPipeline materializes the lexical rank floor as evidence",
          "[search][evidence_pipeline][lexical][catch2]") {
    SearchEngineConfig config;
    config.maxResults = 10;
    config.rrfK = 45.0F;
    config.textWeight = 0.6F;
    config.simeonTextWeight = 0.0F;
    config.vectorWeight = 0.4F;
    config.lexicalFloorBoost = 0.2F;
    config.lexicalFloorTopN = 2;

    const std::vector<ComponentResult> components{
        makeComponent("lexical", 1.0F, ComponentResult::Source::SimeonText, 0),
        makeComponent("vector", 1.0F, ComponentResult::Source::Vector, 0),
    };

    EvidenceSearchPipeline pipeline;
    auto run = pipeline.execute(components, {}, config, 10);

    REQUIRE(run);
    REQUIRE(run.value().results.size() == 2);
    CHECK(run.value().results.front().document.sha256Hash == "lexical");
    CHECK(run.value().results.front().score >= 0.2);
    REQUIRE(run.value().results.front().graphTextScore.has_value());
    CHECK(*run.value().results.front().graphTextScore >= 0.2);
}

TEST_CASE("EvidenceSearchPipeline topology evidence reorders but cannot add or remove candidates",
          "[search][evidence_pipeline][topology][catch2]") {
    SearchEngineConfig config;
    config.maxResults = 10;
    config.rrfK = 1.0F;
    config.textWeight = 1.0F;

    const std::vector<ComponentResult> components{
        makeComponent("doc-a", 1.0F, ComponentResult::Source::Text, 0),
        makeComponent("doc-b", 0.8F, ComponentResult::Source::Text, 1),
    };

    EvidenceSearchPipeline pipeline;
    auto baseline = pipeline.execute(components, {}, config, 10);
    REQUIRE(baseline);
    REQUIRE(baseline.value().results.size() == 2);
    CHECK(baseline.value().results.front().document.sha256Hash == "doc-a");

    TopologyEvidenceByCandidate topology;
    topology.emplace("doc-b", TopologyCandidateEvidence{.scoreAdjustment = 0.5F,
                                                        .confidence = 1.0F,
                                                        .localSupport = 0.9F,
                                                        .boundarySupport = 0.8F,
                                                        .scaleAgreement = 0.7F});
    topology.emplace("invented",
                     TopologyCandidateEvidence{.scoreAdjustment = 1.0F, .confidence = 1.0F});
    auto withTopology = pipeline.execute(components, topology, config, 10);

    REQUIRE(withTopology);
    REQUIRE(withTopology.value().results.size() == 2);
    CHECK(withTopology.value().results.front().document.sha256Hash == "doc-b");
    CHECK(resultIds(withTopology.value().results) == resultIds(baseline.value().results));
    CHECK(withTopology.value().trace.topologyAnnotatedCandidates == 1);
}

TEST_CASE("EvidenceSearchPipeline reranker must preserve the fused candidate set",
          "[search][evidence_pipeline][rerank][catch2]") {
    SearchEngineConfig config;
    config.maxResults = 10;
    config.textWeight = 1.0F;
    const std::vector<ComponentResult> components{
        makeComponent("doc-a", 1.0F, ComponentResult::Source::Text, 0),
        makeComponent("doc-b", 0.8F, ComponentResult::Source::Text, 1),
    };

    EvidenceSearchPipeline pipeline;
    auto reranked =
        pipeline.execute(components, {}, config, 10, {}, {},
                         [](std::vector<yams::metadata::SearchResult>& results) -> Result<void> {
                             auto docB = std::ranges::find_if(results, [](const auto& result) {
                                 return result.document.sha256Hash == "doc-b";
                             });
                             if (docB != results.end()) {
                                 docB->score += 10.0;
                             }
                             return {};
                         });

    REQUIRE(reranked);
    CHECK(reranked.value().results.front().document.sha256Hash == "doc-b");
    CHECK(reranked.value().trace.rerankedCandidates == 2);

    auto invalid =
        pipeline.execute(components, {}, config, 10, {}, {},
                         [](std::vector<yams::metadata::SearchResult>& results) -> Result<void> {
                             yams::metadata::SearchResult invented;
                             invented.document.sha256Hash = "invented";
                             results.push_back(std::move(invented));
                             return {};
                         });

    REQUIRE_FALSE(invalid);
    CHECK(invalid.error().code == ErrorCode::ValidationError);
}

TEST_CASE("EvidenceSearchPipeline additively rescues identity-eligible candidates beyond the "
          "fusion window",
          "[search][evidence_pipeline][topology][fusion-rescue][catch2]") {
    SearchEngineConfig config;
    config.maxResults = 10;
    config.textWeight = 1.0F;
    config.rrfK = 1.0F;

    // Three strong text-only candidates plus two weak tail candidates that both rank far below
    // them on raw fused score. This mirrors the q0 boundary: relation-reachable, admitted, and
    // document-scored, but cut before the fusion window on merged rank alone.
    //   - "rescued": in the eligibility set, has topology evidence -> must be rescued.
    //   - "decoy": has the SAME topology evidence as "rescued" but is NOT in the eligibility set
    //     -> must NOT be rescued, even though it received identical topology evidence. This is
    //     the regression test for "topology evidence alone is not sufficient; identity is".
    //   - "ghost": IS in the eligibility set but has no topology evidence at all -> must NOT be
    //     rescued. Identity membership is never a bypass for lacking topology evidence, since the
    //     eligibility set and the topology-evidence map are populated by independent producers.
    std::vector<ComponentResult> components{
        makeComponent("doc-a", 1.00F, ComponentResult::Source::Text, 0),
        makeComponent("doc-b", 0.90F, ComponentResult::Source::Text, 1),
        makeComponent("doc-c", 0.80F, ComponentResult::Source::Text, 2),
        makeComponent("rescued", 0.01F, ComponentResult::Source::Text, 500),
        makeComponent("decoy", 0.02F, ComponentResult::Source::Text, 501),
        makeComponent("ghost", 0.005F, ComponentResult::Source::Text, 502),
    };

    // Keep the topology score-adjustment small (as in production: topologyEvidenceWeight is a
    // bounded nudge, not a rank override) so "rescued"/"decoy" stay last on ordinary fused score.
    TopologyEvidenceByCandidate topology;
    topology.emplace("rescued", TopologyCandidateEvidence{.scoreAdjustment = 0.3F,
                                                          .confidence = 0.5F,
                                                          .localSupport = 1.0F});
    topology.emplace("decoy", TopologyCandidateEvidence{.scoreAdjustment = 0.3F,
                                                        .confidence = 0.5F,
                                                        .localSupport = 1.0F});
    // "ghost" deliberately gets no topology evidence entry.

    EvidenceSearchPipeline pipeline;

    // Baseline: fusion window of 3 cuts all three tail candidates before they reach
    // response.results.
    auto baseline = pipeline.execute(components, topology, config, 3);
    REQUIRE(baseline);
    CHECK(baseline.value().trace.finalCandidates == 3);
    CHECK(baseline.value().trace.topologyFusionRescuedCandidates == 0);
    CHECK(resultIds(baseline.value().results) ==
          std::unordered_set<std::string>{"doc-a", "doc-b", "doc-c"});

    // Eligibility set includes "rescued" and "ghost" but deliberately excludes "decoy".
    const std::unordered_set<std::string> eligibleHashes{"rescued", "ghost"};
    config.topologyFusionRescueSlots = 2;
    auto rescued = pipeline.execute(components, topology, config, 3, eligibleHashes);
    REQUIRE(rescued);
    // Only "rescued" clears both gates (identity + evidence); "decoy" fails identity; "ghost"
    // fails the has-topology-evidence check.
    CHECK(rescued.value().trace.finalCandidates == 4);
    CHECK(rescued.value().trace.topologyFusionRescuedCandidates == 1);
    CHECK(rescued.value().trace.topologyFusionRescueIdentityMatchesInTail == 2);
    CHECK(rescued.value().trace.topologyFusionRescueIdentityMatchesWithEvidenceInTail == 1);
    CHECK(rescued.value().trace.topologyFusionRescueEligibleIdentitiesInTail ==
          std::vector<std::string>{"rescued", "ghost"});
    REQUIRE(rescued.value().results.size() == 4);
    CHECK(resultIds(rescued.value().results) ==
          std::unordered_set<std::string>{"doc-a", "doc-b", "doc-c", "rescued"});
    for (std::size_t i = 0; i < 3; ++i) {
        CHECK(rescued.value().results[i].document.sha256Hash ==
              baseline.value().results[i].document.sha256Hash);
    }
    const auto rescuedResult = std::ranges::find_if(
        rescued.value().results, [](const auto& r) { return r.document.sha256Hash == "rescued"; });
    REQUIRE(rescuedResult != rescued.value().results.end());
    CHECK(rescuedResult->topologyScore.has_value());
}

TEST_CASE("EvidenceSearchPipeline uses query priority only within topology rescue eligibility",
          "[search][evidence_pipeline][topology-rescue][catch2]") {
    SearchEngineConfig config;
    config.maxResults = 10;
    config.textWeight = 1.0F;
    config.rrfK = 1.0F;
    config.topologyFusionRescueSlots = 1;

    const std::vector<ComponentResult> components{
        makeComponent("doc-a", 1.0F, ComponentResult::Source::Text, 0),
        makeComponent("doc-b", 0.9F, ComponentResult::Source::Text, 1),
        makeComponent("doc-c", 0.8F, ComponentResult::Source::Text, 2),
        makeComponent("first", 0.02F, ComponentResult::Source::Text, 500),
        makeComponent("second", 0.01F, ComponentResult::Source::Text, 501),
    };
    TopologyEvidenceByCandidate topology;
    topology.emplace("first", TopologyCandidateEvidence{.scoreAdjustment = 0.3F,
                                                        .confidence = 0.5F,
                                                        .localSupport = 1.0F});
    topology.emplace("second", TopologyCandidateEvidence{.scoreAdjustment = 0.3F,
                                                         .confidence = 0.5F,
                                                         .localSupport = 1.0F});

    EvidenceSearchPipeline pipeline;
    const std::unordered_set<std::string> eligible{"first", "second"};
    const TopologyRescuePriorityByIdentity priorities{{"first", 0.1F}, {"second", 0.9F}};

    auto result = pipeline.execute(components, topology, config, 3, eligible, priorities);
    REQUIRE(result);
    CHECK(result.value().trace.topologyFusionRescuedCandidates == 1);
    CHECK(result.value().trace.topologyFusionRescueEligibleIdentitiesInTail ==
          std::vector<std::string>{"second", "first"});
    CHECK(resultIds(result.value().results) ==
          std::unordered_set<std::string>{"doc-a", "doc-b", "doc-c", "second"});
}

TEST_CASE("EvidenceSearchPipeline removes candidates only at the final top-k boundary",
          "[search][evidence_pipeline][topk][catch2]") {
    SearchEngineConfig config;
    config.maxResults = 10;
    config.textWeight = 1.0F;
    const std::vector<ComponentResult> components{
        makeComponent("doc-a", 1.0F, ComponentResult::Source::Text, 0),
        makeComponent("doc-b", 0.8F, ComponentResult::Source::Text, 1),
        makeComponent("doc-c", 0.7F, ComponentResult::Source::Text, 2),
    };

    EvidenceSearchPipeline pipeline;
    auto run = pipeline.execute(components, {}, config, 2);

    REQUIRE(run);
    CHECK(run.value().trace.aggregatedCandidates == 3);
    CHECK(run.value().trace.fusedCandidates == 3);
    CHECK(run.value().trace.rerankedCandidates == 3);
    CHECK(run.value().trace.finalCandidates == 2);
    CHECK(run.value().results.size() == 2);
}
