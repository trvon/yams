#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "../../../../src/daemon/components/embedding_derivation_policy.h"
#include "../../../../src/daemon/components/embedding_input_selection.h"
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/embed_preparer.h>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/WorkCoordinator.h>

#include "../../../common/test_helpers_catch2.h"

using namespace std::chrono_literals;

namespace yams::daemon {

TEST_CASE("Prepared embedding payload binds the extracted text snapshot",
          "[daemon][embedding][prepared-freshness]") {
    ConfigResolver::EmbeddingChunkingPolicy policy;
    ConfigResolver::EmbeddingSelectionPolicy selection;
    auto chunker = vector::createChunker(policy.strategy, policy.config, nullptr);
    REQUIRE(chunker);
    embed::EmbedSourceDoc source{"revision", "First extracted text.", "file", "/file",
                                 "text/plain"};
    auto prepared = embed::prepareEmbedPreparedDoc(source, *chunker, selection);
    REQUIRE(prepared);
    prepared->preparationRecipe = embed::embeddingPreparationRecipe(policy, selection);
    CHECK(embed::preparedEmbeddingMatches(*prepared, source.extractedText,
                                          prepared->preparationRecipe));
    CHECK_FALSE(embed::preparedEmbeddingMatches(*prepared, "Replacement extracted text.",
                                                prepared->preparationRecipe));
    CHECK_FALSE(embed::preparedEmbeddingMatches(*prepared, source.extractedText, "other-policy"));
    prepared->sourceTextHash.clear();
    CHECK_FALSE(embed::preparedEmbeddingMatches(*prepared, source.extractedText,
                                                prepared->preparationRecipe));
}

TEST_CASE("Embedding derivation policy binds model identity and preparation parameters",
          "[daemon][embedding][derivation-policy]") {
    ConfigResolver::EmbeddingChunkingPolicy chunking;
    ConfigResolver::EmbeddingSelectionPolicy selection;
    const auto preparation = embed::embeddingPreparationRecipe(chunking, selection);
    const auto recipe = embed::embeddingDerivationRecipe(preparation, "space", "v1", 384);
    CHECK(recipe == embed::embeddingDerivationRecipe(preparation, "space", "v1", 384));
    CHECK(recipe != embed::embeddingDerivationRecipe(preparation, "other-space", "v1", 384));
    CHECK(recipe != embed::embeddingDerivationRecipe(preparation, "space", "v2", 384));
    CHECK(recipe != embed::embeddingDerivationRecipe(preparation, "space", "v1", 768));
    chunking.config.overlap_size++;
    CHECK(preparation != embed::embeddingPreparationRecipe(chunking, selection));
    chunking.config.overlap_size--;
    selection.maxChunksPerDoc++;
    CHECK(preparation != embed::embeddingPreparationRecipe(chunking, selection));
}

TEST_CASE("Embedding input selection falls back for empty prepared payloads",
          "[daemon][embedding][input-selection]") {
    InternalEventBus::EmbedJob job;
    job.hashes = {"empty", "ready", "cold", "cold"};
    InternalEventBus::EmbedPreparedDoc empty;
    empty.hash = "empty";
    InternalEventBus::EmbedPreparedDoc ready;
    ready.hash = "ready";
    ready.chunks.emplace_back();
    job.preparedDocs = {empty, ready, ready};
    const auto selected = embed::selectEmbeddingInputs(job);
    REQUIRE(selected.preparedIndices == std::vector<std::size_t>{1});
    CHECK(selected.gatherHashes == std::vector<std::string>{"empty", "cold"});
}

TEST_CASE("Embedding input selection prefers a valid duplicate over an empty payload",
          "[daemon][embedding][input-selection]") {
    InternalEventBus::EmbedJob job;
    job.hashes = {"revision"};
    InternalEventBus::EmbedPreparedDoc empty;
    empty.hash = "revision";
    auto ready = empty;
    ready.chunks.emplace_back();
    job.preparedDocs = {empty, ready};
    const auto selected = embed::selectEmbeddingInputs(job);
    CHECK(selected.preparedIndices == std::vector<std::size_t>{1});
    CHECK(selected.gatherHashes.empty());
}

class EmbeddingServiceTimingTestAccess {
public:
    static void record(EmbeddingService& service, std::string_view phase) {
        service.recordPhaseTiming(phase, std::chrono::steady_clock::now() - 1ms);
    }
};

namespace {

class RecordingTimingSink final : public EmbeddingPhaseTimingSink {
public:
    void record(std::string_view phase, std::uint64_t elapsedUs) override {
        ++calls;
        lastPhase = phase;
        lastElapsedUs = elapsedUs;
    }

    std::size_t calls{0};
    std::string lastPhase;
    std::uint64_t lastElapsedUs{0};
};

class ThrowingTimingSink final : public EmbeddingPhaseTimingSink {
public:
    void record(std::string_view, std::uint64_t) override {
        throw std::runtime_error("timing sink failure");
    }
};

} // namespace

TEST_CASE("EmbeddingService shutdown before start leaves no deferred service access",
          "[daemon][embedding][shutdown][never-started]") {
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);
    service.shutdown();
    // Poll while service is still alive: the old shutdown queued a closure capturing it.
    // A never-started service must leave no such handler for a later coordinator start.
    CHECK(coordinator.getIOContext()->poll() == 0);
    service.shutdown();
    CHECK(coordinator.getIOContext()->poll() == 0);
}

TEST_CASE("EmbeddingService ignores benchmark environment in production policy",
          "[daemon][components][embedding][config][catch2]") {
    yams::test::ScopedEnvVar benchmarkProfile{"YAMS_BENCH_EMBED_PROFILE", "balanced"};
    yams::test::ScopedEnvVar productConcurrency{"YAMS_EMBED_COREML_SAFE_CONCURRENCY", std::nullopt};
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);

    const auto effective = service.effectiveConcurrencyPolicy();
    CHECK((effective.coremlUnifiedConcurrency == 1U));
    CHECK((effective.coremlUnifiedConcurrencySource == "default"));
}

TEST_CASE("EmbeddingService accepts typed CoreML concurrency policy",
          "[daemon][components][embedding][config][catch2]") {
    yams::test::ScopedEnvVar benchmarkProfile{"YAMS_BENCH_EMBED_PROFILE", "safe"};
    yams::test::ScopedEnvVar productConcurrency{"YAMS_EMBED_COREML_SAFE_CONCURRENCY", "7"};
    WorkCoordinator coordinator;
    EmbeddingServiceConfig policy;
    policy.coremlUnifiedConcurrency = 2U;
    policy.coremlUnifiedConcurrencySource = "harness:typed";
    EmbeddingService service(nullptr, nullptr, &coordinator, policy);

    const auto effective = service.effectiveConcurrencyPolicy();
    CHECK((effective.coremlUnifiedConcurrency == 2U));
    CHECK((effective.coremlUnifiedConcurrencySource == "harness:typed"));
}

TEST_CASE("EmbeddingService freezes the compatibility concurrency at construction",
          "[daemon][components][embedding][config][catch2]") {
    yams::test::ScopedEnvVar productConcurrency{"YAMS_EMBED_COREML_SAFE_CONCURRENCY", "3"};
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);

    {
        yams::test::ScopedEnvVar changedConcurrency{"YAMS_EMBED_COREML_SAFE_CONCURRENCY", "9"};
        const auto effective = service.effectiveConcurrencyPolicy();
        CHECK((effective.coremlUnifiedConcurrency == 3U));
        CHECK((effective.coremlUnifiedConcurrencySource ==
               "environment:YAMS_EMBED_COREML_SAFE_CONCURRENCY"));
    }
}

TEST_CASE("EmbeddingService phase timing is optional and replaceable",
          "[daemon][components][embedding][timing][catch2]") {
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);

    REQUIRE_NOTHROW(EmbeddingServiceTimingTestAccess::record(service, "unset"));

    auto first = std::make_shared<RecordingTimingSink>();
    service.setPhaseTimingSink(first);
    EmbeddingServiceTimingTestAccess::record(service, "infer");
    CHECK((first->calls == 1));
    CHECK((first->lastPhase == "infer"));
    CHECK((first->lastElapsedUs >= 1000));

    auto second = std::make_shared<RecordingTimingSink>();
    service.setPhaseTimingSink(second);
    EmbeddingServiceTimingTestAccess::record(service, "gather");
    CHECK((first->calls == 1));
    CHECK((second->calls == 1));
    CHECK((second->lastPhase == "gather"));

    service.setPhaseTimingSink(std::make_shared<ThrowingTimingSink>());
    REQUIRE_NOTHROW(EmbeddingServiceTimingTestAccess::record(service, "throwing"));
}

} // namespace yams::daemon
