#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/WorkCoordinator.h>

#include "../../../common/test_helpers_catch2.h"

using namespace std::chrono_literals;

namespace yams::daemon {

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
