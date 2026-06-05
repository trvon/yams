#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/document_ingestion_service.h>
#include <yams/daemon/components/TuneAdvisor.h>

namespace {

struct TuneAdvisorBatchReset {
    ~TuneAdvisorBatchReset() {
        yams::daemon::TuneAdvisor::setEnableParallelIngest(true);
        yams::daemon::TuneAdvisor::setMaxIngestWorkers(0);
        yams::daemon::TuneAdvisor::setStoragePoolSize(0);
    }
};

} // namespace

TEST_CASE("DocumentIngestionService batch concurrency honors explicit limits",
          "[unit][app][services][ingestion][batch]") {
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(2, 8) ==
           2));
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(5, 3) ==
           3));
}

TEST_CASE("DocumentIngestionService batch concurrency auto-sizes from ingest tuning",
          "[unit][app][services][ingestion][batch]") {
    TuneAdvisorBatchReset reset;
    yams::daemon::TuneAdvisor::setEnableParallelIngest(true);
    yams::daemon::TuneAdvisor::setMaxIngestWorkers(8);
    yams::daemon::TuneAdvisor::setStoragePoolSize(4);

    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(3, 0) ==
           3));
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(160, 0) ==
           4));
}

TEST_CASE("DocumentIngestionService batch concurrency disables auto fan-out when ingest is off",
          "[unit][app][services][ingestion][batch]") {
    TuneAdvisorBatchReset reset;
    yams::daemon::TuneAdvisor::setEnableParallelIngest(false);
    yams::daemon::TuneAdvisor::setMaxIngestWorkers(8);
    yams::daemon::TuneAdvisor::setStoragePoolSize(4);

    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(160, 0) ==
           1));
}

TEST_CASE("DocumentIngestionService batch concurrency never returns zero",
          "[unit][app][services][ingestion][batch][assert]") {
    // YAMS_POSTCONDITION(result >= 1) must hold for all valid inputs.
    // Exercise edge cases that pass through different early-return branches.
    TuneAdvisorBatchReset reset;

    // Zero batch size returns 1 via early-return.
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(0, 0) ==
           1));
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(0, 8) ==
           1));

    // Single-item batch always returns 1.
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(1, 8) ==
           1));
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(1, 0) ==
           1));

    // With parallel ingest disabled and no explicit concurrency, returns 1.
    yams::daemon::TuneAdvisor::setEnableParallelIngest(false);
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(100, 0) ==
           1));

    // Storage cap of 0 defaults to 1.
    yams::daemon::TuneAdvisor::setEnableParallelIngest(true);
    yams::daemon::TuneAdvisor::setMaxIngestWorkers(0);
    yams::daemon::TuneAdvisor::setStoragePoolSize(0);
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(100, 0) >=
           1));
}

TEST_CASE("DocumentIngestionService batch concurrency respects explicit maxConcurrent",
          "[unit][app][services][ingestion][batch]") {
    TuneAdvisorBatchReset reset;

    // maxConcurrent > 0 bypasses all tuning — uses std::min(maxConcurrent, batchSize).
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(100, 1) ==
           1));
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(100, 4) ==
           4));
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(100, 8) ==
           8));

    // maxConcurrent clamps to batchSize.
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(3, 8) ==
           3));
    CHECK((yams::app::services::DocumentIngestionService::testing_resolveBatchConcurrency(1, 16) ==
           1));
}
