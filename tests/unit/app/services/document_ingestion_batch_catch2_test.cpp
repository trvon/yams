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
