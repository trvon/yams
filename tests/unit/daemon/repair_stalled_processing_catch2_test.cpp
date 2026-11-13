// Idle repair of embedding attempts left Processing by a failed cleanup.
//
// An embed job marks its documents Processing before provider work. When the job fails while
// allocating, the exception cleanup can itself fail to enqueue a fenced status update, so the
// durable row stays Processing with an unfinished derivation. Startup recovery only runs once
// per process, so a long-lived daemon needs an idle, cutoff-based sweep using the existing
// stalled threshold. The sweep must never touch a newer attempt.

#include <catch2/catch_test_macros.hpp>

#include "../../common/metadata_test_db.h"
#include "../../common/test_helpers_catch2.h"

#include <yams/daemon/components/RepairService.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>

using namespace yams;
using namespace yams::daemon;
using namespace yams::metadata;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    return yams::test::migrated_metadata_db_template().clone(prefix);
}

class RepairDbFixture {
public:
    explicit RepairDbFixture(std::size_t maxConnections = 2) {
        dbPath_ = tempDbPath("yams_repair_stalled_processing_");

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = maxConnections;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        REQUIRE(pool_->initialize().has_value());

        repository_ = std::make_shared<MetadataRepository>(
            *pool_, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }

    ~RepairDbFixture() {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        yams::test::remove_sqlite_artifacts(dbPath_);
    }

    std::string insertPendingDocument(const std::string& hash) {
        DocumentInfo info;
        info.filePath = "/tmp/" + hash + ".txt";
        info.fileName = hash + ".txt";
        info.fileExtension = ".txt";
        info.fileSize = 16;
        info.sha256Hash = hash;
        info.mimeType = "text/plain";
        info.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        info.modifiedTime = info.createdTime;
        info.indexedTime = info.createdTime;
        populatePathDerivedFields(info);
        REQUIRE(repository_->insertDocument(info).has_value());
        return hash;
    }

    /// Marks the document's current derivation as in-flight and backdates when the attempt began.
    /// Returns the token so tests can prove that the reset invalidates it.
    EmbeddingDerivationToken beginAttempt(const std::string& hash, std::chrono::seconds age) {
        auto token = repository_->beginDocumentEmbeddingDerivation(hash, "recipe-v1");
        REQUIRE(token.has_value());
        REQUIRE(repository_->batchUpdateDocumentRepairStatuses({hash}, RepairStatus::Processing)
                    .has_value());
        backdateAttempt(hash, age);
        return token.value();
    }

    /// Re-enters Processing at an old timestamp without consuming retry budget.
    void forceProcessingAt(const std::string& hash, std::chrono::seconds age) {
        auto conn = pool_->acquire();
        REQUIRE(conn.has_value());
        auto& db = **conn.value();
        auto stmt = db.prepare(R"(
            UPDATE documents SET repair_status = 'processing', repair_attempted_at = unixepoch() - ?
            WHERE sha256_hash = ?
        )");
        REQUIRE(stmt.has_value());
        REQUIRE(stmt.value().bind(1, static_cast<int64_t>(age.count())).has_value());
        REQUIRE(stmt.value().bind(2, hash).has_value());
        REQUIRE(stmt.value().execute().has_value());
    }

    void backdateAttempt(const std::string& hash, std::chrono::seconds age) {
        auto conn = pool_->acquire();
        REQUIRE(conn.has_value());
        auto& db = **conn.value();
        auto stmt = db.prepare(R"(
            UPDATE documents SET repair_attempted_at = unixepoch() - ?
            WHERE sha256_hash = ?
        )");
        REQUIRE(stmt.has_value());
        REQUIRE(stmt.value().bind(1, static_cast<int64_t>(age.count())).has_value());
        REQUIRE(stmt.value().bind(2, hash).has_value());
        REQUIRE(stmt.value().execute().has_value());
    }

    std::optional<DocumentInfo> document(const std::string& hash) {
        auto result = repository_->getDocumentByHash(hash);
        REQUIRE(result.has_value());
        return result.value();
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> repository_;
};

RepairService makeService(RepairDbFixture& fix) {
    RepairServiceContext ctx;
    ctx.getMetadataRepo = [&fix] { return fix.repository_; };
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.stalledThreshold = std::chrono::seconds{3600};
    return RepairService(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
}

std::int64_t epochSecondsAgo(std::chrono::seconds ago) {
    return std::chrono::duration_cast<std::chrono::seconds>(
               (std::chrono::system_clock::now() - ago).time_since_epoch())
        .count();
}

} // namespace

TEST_CASE("RepairService recovers an idle stalled Processing attempt",
          "[unit][daemon][repair][embedding][catch2]") {
    RepairDbFixture fix;
    auto service = makeService(fix);

    SECTION("an attempt older than the cutoff returns to Pending") {
        const std::string hash = fix.insertPendingDocument("stalled-old");
        fix.beginAttempt(hash, std::chrono::seconds{7200});

        // Cutoff derived from the existing stalled threshold, not a sleep.
        const auto cutoff = epochSecondsAgo(std::chrono::seconds{3600});
        CHECK(service.recoverStalledProcessing(cutoff) == 1);

        auto doc = fix.document(hash);
        REQUIRE(doc.has_value());
        CHECK(doc->repairStatus == RepairStatus::Pending);
    }

    SECTION("a freshly attempted row is left alone") {
        const std::string hash = fix.insertPendingDocument("stalled-fresh");
        fix.beginAttempt(hash, std::chrono::seconds{0});

        const auto cutoff = epochSecondsAgo(std::chrono::seconds{3600});
        CHECK(service.recoverStalledProcessing(cutoff) == 0);

        auto doc = fix.document(hash);
        REQUIRE(doc.has_value());
        CHECK(doc->repairStatus == RepairStatus::Processing);
    }

    SECTION("a completed derivation is not reset") {
        const std::string hash = fix.insertPendingDocument("stalled-completed");
        auto token = fix.repository_->beginDocumentEmbeddingDerivation(hash, "recipe-v1");
        REQUIRE(token.has_value());
        REQUIRE(fix.repository_->batchUpdateDocumentRepairStatuses({hash}, RepairStatus::Completed)
                    .has_value());

        const auto cutoff = epochSecondsAgo(std::chrono::seconds{0});
        CHECK(service.recoverStalledProcessing(cutoff) == 0);

        auto doc = fix.document(hash);
        REQUIRE(doc.has_value());
        CHECK(doc->repairStatus == RepairStatus::Completed);
    }

    SECTION("a stale Processing row with no derivation is requeued") {
        const std::string hash = fix.insertPendingDocument("stalled-no-derivation");
        // A repair batch marks Processing before dispatch; the job can die before minting.
        REQUIRE(fix.repository_->batchUpdateDocumentRepairStatuses({hash}, RepairStatus::Processing)
                    .has_value());
        fix.backdateAttempt(hash, std::chrono::seconds{7200});

        const auto cutoff = epochSecondsAgo(std::chrono::seconds{3600});
        CHECK(service.recoverStalledProcessing(cutoff) == 1);

        auto doc = fix.document(hash);
        REQUIRE(doc.has_value());
        CHECK(doc->repairStatus == RepairStatus::Pending);
    }

    SECTION("the sweep is idempotent") {
        const std::string hash = fix.insertPendingDocument("stalled-twice");
        fix.beginAttempt(hash, std::chrono::seconds{7200});

        const auto cutoff = epochSecondsAgo(std::chrono::seconds{3600});
        CHECK(service.recoverStalledProcessing(cutoff) == 1);
        CHECK(service.recoverStalledProcessing(cutoff) == 0);
    }
}

TEST_CASE("Stalled repair reset reconciles and preserves attempt budget",
          "[unit][daemon][repair][embedding][catch2]") {
    RepairDbFixture fix;
    auto service = makeService(fix);
    const auto cutoff = epochSecondsAgo(std::chrono::seconds{3600});

    SECTION("a completed derivation leaves Processing for Completed with no reset") {
        const std::string hash = fix.insertPendingDocument("stalled-derivation-done");
        auto token = fix.repository_->beginDocumentEmbeddingDerivation(hash, "recipe-v1");
        REQUIRE(token.has_value());
        auto completed =
            fix.repository_->batchCompleteDocumentEmbeddingDerivations({token.value()}, "model-x");
        REQUIRE(completed.has_value());
        CHECK(completed.value() == 1);
        // Simulate cleanup that failed after the derivation published, leaving the row Processing.
        fix.forceProcessingAt(hash, std::chrono::seconds{7200});

        CHECK(service.recoverStalledProcessing(cutoff) == 0);

        auto doc = fix.document(hash);
        REQUIRE(doc.has_value());
        CHECK(doc->repairStatus == RepairStatus::Completed);
    }

    SECTION("an unfinished stale attempt returns to Pending without consuming budget") {
        const std::string hash = fix.insertPendingDocument("stalled-budget");
        fix.beginAttempt(hash, std::chrono::seconds{7200});

        auto before = fix.document(hash);
        REQUIRE(before.has_value());
        REQUIRE(before->repairAttempts == 1);

        CHECK(service.recoverStalledProcessing(cutoff) == 1);

        auto after = fix.document(hash);
        REQUIRE(after.has_value());
        CHECK(after->repairStatus == RepairStatus::Pending);
        CHECK(after->repairAttempts == before->repairAttempts);
        CHECK(after->repairAttemptedAt == before->repairAttemptedAt);
        CHECK(service.recoverStalledProcessing(cutoff) == 0);
    }

    SECTION("the pre-reset token can no longer publish an embedding") {
        const std::string hash = fix.insertPendingDocument("stalled-old-token");
        const auto oldToken = fix.beginAttempt(hash, std::chrono::seconds{7200});

        CHECK(service.recoverStalledProcessing(cutoff) == 1);

        const auto completed =
            fix.repository_->batchCompleteDocumentEmbeddingDerivations({oldToken}, "model-x");
        REQUIRE(completed.has_value());
        CHECK(completed.value() == 0);

        auto embedded = fix.repository_->hasDocumentEmbeddingByHash(hash);
        REQUIRE(embedded.has_value());
        CHECK_FALSE(embedded.value());
    }
}
