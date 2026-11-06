// RepairScanCursor and the startup scan: the scan resumes after hitting the queue cap and retries
// documents that are still unembedded, so an embedding-space migration re-embeds the whole corpus.

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <sqlite3.h>
#include <boost/asio/io_context.hpp>
#include <yams/api/content_store.h>
#include <yams/api/content_store_builder.h>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/extraction/content_extractor.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

using yams::daemon::RepairScanCursor;

TEST_CASE("RepairScanCursor runs once when the scan completes", "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0);
    CHECK_FALSE(cursor.shouldStart()); // in flight
    cursor.finished();
    CHECK_FALSE(cursor.shouldStart()); // whole corpus scanned
}

TEST_CASE("RepairScanCursor resumes where a capped scan stopped",
          "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0);
    cursor.stoppedAt(1000);
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 1000);
    cursor.stoppedAt(2000);
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 2000);
    cursor.finished();
    CHECK_FALSE(cursor.shouldStart());
}

TEST_CASE("RepairScanCursor never starts two scans at once", "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    REQUIRE(cursor.shouldStart());
    (void)cursor.begin();
    CHECK_FALSE(cursor.shouldStart());
    cursor.aborted(); // exception or shutdown mid-scan: allow a retry from the same offset
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.begin() == 0);
}

TEST_CASE("RepairScanCursor retries a sweep that queued work, a bounded number of times",
          "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    (void)cursor.begin();
    cursor.stoppedAt(500, /*queuedWork=*/true);
    CHECK_FALSE(cursor.retryDue()); // resuming, not retrying
    CHECK(cursor.begin() == 500);
    cursor.finished(); // the capped segment queued work earlier in the sweep
    REQUIRE(cursor.shouldStart());
    CHECK(cursor.retryDue());
    for (int i = 0; i < RepairScanCursor::kMaxRetrySweeps; ++i) {
        REQUIRE(cursor.shouldStart());
        CHECK(cursor.begin() == 0);
        cursor.finished(/*queuedWork=*/true);
    }
    CHECK_FALSE(cursor.shouldStart());
}

TEST_CASE("RepairScanCursor stops after a sweep that queued nothing",
          "[daemon][repair][scan][catch2]") {
    RepairScanCursor cursor;
    (void)cursor.begin();
    cursor.finished(/*queuedWork=*/true);
    REQUIRE(cursor.shouldStart());
    (void)cursor.begin();
    cursor.finished(/*queuedWork=*/false);
    CHECK_FALSE(cursor.shouldStart());
}

namespace {

using yams::daemon::RepairService;
using yams::daemon::RepairServiceContext;

// A metadata store with `n` documents that have content but no embeddings, and a RepairService
// over it with vectors enabled and no vector database (embedding state comes from metadata).
struct ScanHarness {
    yams::test::TempDirGuard tmp{"yams_repair_scan_"};
    std::unique_ptr<yams::metadata::ConnectionPool> pool;
    std::shared_ptr<yams::metadata::MetadataRepository> repo;
    std::vector<std::string> hashes;
    std::vector<int64_t> ids;
    std::unique_ptr<RepairService> service;
    std::vector<std::shared_ptr<yams::extraction::IContentExtractor>> extractors;
    // The unit lanes disable vectors through any of these; this harness needs embeddings on.
    yams::test::ScopedEnvVar vectorsOn{"YAMS_DISABLE_VECTORS", std::nullopt};
    yams::test::ScopedEnvVar vectorsOnAlias{"YAMS_DISABLE_VECTOR_DB", std::nullopt};
    yams::test::ScopedEnvVar vectorsOnSingular{"YAMS_DISABLE_VECTOR", std::nullopt};

    explicit ScanHarness(int n, std::size_t maxPending = 1000) {
        yams::daemon::ResourceGovernor::instance().testing_setPressureState(
            yams::daemon::ResourcePressureLevel::Normal, std::chrono::steady_clock::now());
        yams::metadata::ConnectionPoolConfig pc;
        pc.minConnections = 1;
        pc.maxConnections = 2;
        pool =
            std::make_unique<yams::metadata::ConnectionPool>((tmp.path() / "yams.db").string(), pc);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_shared<yams::metadata::MetadataRepository>(*pool);
        for (int i = 0; i < n; ++i) {
            yams::metadata::DocumentInfo doc;
            doc.fileName = "doc" + std::to_string(i) + ".txt";
            doc.filePath = (tmp.path() / doc.fileName).string();
            doc.fileExtension = ".txt";
            doc.fileSize = 4;
            doc.sha256Hash = std::string(63, 'a') + static_cast<char>('a' + i);
            doc.mimeType = "text/plain";
            doc.contentExtracted = true;
            doc.extractionStatus = yams::metadata::ExtractionStatus::Skipped;
            auto id = repo->insertDocument(doc);
            REQUIRE(id.has_value());
            ids.push_back(id.value());
            hashes.push_back(doc.sha256Hash);
        }
        RepairServiceContext ctx;
        ctx.getMetadataRepo = [r = repo] { return r; };
        ctx.getContentExtractors = [this] { return extractors; };
        RepairService::Config cfg;
        cfg.enable = true;
        cfg.dataDir = tmp.path();
        cfg.maxPendingRepairs = maxPending;
        // A data dir that already finished a sweep with no extractors; tests that need the
        // first-run retry remove it.
        // Binary, as the service writes it: text mode turns \n into \r\n on Windows, which reads
        // back as a changed extractor set.
        {
            std::ofstream marker(tmp.path() / "repair-extractor-set", std::ios::binary);
            REQUIRE(marker.is_open());
            marker << RepairService::extractorFingerprint({});
            REQUIRE(marker.good());
        }
        service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    }
    ~ScanHarness() {
        service.reset();
        repo.reset();
        pool->shutdown();
    }
    // Extraction ran and produced nothing (a binary blob): no content, no embedding.
    void markExtractionFailed(int i) {
        REQUIRE(repo->updateDocumentExtractionStatus(
            ids[i], false, yams::metadata::ExtractionStatus::Failed, "No text extracted"));
        REQUIRE(repo->updateDocumentRepairStatus(hashes[i], yams::metadata::RepairStatus::Failed));
    }
    bool queuedIn(const std::vector<std::string>& q, const std::string& h) const {
        return std::find(q.begin(), q.end(), h) != q.end();
    }
};

} // namespace

// A daemon killed during a re-embed leaves the batches it had queued marked Processing. The next
// start owns no such work, so those documents must be scanned again.
TEST_CASE("Startup scan re-queues documents a previous run left Processing",
          "[daemon][repair][scan][migrate][catch2]") {
    ScanHarness h(3);
    REQUIRE(
        h.repo->updateDocumentRepairStatus(h.hashes[1], yams::metadata::RepairStatus::Processing));
    const auto queued = h.service->testing_runStartupScanPass();
    CHECK(h.queuedIn(queued, h.hashes[0]));
    CHECK(h.queuedIn(queued, h.hashes[1]));
    CHECK(h.queuedIn(queued, h.hashes[2]));
}

// Capped passes resume from a remembered position. Deleting documents the scan already passed
// must not shift the remaining ones past it.
TEST_CASE("Startup scan covers every document when earlier ones are deleted between passes",
          "[daemon][repair][scan][migrate][catch2]") {
    yams::test::ScopedEnvVar batch{"YAMS_REPAIR_STARTUP_BATCH", std::string{"2"}};
    ScanHarness h(6, /*maxPending=*/3);

    std::set<std::string> seen;
    auto first = h.service->testing_runStartupScanPass();
    seen.insert(first.begin(), first.end());
    REQUIRE(first.size() == 3U); // capped
    REQUIRE(h.service->testing_startupScanDue());
    // The first three were embedded; the user then deletes two of them.
    for (int i = 0; i < 3; ++i)
        REQUIRE(h.repo->updateDocumentEmbeddingStatusByHash(h.hashes[i], true, "m"));
    REQUIRE(h.repo->deleteDocument(h.ids[0]));
    REQUIRE(h.repo->deleteDocument(h.ids[1]));

    for (int pass = 0; pass < 5 && h.service->testing_startupScanDue(); ++pass) {
        auto q = h.service->testing_runStartupScanPass();
        seen.insert(q.begin(), q.end());
    }
    for (int i = 3; i < 6; ++i) {
        INFO("document " << i);
        CHECK(seen.count(h.hashes[i]) == 1U);
    }
}

// Embedding can fail transiently right after a migration (provider still loading). Once the
// queue drains, documents that still have no vector must get another pass in the same run.
TEST_CASE("Startup scan runs again when documents are still unembedded after the queue drains",
          "[daemon][repair][scan][migrate][catch2]") {
    ScanHarness h(2);
    const auto queued = h.service->testing_runStartupScanPass();
    REQUIRE(queued.size() == 2U);
    // hashes[0] embedded; hashes[1] failed.
    REQUIRE(h.repo->updateDocumentEmbeddingStatusByHash(h.hashes[0], true, "m"));
    REQUIRE(h.repo->updateDocumentRepairStatus(h.hashes[1], yams::metadata::RepairStatus::Failed));
    REQUIRE(h.service->testing_startupScanDue());
    const auto retry = h.service->testing_runStartupScanPass();
    CHECK(retry == std::vector<std::string>{h.hashes[1]});
}

namespace {
// Claims every document, as a newly installed extractor plugin would for its formats.
class AcceptAllExtractor final : public yams::extraction::IContentExtractor {
public:
    bool supports(const std::string&, const std::string&) const override { return true; }
    std::optional<std::string> extractText(const std::vector<std::byte>&, const std::string&,
                                           const std::string&) override {
        return std::string{"text"};
    }
};
} // namespace

// A binary whose extraction failed has no content to embed or index. Re-queuing it every sweep
// failed the same ~1.6k documents 4 times per daemon run on the dev corpus and churned their
// status rows; it is terminal until something that could extract it changes.
TEST_CASE("Startup scan skips documents whose extraction failed with no content",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(3);
    h.markExtractionFailed(1);
    const auto queued = h.service->testing_runStartupScanPass();
    CHECK(h.queuedIn(queued, h.hashes[0]));
    CHECK_FALSE(h.queuedIn(queued, h.hashes[1]));
    CHECK(h.queuedIn(queued, h.hashes[2]));
}

TEST_CASE("Startup scan does not retry a sweep whose only candidates are terminal",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(2);
    h.markExtractionFailed(0);
    h.markExtractionFailed(1);
    CHECK(h.service->testing_runStartupScanPass().empty());
    CHECK_FALSE(h.service->testing_startupScanDue());
}

// Documents the per-document check would drop (already embedded, extraction pending, a format
// nothing can extract) must not be queued either, or every sweep counts as "queued work" and
// re-arms a retry for nothing.
TEST_CASE("Startup scan only queues documents the repair batch would act on",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(1);
    REQUIRE(h.repo->updateDocumentEmbeddingStatusByHash(h.hashes[0], true, "m"));
    auto doc = h.repo->getDocumentByHash(h.hashes[0]);
    REQUIRE(doc.has_value());
    REQUIRE(doc.value().has_value());
    auto info = *doc.value();
    info.mimeType = "application/x-unextractable";
    info.fileExtension = ".bin";
    REQUIRE(h.repo->updateDocument(info));
    REQUIRE(h.repo->updateDocumentExtractionStatus(h.ids[0], false,
                                                   yams::metadata::ExtractionStatus::Pending));
    CHECK(h.service->testing_runStartupScanPass().empty());
    CHECK_FALSE(h.service->testing_startupScanDue());
}

TEST_CASE("Startup scan retries terminal documents once the extractor set changes",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(1);
    h.markExtractionFailed(0);
    REQUIRE(h.service->testing_runStartupScanPass().empty());

    // A new extractor plugin is installed; the next daemon run scans from the start.
    h.extractors.push_back(std::make_shared<AcceptAllExtractor>());
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = h.tmp.path();
    RepairServiceContext ctx;
    ctx.getMetadataRepo = [r = h.repo] { return r; };
    ctx.getContentExtractors = [&h] { return h.extractors; };
    h.service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    const auto queued = h.service->testing_runStartupScanPass();
    CHECK(h.queuedIn(queued, h.hashes[0]));

    // Once that sweep finished, the same extractor set does not retry it again.
    h.service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    CHECK(h.service->testing_runStartupScanPass().empty());
}

// The extractor set is recorded only when a sweep finishes. A first sweep that stops at the queue
// cap must leave no marker, or documents after the stop point would lose their retry.
TEST_CASE("Startup scan records the extractor set only after a finished sweep",
          "[daemon][repair][scan][terminal][catch2]") {
    yams::test::ScopedEnvVar batch{"YAMS_REPAIR_STARTUP_BATCH", std::string{"1"}};
    ScanHarness h(3, /*maxPending=*/1);
    const auto marker = h.tmp.path() / "repair-extractor-set";
    std::filesystem::remove(marker); // a data dir from before the marker existed
    REQUIRE(h.service->testing_runStartupScanPass().size() == 1U); // capped
    CHECK_FALSE(std::filesystem::exists(marker));
    for (int pass = 0; pass < 6 && h.service->testing_startupScanDue(); ++pass) {
        (void)h.service->testing_runStartupScanPass();
    }
    CHECK(std::filesystem::exists(marker));
}

// Ingest and explicit requests also feed the repair queue. Documents handled before the scan has
// queued anything must not count toward scan documents queued later.
TEST_CASE("Repair progress does not pre-count documents outside the scan backlog",
          "[daemon][repair][progress][catch2]") {
    yams::test::ScopedEnvVar batch{"YAMS_REPAIR_STARTUP_BATCH", std::string{"2"}};
    ScanHarness h(6, /*maxPending=*/3);
    REQUIRE(h.service->testing_runStartupScanPass().size() == 3U); // capped
    h.service->testing_noteRepairResolved(3);
    CHECK(h.service->testing_repairProgress() == std::pair<std::uint64_t, std::uint64_t>{3, 3});

    // Freshly ingested documents are handled before the scan resumes.
    h.service->testing_noteRepairResolved(2);
    REQUIRE(h.service->testing_startupScanDue());
    REQUIRE(h.service->testing_runStartupScanPass().size() == 3U); // resumed pass
    CHECK(h.service->testing_repairProgress() == std::pair<std::uint64_t, std::uint64_t>{6, 3});
}

// A data dir from before the marker existed: documents that failed extraction under unknown rules
// (here, PEM files that were not yet treated as text) get one retry sweep, then stay terminal.
TEST_CASE("Startup scan retries terminal documents once when no extractor set is recorded",
          "[daemon][repair][scan][terminal][catch2]") {
    ScanHarness h(1);
    std::filesystem::remove(h.tmp.path() / "repair-extractor-set");
    h.markExtractionFailed(0);
    RepairServiceContext ctx;
    ctx.getMetadataRepo = [r = h.repo] { return r; };
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = h.tmp.path();
    h.service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    CHECK(h.queuedIn(h.service->testing_runStartupScanPass(), h.hashes[0]));

    h.service = std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    CHECK(h.service->testing_runStartupScanPass().empty());
}

// A retry sweep that finds nothing to do must not wipe the finished sweep's progress to 0/0:
// status would read as if no repair had happened, and a status poll could miss the result.
TEST_CASE("Repair progress survives a retry sweep that queues nothing",
          "[daemon][repair][progress][catch2]") {
    ScanHarness h(2);
    REQUIRE(h.service->testing_runStartupScanPass().size() == 2U);
    h.service->testing_noteRepairResolved(2);
    REQUIRE(h.service->testing_repairProgress() == std::pair<std::uint64_t, std::uint64_t>{2, 2});
    for (int i = 0; i < 2; ++i)
        REQUIRE(h.repo->updateDocumentEmbeddingStatusByHash(h.hashes[i], true, "m"));
    REQUIRE(h.service->testing_startupScanDue()); // the sweep queued work, so a retry is armed
    CHECK(h.service->testing_runStartupScanPass().empty());
    CHECK(h.service->testing_repairProgress() == std::pair<std::uint64_t, std::uint64_t>{2, 2});
}

namespace {
using yams::Result;
// Content store where only existence matters: present, missing, or the check itself fails.
class ExistenceStore final : public yams::api::IContentStore {
public:
    std::set<std::string> present;
    std::set<std::string> failing;

    Result<bool> exists(const std::string& hash) const override {
        if (failing.contains(hash))
            return yams::Error{yams::ErrorCode::IOError, "storage temporarily unavailable"};
        return present.contains(hash);
    }

    template <typename T> static Result<T> no() {
        return yams::Error{yams::ErrorCode::NotImplemented, "not used by this test"};
    }
    Result<yams::api::StoreResult> store(const std::filesystem::path&,
                                         const yams::api::ContentMetadata&,
                                         yams::api::ProgressCallback) override {
        return no<yams::api::StoreResult>();
    }
    Result<yams::api::RetrieveResult> retrieve(const std::string&, const std::filesystem::path&,
                                               yams::api::ProgressCallback) override {
        return no<yams::api::RetrieveResult>();
    }
    Result<yams::api::StoreResult> storeStream(std::istream&, const yams::api::ContentMetadata&,
                                               yams::api::ProgressCallback) override {
        return no<yams::api::StoreResult>();
    }
    Result<yams::api::RetrieveResult> retrieveStream(const std::string&, std::ostream&,
                                                     yams::api::ProgressCallback) override {
        return no<yams::api::RetrieveResult>();
    }
    Result<yams::api::StoreResult> storeBytes(std::span<const std::byte>,
                                              const yams::api::ContentMetadata&) override {
        return no<yams::api::StoreResult>();
    }
    Result<std::vector<std::byte>> retrieveBytes(const std::string&) override {
        return no<std::vector<std::byte>>();
    }
    Result<std::vector<std::byte>> retrieveBytesPrefix(const std::string&, std::size_t) override {
        return no<std::vector<std::byte>>();
    }
    Result<RawContent> retrieveRaw(const std::string&) override { return no<RawContent>(); }
    std::future<Result<RawContent>> retrieveRawAsync(const std::string&) override {
        return std::async(std::launch::deferred, [] { return no<RawContent>(); });
    }
    Result<bool> remove(const std::string&) override { return no<bool>(); }
    Result<yams::api::ContentMetadata> getMetadata(const std::string&) const override {
        return no<yams::api::ContentMetadata>();
    }
    Result<void> updateMetadata(const std::string&, const yams::api::ContentMetadata&) override {
        return yams::Error{yams::ErrorCode::NotImplemented, "not used"};
    }
    std::vector<Result<yams::api::StoreResult>>
    storeBatch(const std::vector<std::filesystem::path>&,
               const std::vector<yams::api::ContentMetadata>&) override {
        return {};
    }
    std::vector<Result<bool>> removeBatch(const std::vector<std::string>&) override { return {}; }
    yams::api::ContentStoreStats getStats() const override { return {}; }
    yams::api::HealthStatus checkHealth() const override { return {}; }
    Result<void> verify(yams::api::ProgressCallback) override { return {}; }
    Result<void> compact(yams::api::ProgressCallback) override { return {}; }
    Result<void> garbageCollect(yams::api::ProgressCallback) override { return {}; }
};

std::unique_ptr<RepairService> serviceWithStore(ScanHarness& h,
                                                std::shared_ptr<ExistenceStore> store) {
    RepairServiceContext ctx;
    ctx.getMetadataRepo = [r = h.repo] { return r; };
    ctx.getContentStore = [store] { return store; };
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = h.tmp.path();
    return std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
}

bool documentExists(ScanHarness& h, int i) {
    auto doc = h.repo->getDocumentByHash(h.hashes[i]);
    REQUIRE(doc.has_value());
    return doc.value().has_value();
}
} // namespace

// `yams repair --orphans` deletes metadata whose content blob is gone. It is destructive, so only
// documents whose blob is known to be missing may go.
TEST_CASE("Orphan cleanup deletes metadata only for documents whose blob is missing",
          "[daemon][repair][orphans][catch2]") {
    ScanHarness h(3);
    auto store = std::make_shared<ExistenceStore>();
    store->present = {h.hashes[0], h.hashes[2]};
    auto service = serviceWithStore(h, store);

    SECTION("a dry run reports the orphan and deletes nothing") {
        const auto result = service->testing_cleanOrphanedMetadata(/*dryRun=*/true);
        CHECK(result.skipped == 1);
        CHECK(result.succeeded == 0);
        CHECK(documentExists(h, 1));
    }
    SECTION("a real run deletes only the orphan") {
        const auto result = service->testing_cleanOrphanedMetadata(/*dryRun=*/false);
        CHECK(result.succeeded == 1);
        CHECK(documentExists(h, 0));
        CHECK_FALSE(documentExists(h, 1));
        CHECK(documentExists(h, 2));
    }
}

// A failed existence check is not evidence the blob is gone: a transient storage error must not
// turn into deleted metadata.
TEST_CASE("Orphan cleanup keeps documents whose blob existence cannot be checked",
          "[daemon][repair][orphans][catch2]") {
    ScanHarness h(2);
    auto store = std::make_shared<ExistenceStore>();
    store->present = {h.hashes[0]};
    store->failing = {h.hashes[1]};
    auto service = serviceWithStore(h, store);

    const auto result = service->testing_cleanOrphanedMetadata(/*dryRun=*/false);
    CHECK(result.succeeded == 0);
    CHECK(result.failed == 1);
    CHECK(documentExists(h, 0));
    CHECK(documentExists(h, 1));
}

namespace {
// A real content store under <dataDir>/storage holding one multi-chunk document, plus a planted
// object file nothing references.
struct ChunkStoreHarness {
    ScanHarness h{0};
    std::unique_ptr<yams::api::IContentStore> store;
    std::string documentHash;
    std::vector<std::byte> payload;
    std::filesystem::path planted;

    ChunkStoreHarness() {
        auto built = yams::api::ContentStoreBuilder()
                         .withStoragePath(h.tmp.path() / "storage")
                         .withChunkSize(65536)
                         .withCompression(false)
                         .withDeduplication(true)
                         .build();
        INFO((built ? std::string{} : built.error().message));
        REQUIRE(built);
        store = std::move(built.value());
        payload.resize(1024 * 1024);
        std::uint32_t x = 2463534242U;
        for (auto& b : payload) {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            b = static_cast<std::byte>(x & 0xFF);
        }
        auto stored = store->storeBytes(payload);
        REQUIRE(stored);
        documentHash = stored.value().contentHash;
        planted = h.tmp.path() / "storage" / "objects" / "ff" / std::string(62, 'e');
        std::filesystem::create_directories(planted.parent_path());
        std::ofstream(planted, std::ios::binary) << "orphaned";
    }

    std::filesystem::path refsDb() const { return h.tmp.path() / "storage" / "refs.db"; }

    // Simulate reference-count drift on one of the document's chunks.
    std::string driftOneChunk() {
        sqlite3* db = nullptr;
        REQUIRE(sqlite3_open(refsDb().string().c_str(), &db) == SQLITE_OK);
        sqlite3_stmt* stmt = nullptr;
        REQUIRE(sqlite3_prepare_v2(db,
                                   "SELECT block_hash FROM block_references WHERE ref_count > 0 "
                                   "AND block_hash <> ? ORDER BY block_hash LIMIT 1",
                                   -1, &stmt, nullptr) == SQLITE_OK);
        sqlite3_bind_text(stmt, 1, documentHash.c_str(), -1, SQLITE_TRANSIENT);
        REQUIRE(sqlite3_step(stmt) == SQLITE_ROW);
        std::string chunk = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        sqlite3_finalize(stmt);
        const auto sql =
            "UPDATE block_references SET ref_count = 0 WHERE block_hash = '" + chunk + "'";
        REQUIRE(sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK);
        sqlite3_close(db);
        return chunk;
    }

    std::filesystem::path objectPath(const std::string& hash) const {
        return h.tmp.path() / "storage" / "objects" / hash.substr(0, 2) / hash.substr(2);
    }

    std::unique_ptr<RepairService> service() {
        RepairServiceContext ctx;
        ctx.getMetadataRepo = [r = h.repo] { return r; };
        RepairService::Config cfg;
        cfg.enable = true;
        cfg.dataDir = h.tmp.path();
        return std::make_unique<RepairService>(ctx, nullptr, [] { return std::size_t{0}; }, cfg);
    }
};
} // namespace

// `yams repair --chunks` deletes object files nothing references. A manifest is a reference even
// when refs.db has drifted to zero for one of its chunks; deleting it would lose the document.
TEST_CASE("Chunk cleanup keeps chunks a manifest references despite reference-count drift",
          "[daemon][repair][chunks][catch2]") {
    ChunkStoreHarness c;
    const auto drifted = c.driftOneChunk();
    REQUIRE(std::filesystem::exists(c.objectPath(drifted)));

    const auto result = c.service()->testing_cleanOrphanedChunks(/*dryRun=*/false);
    INFO(result.message);
    CHECK_FALSE(std::filesystem::exists(c.planted));
    CHECK(std::filesystem::exists(c.objectPath(drifted)));
    auto back = c.store->retrieveBytes(c.documentHash);
    REQUIRE(back);
    CHECK(back.value() == c.payload);
}

// If any manifest cannot be read, the set of referenced chunks is unknown: delete nothing.
TEST_CASE("Chunk cleanup deletes nothing when a manifest cannot be parsed",
          "[daemon][repair][chunks][catch2]") {
    ChunkStoreHarness c;
    (void)c.driftOneChunk();
    const auto corrupt =
        c.h.tmp.path() / "storage" / "manifests" / "00" / (std::string(62, '0') + ".manifest");
    std::filesystem::create_directories(corrupt.parent_path());
    std::ofstream(corrupt, std::ios::binary) << "not a manifest";

    const auto result = c.service()->testing_cleanOrphanedChunks(/*dryRun=*/false);
    INFO(result.message);
    CHECK(result.succeeded == 0);
    CHECK(result.failed >= 1);
    CHECK(std::filesystem::exists(c.planted));
}

// With a WriteCoordinator the orphan node deletes are batched and applied after the scan, so the
// scan must not rewind its paging offset as if they had already happened: a full page of
// orphans would then be re-read forever, appending the same deletes without bound.
TEST_CASE("KG orphan cleanup finishes when more than a page of nodes is orphaned",
          "[daemon][repair][kg][catch2]") {
    ScanHarness h(0);
    auto kgRes = yams::metadata::makeSqliteKnowledgeGraphStore(*h.pool);
    REQUIRE(kgRes);
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg = std::move(kgRes.value());
    constexpr int kOrphans = 1200; // more than two 500-node scan pages
    for (int i = 0; i < kOrphans; ++i) {
        yams::metadata::KGNode node;
        node.nodeKey = "doc:orphan-" + std::to_string(i);
        node.type = "document";
        REQUIRE(kg->upsertNode(node));
    }

    boost::asio::io_context io;
    yams::daemon::WriteCoordinator::Config config;
    config.maxBatchSize = 64;
    config.maxBatchDelayMs = std::chrono::milliseconds{1};
    config.channelCapacity = 64;
    yams::daemon::WriteCoordinator coordinator(io, kg, {}, config);
    coordinator.start();
    std::thread writer([&io] { io.run(); });

    RepairServiceContext ctx;
    ctx.getMetadataRepo = [r = h.repo] { return r; };
    ctx.getKgStore = [kg] { return kg; };
    ctx.getWriteCoordinator = [&coordinator] { return &coordinator; };
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = h.tmp.path();
    RepairService service(ctx, nullptr, [] { return std::size_t{0}; }, cfg);

    auto run =
        std::async(std::launch::async, [&] { return service.testing_cleanOrphanedKgEntries(); });
    const bool finished = run.wait_for(std::chrono::seconds(60)) == std::future_status::ready;
    CHECK(finished);
    if (finished) {
        const auto [scanned, deleted, errors] = run.get();
        CHECK(deleted == static_cast<std::uint64_t>(kOrphans));
        CHECK(errors == 0);
        CHECK(scanned < static_cast<std::uint64_t>(3 * kOrphans));
        auto remaining = kg->findNodesByType("document", 2000, 0);
        REQUIRE(remaining);
        CHECK(remaining.value().empty());
    }
    coordinator.shutdown();
    io.stop();
    writer.join();
    if (!finished)
        std::_Exit(1); // the scan is still looping on another thread; do not hang the suite
}

// `yams repair --downloads` turns documents whose path is a URL into downloaded files: the path
// becomes the file name and the URL moves to source_url with host and scheme tags.
TEST_CASE("Download repair keeps URL paths and records their source",
          "[daemon][repair][downloads][catch2]") {
    ScanHarness h(0);
    const auto addUrlDoc = [&](const std::string& url, char fill, bool withLocation) {
        yams::metadata::DocumentInfo doc;
        doc.filePath = url;
        doc.fileName = url;
        doc.sha256Hash = std::string(64, fill);
        doc.mimeType = "text/html";
        auto id = h.repo->insertDocument(doc);
        REQUIRE(id.has_value());
        if (withLocation)
            REQUIRE(h.repo->recordDocumentPath(doc, id.value(), {}).has_value());
        return id.value();
    };
    // The first has a live path head, as current ingest creates; the second is a legacy row.
    const auto first = addUrlDoc("https://a.example/x/report.pdf?dl=1", 'a', true);
    const auto second = addUrlDoc("https://b.example/y/report.pdf", 'b', false);
    RepairServiceContext ctx;
    ctx.getMetadataRepo = [r = h.repo] { return r; };
    RepairService::Config cfg;
    cfg.enable = true;
    cfg.dataDir = h.tmp.path();
    RepairService service(ctx, nullptr, [] { return std::size_t{0}; }, cfg);

    SECTION("a dry run changes nothing") {
        const auto result = service.testing_repairDownloads(/*dryRun=*/true);
        CHECK(result.processed == 2);
        CHECK(result.succeeded == 0);
        auto doc = h.repo->getDocument(first);
        REQUIRE(doc.has_value());
        REQUIRE(doc.value().has_value());
        CHECK(doc.value()->filePath == "https://a.example/x/report.pdf?dl=1");
    }
    SECTION("a real run moves the URL into metadata") {
        const auto result = service.testing_repairDownloads(/*dryRun=*/false);
        INFO(result.message);
        auto doc = h.repo->getDocument(first);
        REQUIRE(doc.value().has_value());
        CHECK(doc.value()->fileName == "report.pdf");
        CHECK(doc.value()->fileExtension == ".pdf");
        auto url = h.repo->getMetadata(first, "source_url");
        REQUIRE(url.has_value());
        REQUIRE(url.value().has_value());
        CHECK(url.value()->asString() == "https://a.example/x/report.pdf?dl=1");
        auto host = h.repo->getMetadata(first, "tag:host:a.example");
        REQUIRE(host.has_value());
        CHECK(host.value().has_value());

        // Both documents keep distinct paths even though their file names match.
        auto other = h.repo->getDocument(second);
        REQUIRE(other.value().has_value());
        auto otherUrl = h.repo->getMetadata(second, "source_url");
        REQUIRE(otherUrl.has_value());
        REQUIRE(otherUrl.value().has_value());
        CHECK(otherUrl.value()->asString() == "https://b.example/y/report.pdf");
        CHECK(result.succeeded == 2);
        CHECK(doc.value()->filePath == "https://a.example/x/report.pdf?dl=1");
        CHECK(other.value()->filePath == "https://b.example/y/report.pdf");

        // Path-aware listings read the name from the live location, and the location carries
        // the mirrored tags.
        yams::metadata::DocumentQueryOptions options;
        options.usePathLocations = true;
        options.currentPathsOnly = true;
        options.tags = {"host:a.example"};
        auto listed = h.repo->queryDocumentsForListProjection(options);
        REQUIRE(listed.has_value());
        REQUIRE_FALSE(listed.value().empty());
        for (const auto& d : listed.value()) {
            CHECK(d.id == first);
            CHECK(d.fileName == "report.pdf");
            CHECK(d.fileExtension == ".pdf");
        }
        auto locations = h.repo->getDocumentPathReferences(first);
        REQUIRE(locations.has_value());
        std::size_t heads = 0;
        for (const auto& location : locations.value()) {
            auto located = h.repo->getDocumentPathMetadata(location.filePath, first);
            REQUIRE(located.has_value());
            if (!located.value())
                continue; // a legacy row with no location metadata
            ++heads;
            CHECK(located.value()->count("tag:host:a.example") == 1);
            CHECK(located.value()->count("tag:scheme:https") == 1);
            CHECK(located.value()->count("tag:downloaded") == 1);
        }
        CHECK(heads >= 1);

        // A second run finds nothing left to repair.
        const auto again = service.testing_repairDownloads(/*dryRun=*/false);
        CHECK(again.processed == 0);
    }
}
