// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/compat/thread_stop_compat.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include "../../common/metadata_test_db.h"

#include <catch2/catch_test_macros.hpp>

#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace yams;
using namespace yams::metadata;

namespace {

class SpdlogCaptureGuard {
public:
    explicit SpdlogCaptureGuard(spdlog::level::level_enum level)
        : previousLogger_(spdlog::default_logger()), previousLevel_(spdlog::get_level()) {
        auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream_);
        logger_ = std::make_shared<spdlog::logger>("metadata_content_transaction_capture", sink);
        logger_->set_level(level);
        logger_->set_pattern("[%l] %v");
        spdlog::set_default_logger(logger_);
        spdlog::set_level(level);
    }

    ~SpdlogCaptureGuard() {
        if (previousLogger_) {
            spdlog::set_default_logger(previousLogger_);
        }
        spdlog::set_level(previousLevel_);
    }

    std::string str() const { return stream_.str(); }

private:
    std::ostringstream stream_;
    std::shared_ptr<spdlog::logger> previousLogger_;
    std::shared_ptr<spdlog::logger> logger_;
    spdlog::level::level_enum previousLevel_;
};

DocumentInfo makePendingDocument(std::string path, std::string hash) {
    DocumentInfo info;
    info.filePath = std::move(path);
    info.fileName = std::filesystem::path(info.filePath).filename().string();
    info.fileExtension = std::filesystem::path(info.filePath).extension().string();
    info.fileSize = 1234;
    info.sha256Hash = std::move(hash);
    info.mimeType = "text/plain";
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    info.contentExtracted = false;
    info.extractionStatus = ExtractionStatus::Pending;
    populatePathDerivedFields(info);
    return info;
}

BatchContentEntry makeContentEntry(int64_t documentId, std::string title, std::string content,
                                   std::string metadataTitle = {}) {
    BatchContentEntry entry;
    entry.documentId = documentId;
    entry.title = std::move(title);
    entry.contentText = std::move(content);
    entry.mimeType = "text/plain";
    entry.extractionMethod = "transaction-test";
    entry.language = "en";
    entry.metadataTitle = std::move(metadataTitle);
    return entry;
}

class ContentTransactionFixture {
public:
    ContentTransactionFixture()
        : dbPath_(
              yams::test::migrated_metadata_db_template().clone("metadata_content_transaction_")) {
        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        config.busyTimeout = std::chrono::milliseconds(10);
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        REQUIRE(pool_->initialize().has_value());
        repository_ = std::make_unique<MetadataRepository>(
            *pool_, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }

    ~ContentTransactionFixture() {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        yams::test::remove_sqlite_artifacts(dbPath_);
    }

    Result<void> execute(std::string sql) {
        return pool_->withConnection([&](Database& db) -> Result<void> { return db.execute(sql); });
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

struct FtsSurface {
    std::string content;
    std::string title;

    bool operator==(const FtsSurface&) const = default;
};

Result<std::optional<FtsSurface>> getFtsSurface(ConnectionPool& pool, int64_t documentId) {
    return pool.withConnection([&](Database& db) -> Result<std::optional<FtsSurface>> {
        auto statement = db.prepare("SELECT content, title FROM documents_fts WHERE rowid = ?");
        if (!statement) {
            return statement.error();
        }
        auto bind = statement.value().bind(1, documentId);
        if (!bind) {
            return bind.error();
        }
        auto row = statement.value().step();
        if (!row) {
            return row.error();
        }
        if (!row.value()) {
            return std::optional<FtsSurface>{};
        }
        return std::optional<FtsSurface>{
            FtsSurface{statement.value().getString(0), statement.value().getString(1)}};
    });
}

void requireRolledBack(ContentTransactionFixture& fix, const std::vector<int64_t>& documentIds,
                       uint64_t extractedBefore, uint64_t indexedBefore) {
    for (const auto documentId : documentIds) {
        auto content = fix.repository_->getContent(documentId);
        REQUIRE(content.has_value());
        CHECK_FALSE(content.value().has_value());

        auto title = fix.repository_->getMetadata(documentId, "title");
        REQUIRE(title.has_value());
        CHECK_FALSE(title.value().has_value());

        auto fts = getFtsSurface(*fix.pool_, documentId);
        REQUIRE(fts.has_value());
        CHECK_FALSE(fts.value().has_value());

        auto document = fix.repository_->getDocument(documentId);
        REQUIRE(document.has_value());
        REQUIRE(document.value().has_value());
        CHECK_FALSE(document.value()->contentExtracted);
        CHECK((document.value()->extractionStatus == ExtractionStatus::Pending));
        CHECK((document.value()->repairStatus != RepairStatus::Completed));
        CHECK((document.value()->repairAttempts == 0));
    }

    CHECK((fix.repository_->getCachedExtractedCount() == extractedBefore));
    CHECK((fix.repository_->getCachedIndexedCount() == indexedBefore));
    auto indexedIds = fix.repository_->getAllFts5IndexedDocumentIds();
    REQUIRE(indexedIds.has_value());
    CHECK(indexedIds.value().empty());
}

struct ObservableContentState {
    std::string content;
    std::string metadataTitle;
    FtsSurface fts;
    bool extracted{false};
    ExtractionStatus extractionStatus{ExtractionStatus::Pending};
    RepairStatus repairStatus{RepairStatus::Pending};
    int repairAttempts{0};

    bool operator==(const ObservableContentState&) const = default;
};

Result<ObservableContentState> readObservableState(ContentTransactionFixture& fix,
                                                   int64_t documentId) {
    ObservableContentState state;
    auto content = fix.repository_->getContent(documentId);
    if (!content) {
        return content.error();
    }
    if (!content.value()) {
        return Error{ErrorCode::NotFound, "content missing"};
    }
    state.content = content.value()->contentText;

    auto title = fix.repository_->getMetadata(documentId, "title");
    if (!title) {
        return title.error();
    }
    if (!title.value()) {
        return Error{ErrorCode::NotFound, "title metadata missing"};
    }
    state.metadataTitle = title.value()->value;

    auto fts = getFtsSurface(*fix.pool_, documentId);
    if (!fts) {
        return fts.error();
    }
    if (!fts.value()) {
        return Error{ErrorCode::NotFound, "FTS row missing"};
    }
    state.fts = *fts.value();

    auto document = fix.repository_->getDocument(documentId);
    if (!document) {
        return document.error();
    }
    if (!document.value()) {
        return Error{ErrorCode::NotFound, "document missing"};
    }
    state.extracted = document.value()->contentExtracted;
    state.extractionStatus = document.value()->extractionStatus;
    state.repairStatus = document.value()->repairStatus;
    state.repairAttempts = document.value()->repairAttempts;
    return state;
}

} // namespace

TEST_CASE("batch content transaction rolls back persisted and cached surfaces",
          "[unit][metadata-repo][batch-content][transaction][rollback]") {
    ContentTransactionFixture fix;
    const auto first = fix.repository_->insertDocument(
        makePendingDocument("/tmp/rollback-first.txt", "rollback-first-hash"));
    const auto second = fix.repository_->insertDocument(
        makePendingDocument("/tmp/rollback-second.txt", "rollback-second-hash"));
    REQUIRE(first.has_value());
    REQUIRE(second.has_value());

    fix.repository_->initializeCounters();
    const auto extractedBefore = fix.repository_->getCachedExtractedCount();
    const auto indexedBefore = fix.repository_->getCachedIndexedCount();
    auto warmedIds = fix.repository_->getAllFts5IndexedDocumentIds();
    REQUIRE(warmedIds.has_value());
    REQUIRE(warmedIds.value().empty());

    const auto firstId = first.value();
    const auto secondId = second.value();
    std::vector<BatchContentEntry> entries{
        makeContentEntry(firstId, "First title", "First content", "First metadata title"),
        makeContentEntry(secondId, "Second title", "Second content", "Second metadata title")};

    SECTION("content upsert failure after an earlier row") {
        REQUIRE(fix.execute("CREATE TRIGGER fail_batch_content BEFORE INSERT ON document_content "
                            "WHEN NEW.document_id = " +
                            std::to_string(secondId) +
                            " BEGIN SELECT RAISE(ABORT, 'content boundary'); END")
                    .has_value());
    }

    SECTION("metadata failure after content and FTS upserts") {
        REQUIRE(fix.execute("CREATE TRIGGER fail_batch_metadata BEFORE INSERT ON metadata "
                            "WHEN NEW.key = 'title' BEGIN SELECT RAISE(ABORT, 'metadata "
                            "boundary'); END")
                    .has_value());
    }

    SECTION("status failure after content FTS and metadata upserts") {
        REQUIRE(fix.execute("CREATE TRIGGER fail_batch_status BEFORE UPDATE ON documents "
                            "WHEN NEW.repair_status = 'completed' BEGIN SELECT RAISE(ABORT, "
                            "'status boundary'); END")
                    .has_value());
    }

    SECTION("deferred constraint failure at commit") {
        REQUIRE(fix.execute("PRAGMA foreign_keys = ON").has_value());
        REQUIRE(
            fix.execute("CREATE TABLE batch_commit_parent(id INTEGER PRIMARY KEY)").has_value());
        REQUIRE(
            fix.execute("CREATE TABLE batch_commit_guard(document_id INTEGER, parent_id INTEGER, "
                        "FOREIGN KEY(parent_id) REFERENCES batch_commit_parent(id) DEFERRABLE "
                        "INITIALLY DEFERRED)")
                .has_value());
        REQUIRE(fix.execute("CREATE TRIGGER fail_batch_commit AFTER UPDATE ON documents WHEN "
                            "NEW.repair_status = 'completed' BEGIN INSERT INTO batch_commit_guard "
                            "VALUES (NEW.id, 999999); END")
                    .has_value());
    }

    auto result = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE_FALSE(result.has_value());
    requireRolledBack(fix, {firstId, secondId}, extractedBefore, indexedBefore);
}

TEST_CASE("batch content precheck and FTS preparation failures close the transaction",
          "[unit][metadata-repo][batch-content][transaction][rollback][precheck][fts]") {
    ContentTransactionFixture fix;
    auto inserted = fix.repository_->insertDocument(
        makePendingDocument("/tmp/precheck-fts-failure.txt", "precheck-fts-failure-hash"));
    REQUIRE(inserted.has_value());
    fix.repository_->initializeCounters();
    const auto extractedBefore = fix.repository_->getCachedExtractedCount();
    const auto indexedBefore = fix.repository_->getCachedIndexedCount();
    REQUIRE(fix.repository_->getAllFts5IndexedDocumentIds().value().empty());

    const auto entry = makeContentEntry(inserted.value(), "Recovery title", "Recovery content",
                                        "Recovery metadata");

    SECTION("precheck statement preparation") {
        REQUIRE(fix.execute("ALTER TABLE documents RENAME TO documents_hidden").has_value());
        auto result = fix.repository_->batchInsertContentAndIndex({entry});
        REQUIRE_FALSE(result.has_value());
        REQUIRE(fix.execute("ALTER TABLE documents_hidden RENAME TO documents").has_value());
    }

    SECTION("FTS statement preparation") {
        REQUIRE(fix.execute("DROP TABLE documents_fts").has_value());
        auto result = fix.repository_->batchInsertContentAndIndex({entry});
        REQUIRE_FALSE(result.has_value());
        REQUIRE(fix.execute(R"(
            CREATE VIRTUAL TABLE documents_fts USING fts5(
                content,
                title,
                content_type,
                tokenize='unicode61 tokenchars ''_-'''
            )
        )")
                    .has_value());
    }

    requireRolledBack(fix, {inserted.value()}, extractedBefore, indexedBefore);

    // A successful write on the same pool proves the failure path left no open transaction.
    REQUIRE(fix.repository_->batchInsertContentAndIndex({entry}).has_value());
    auto state = readObservableState(fix, inserted.value());
    REQUIRE(state.has_value());
    CHECK((state.value().content == "Recovery content"));
    CHECK((state.value().metadataTitle == "Recovery metadata"));
    CHECK((state.value().repairAttempts == 1));
}

TEST_CASE("batch content derives FTS state from the transaction instead of a stale cache",
          "[unit][metadata-repo][batch-content][transaction][fts-cache]") {
    ContentTransactionFixture fix;
    auto inserted = fix.repository_->insertDocument(
        makePendingDocument("/tmp/stale-fts-cache.txt", "stale-fts-cache-hash"));
    REQUIRE(inserted.has_value());
    fix.repository_->initializeCounters();
    const auto indexedBefore = fix.repository_->getCachedIndexedCount();
    const auto extractedBefore = fix.repository_->getCachedExtractedCount();
    REQUIRE(fix.repository_->getAllFts5IndexedDocumentIds().value().empty());

    // Model the narrow concurrent-writer window after COMMIT but before post-commit cache
    // publication: the database is authoritative while the warmed FTS-ID cache is stale-negative.
    REQUIRE(fix.execute("INSERT INTO documents_fts(rowid, content, title) VALUES (" +
                        std::to_string(inserted.value()) +
                        ", 'committed content', 'committed title')")
                .has_value());
    REQUIRE(fix.execute("UPDATE documents SET content_extracted = 1, "
                        "extraction_status = 'Success', repair_status = 'completed' WHERE id = " +
                        std::to_string(inserted.value()))
                .has_value());

    auto result = fix.repository_->batchInsertContentAndIndex(
        {makeContentEntry(inserted.value(), "Replacement title", "Replacement content")});
    REQUIRE(result.has_value());
    CHECK((fix.repository_->getCachedIndexedCount() == indexedBefore));
    CHECK((fix.repository_->getCachedExtractedCount() == extractedBefore));

    auto fts = getFtsSurface(*fix.pool_, inserted.value());
    REQUIRE(fts.has_value());
    REQUIRE(fts.value().has_value());
    CHECK((fts.value()->title == "Replacement title"));
}

TEST_CASE("batch content status chunk failure rolls back earlier chunks",
          "[unit][metadata-repo][batch-content][transaction][rollback][chunk]") {
    ContentTransactionFixture fix;
    constexpr int kDocumentCount = 401;
    std::vector<BatchDocumentInsert> records;
    records.reserve(kDocumentCount);
    for (int i = 0; i < kDocumentCount; ++i) {
        BatchDocumentInsert record;
        record.info = makePendingDocument("/tmp/chunk-" + std::to_string(i) + ".txt",
                                          "chunk-hash-" + std::to_string(i));
        records.push_back(std::move(record));
    }
    auto inserted = fix.repository_->batchInsertDocumentsWithMetadata(records);
    REQUIRE(inserted.has_value());
    REQUIRE((inserted.value().size() == kDocumentCount));

    std::vector<int64_t> documentIds;
    std::vector<BatchContentEntry> entries;
    documentIds.reserve(kDocumentCount);
    entries.reserve(kDocumentCount);
    for (int i = 0; i < kDocumentCount; ++i) {
        const auto id = inserted.value()[static_cast<std::size_t>(i)].documentId;
        documentIds.push_back(id);
        entries.push_back(makeContentEntry(id, "Chunk title " + std::to_string(i),
                                           "Chunk content " + std::to_string(i)));
    }

    fix.repository_->initializeCounters();
    const auto extractedBefore = fix.repository_->getCachedExtractedCount();
    const auto indexedBefore = fix.repository_->getCachedIndexedCount();
    REQUIRE(fix.repository_->getAllFts5IndexedDocumentIds().value().empty());

    REQUIRE(fix.execute("CREATE TRIGGER fail_second_status_chunk BEFORE UPDATE ON documents "
                        "WHEN NEW.id = " +
                        std::to_string(documentIds.back()) +
                        " AND NEW.repair_status = 'completed' BEGIN SELECT RAISE(ABORT, "
                        "'second status chunk'); END")
                .has_value());

    auto result = fix.repository_->batchInsertContentAndIndex(entries);
    REQUIRE_FALSE(result.has_value());
    requireRolledBack(fix, {documentIds.front(), documentIds[399], documentIds.back()},
                      extractedBefore, indexedBefore);
}

TEST_CASE("batch content successful status chunks match singleton execution",
          "[unit][metadata-repo][batch-content][transaction][equivalence][chunk]") {
    ContentTransactionFixture fix;
    constexpr std::size_t kDocumentCount = 401;
    constexpr std::uint64_t kExpectedCommittedDocuments = 802;
    std::vector<BatchDocumentInsert> records;
    records.reserve(kDocumentCount * 2U);
    for (std::size_t i = 0; i < kDocumentCount; ++i) {
        BatchDocumentInsert chunked;
        chunked.info = makePendingDocument("/tmp/success-chunk-" + std::to_string(i) + ".txt",
                                           "success-chunk-hash-" + std::to_string(i));
        records.push_back(std::move(chunked));
        BatchDocumentInsert singleton;
        singleton.info = makePendingDocument("/tmp/success-singleton-" + std::to_string(i) + ".txt",
                                             "success-singleton-hash-" + std::to_string(i));
        records.push_back(std::move(singleton));
    }
    auto inserted = fix.repository_->batchInsertDocumentsWithMetadata(records);
    REQUIRE(inserted.has_value());
    REQUIRE((inserted.value().size() == kDocumentCount * 2U));
    fix.repository_->initializeCounters();

    std::vector<int64_t> chunkedIds;
    std::vector<int64_t> singletonIds;
    std::vector<BatchContentEntry> chunkedEntries;
    std::vector<BatchContentEntry> singletonEntries;
    chunkedIds.reserve(kDocumentCount);
    singletonIds.reserve(kDocumentCount);
    chunkedEntries.reserve(kDocumentCount);
    singletonEntries.reserve(kDocumentCount);
    for (std::size_t i = 0; i < kDocumentCount; ++i) {
        const auto chunkedId = inserted.value()[i * 2U].documentId;
        const auto singletonId = inserted.value()[i * 2U + 1U].documentId;
        chunkedIds.push_back(chunkedId);
        singletonIds.push_back(singletonId);
        const auto title = "Success title " + std::to_string(i);
        const auto body = "Success body " + std::to_string(i);
        const auto metadataTitle = "Success metadata " + std::to_string(i);
        chunkedEntries.push_back(makeContentEntry(chunkedId, title, body, metadataTitle));
        singletonEntries.push_back(makeContentEntry(singletonId, title, body, metadataTitle));
    }

    REQUIRE(fix.repository_->batchInsertContentAndIndex(chunkedEntries).has_value());
    for (const auto& entry : singletonEntries) {
        REQUIRE(fix.repository_->batchInsertContentAndIndex({entry}).has_value());
    }

    for (std::size_t index = 0; index < kDocumentCount; ++index) {
        auto chunked = readObservableState(fix, chunkedIds[index]);
        auto singleton = readObservableState(fix, singletonIds[index]);
        REQUIRE(chunked.has_value());
        REQUIRE(singleton.has_value());
        CHECK((chunked.value() == singleton.value()));
    }
    CHECK((fix.repository_->getCachedExtractedCount() == kExpectedCommittedDocuments));
    CHECK((fix.repository_->getCachedIndexedCount() == kExpectedCommittedDocuments));
}

TEST_CASE("batch content duplicate row and singleton execution are observably equivalent",
          "[unit][metadata-repo][batch-content][transaction][equivalence]") {
    ContentTransactionFixture fix;
    std::vector<int64_t> batchedIds;
    std::vector<int64_t> singletonIds;
    for (int i = 0; i < 2; ++i) {
        auto batched = fix.repository_->insertDocument(
            makePendingDocument("/tmp/equivalence-batch-" + std::to_string(i) + ".txt",
                                "equivalence-batch-hash-" + std::to_string(i)));
        auto singleton = fix.repository_->insertDocument(
            makePendingDocument("/tmp/equivalence-singleton-" + std::to_string(i) + ".txt",
                                "equivalence-singleton-hash-" + std::to_string(i)));
        REQUIRE(batched.has_value());
        REQUIRE(singleton.has_value());
        batchedIds.push_back(batched.value());
        singletonIds.push_back(singleton.value());
    }
    fix.repository_->initializeCounters();

    std::vector<BatchContentEntry> batched{
        makeContentEntry(batchedIds[0], "First title", "First body", "First metadata"),
        makeContentEntry(batchedIds[1], "Peer title", "Peer body", "Peer metadata"),
        makeContentEntry(batchedIds[0], "Final title", "Final body", "Final metadata")};
    REQUIRE(fix.repository_->batchInsertContentAndIndex(batched).has_value());

    std::vector<BatchContentEntry> singleton{
        makeContentEntry(singletonIds[0], "First title", "First body", "First metadata"),
        makeContentEntry(singletonIds[1], "Peer title", "Peer body", "Peer metadata"),
        makeContentEntry(singletonIds[0], "Final title", "Final body", "Final metadata")};
    for (const auto& entry : singleton) {
        REQUIRE(fix.repository_->batchInsertContentAndIndex({entry}).has_value());
    }

    auto batchedFirst = readObservableState(fix, batchedIds[0]);
    auto singletonFirst = readObservableState(fix, singletonIds[0]);
    auto batchedPeer = readObservableState(fix, batchedIds[1]);
    auto singletonPeer = readObservableState(fix, singletonIds[1]);
    REQUIRE(batchedFirst.has_value());
    REQUIRE(singletonFirst.has_value());
    REQUIRE(batchedPeer.has_value());
    REQUIRE(singletonPeer.has_value());
    CHECK((batchedFirst.value() == singletonFirst.value()));
    CHECK((batchedPeer.value() == singletonPeer.value()));
    CHECK((fix.repository_->getCachedExtractedCount() == 4));
    CHECK((fix.repository_->getCachedIndexedCount() == 4));
}

TEST_CASE("batch content retries transient busy without duplicating logical effects",
          "[unit][metadata-repo][batch-content][transaction][contention]") {
    const auto dbPath =
        yams::test::migrated_metadata_db_template().clone("metadata_content_busy_retry_");
    ConnectionPoolConfig config;
    config.minConnections = 1;
    config.maxConnections = 1;
    config.busyTimeout = std::chrono::milliseconds(10);
    ConnectionPool repositoryPool(dbPath.string(), config);
    ConnectionPool lockerPool(dbPath.string(), config);
    REQUIRE(repositoryPool.initialize().has_value());
    REQUIRE(lockerPool.initialize().has_value());
    auto repository = std::make_unique<MetadataRepository>(
        repositoryPool, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);

    auto inserted =
        repository->insertDocument(makePendingDocument("/tmp/busy-retry.txt", "busy-retry-hash"));
    auto exhausted = repository->insertDocument(
        makePendingDocument("/tmp/busy-exhausted.txt", "busy-exhausted-hash"));
    REQUIRE(inserted.has_value());
    REQUIRE(exhausted.has_value());
    repository->initializeCounters();
    SpdlogCaptureGuard capturedLogs(spdlog::level::debug);

    std::promise<Result<void>> lockHeld;
    auto lockHeldFuture = lockHeld.get_future();
    std::promise<void> releaseLock;
    auto releaseFuture = releaseLock.get_future();
    std::promise<Result<void>> lockerFinished;
    auto lockerFinishedFuture = lockerFinished.get_future();
    yams::compat::jthread locker([&] {
        auto connection = lockerPool.acquire();
        if (!connection) {
            lockHeld.set_value(connection.error());
            lockerFinished.set_value(connection.error());
            return;
        }
        auto begin = (*connection.value())->execute("BEGIN IMMEDIATE");
        lockHeld.set_value(begin);
        if (!begin) {
            lockerFinished.set_value(begin.error());
            return;
        }
        releaseFuture.wait();
        lockerFinished.set_value((*connection.value())->execute("ROLLBACK"));
    });
    REQUIRE(lockHeldFuture.get().has_value());

    yams::compat::jthread releaser([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        releaseLock.set_value();
    });

    auto result = repository->batchInsertContentAndIndex(
        {makeContentEntry(inserted.value(), "Busy title", "Busy content", "Busy metadata")});
    REQUIRE(result.has_value());
    locker.join();
    releaser.join();
    REQUIRE(lockerFinishedFuture.get().has_value());
    CHECK((capturedLogs.str().find("succeeded after") != std::string::npos));

    auto state = repository->getDocument(inserted.value());
    REQUIRE(state.has_value());
    REQUIRE(state.value().has_value());
    CHECK((state.value()->repairAttempts == 1));
    CHECK((repository->getCachedExtractedCount() == 1));
    CHECK((repository->getCachedIndexedCount() == 1));
    auto content = repository->getContent(inserted.value());
    REQUIRE(content.has_value());
    REQUIRE(content.value().has_value());
    CHECK((content.value()->contentText == "Busy content"));

    {
        auto heldConnection = lockerPool.acquire();
        REQUIRE(heldConnection.has_value());
        REQUIRE((*heldConnection.value())->execute("BEGIN IMMEDIATE").has_value());
        auto exhaustedResult = repository->batchInsertContentAndIndex({makeContentEntry(
            exhausted.value(), "Exhausted title", "Exhausted content", "Exhausted metadata")});
        REQUIRE_FALSE(exhaustedResult.has_value());
        REQUIRE((*heldConnection.value())->execute("ROLLBACK").has_value());
    }

    auto exhaustedContent = repository->getContent(exhausted.value());
    REQUIRE(exhaustedContent.has_value());
    CHECK_FALSE(exhaustedContent.value().has_value());
    auto exhaustedState = repository->getDocument(exhausted.value());
    REQUIRE(exhaustedState.has_value());
    REQUIRE(exhaustedState.value().has_value());
    CHECK((exhaustedState.value()->repairAttempts == 0));
    CHECK_FALSE(exhaustedState.value()->contentExtracted);
    CHECK((repository->getCachedExtractedCount() == 1));
    CHECK((repository->getCachedIndexedCount() == 1));
    auto indexedIds = repository->getAllFts5IndexedDocumentIds();
    REQUIRE(indexedIds.has_value());
    CHECK((std::find(indexedIds.value().begin(), indexedIds.value().end(), exhausted.value()) ==
           indexedIds.value().end()));

    repository.reset();
    repositoryPool.shutdown();
    lockerPool.shutdown();
    yams::test::remove_sqlite_artifacts(dbPath);
}
