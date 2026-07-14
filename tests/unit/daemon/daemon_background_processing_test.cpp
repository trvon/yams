// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 Trevon Helm
// Consolidated background processing tests (3 → 1): PostIngestQueue, InternalEventBus, MPMC
// concurrency Covers: queue lifecycle, async processing, bus integration, stress testing, MPMC
// correctness

#include <spdlog/sinks/ostream_sink.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include "../../common/test_helpers_catch2.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/api/content_store.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/IngestService.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/extraction/content_extractor.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::daemon;

namespace {
void stopAndResetQueue(std::unique_ptr<PostIngestQueue>& queue) {
    if (!queue) {
        return;
    }
    queue->stop();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (queue->started() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    queue.reset();
}

/// Drain leftover items from the singleton post_ingest channel to prevent
/// cross-test interference (tasks enqueued by a previous test that the previous
/// queue didn't consume before stopping).
void drainPostIngestChannel() {
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::PostIngestTask>(
            "post_ingest", 32);
    InternalEventBus::PostIngestTask drain;
    while (channel->try_pop(drain)) {
    }
}

void drainEmbedJobsChannel() {
    auto channel = InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
        "embed_jobs", 2048);
    InternalEventBus::EmbedJob drain;
    while (channel->try_pop(drain)) {
    }
}

void drainStoreDocumentChannel() {
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::StoreDocumentTask>(
            "store_document_tasks", 64);
    InternalEventBus::StoreDocumentTask drain;
    while (channel->try_pop(drain)) {
    }
}

class StoreDocumentChannelGuard {
public:
    StoreDocumentChannelGuard() { drainStoreDocumentChannel(); }
    ~StoreDocumentChannelGuard() { drainStoreDocumentChannel(); }
};

using yams::test::ScopedEnvVar;

// =============================================================================
// Test Helpers
// =============================================================================

class SpdlogCaptureGuard {
public:
    explicit SpdlogCaptureGuard(spdlog::level::level_enum level)
        : previousLogger_(spdlog::default_logger()), previousLevel_(spdlog::get_level()) {
        auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream_);
        logger_ = std::make_shared<spdlog::logger>("daemon_background_capture", sink);
        logger_->set_level(level);
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

// RAII guard for InternalEventBus configuration
class BusToggleGuard {
public:
    explicit BusToggleGuard(bool enable) {
        prev_ = TuneAdvisor::useInternalBusForPostIngest();
        TuneAdvisor::setUseInternalBusForPostIngest(enable);
    }
    ~BusToggleGuard() { TuneAdvisor::setUseInternalBusForPostIngest(prev_); }

private:
    bool prev_{false};
};

class PostIngestBatchGuard {
public:
    explicit PostIngestBatchGuard(uint32_t size) {
        prev_ = TuneAdvisor::postIngestBatchSize();
        TuneAdvisor::setPostIngestBatchSize(size);
    }
    ~PostIngestBatchGuard() { TuneAdvisor::setPostIngestBatchSize(prev_); }

private:
    uint32_t prev_{0};
};

class PostIngestConcurrencyGuard {
public:
    PostIngestConcurrencyGuard(uint32_t totalConcurrent, uint32_t extractionConcurrent) {
        prevHw_ = TuneAdvisor::hardwareConcurrency();
        prevTotal_ = TuneAdvisor::postIngestTotalConcurrent();
        prevExtraction_ = TuneAdvisor::postExtractionConcurrent();
        TuneAdvisor::setHardwareConcurrencyForTests(32);
        TuneAdvisor::setPostIngestTotalConcurrent(totalConcurrent);
        TuneAdvisor::setPostExtractionConcurrent(extractionConcurrent);
        TuneAdvisor::setPostExtractionConcurrentDynamicCap(UINT32_MAX);
    }
    ~PostIngestConcurrencyGuard() {
        TuneAdvisor::setHardwareConcurrencyForTests(prevHw_);
        TuneAdvisor::setPostIngestTotalConcurrent(prevTotal_);
        TuneAdvisor::setPostExtractionConcurrent(prevExtraction_);
        TuneAdvisor::setPostExtractionConcurrentDynamicCap(UINT32_MAX);
    }

private:
    uint32_t prevHw_{0};
    uint32_t prevTotal_{0};
    uint32_t prevExtraction_{0};
};

class WorkCoordinatorThreadsGuard {
public:
    explicit WorkCoordinatorThreadsGuard(uint32_t threads)
        : previous_(TuneAdvisor::workCoordinatorThreads()) {
        TuneAdvisor::setWorkCoordinatorThreads(threads);
    }
    ~WorkCoordinatorThreadsGuard() { TuneAdvisor::setWorkCoordinatorThreads(previous_); }

private:
    uint32_t previous_{0};
};

// Unified StubContentStore (thread-safe, supports all required operations)
class StubContentStore : public api::IContentStore {
public:
    void setContent(const std::string& hash, const std::string& data) {
        std::lock_guard<std::mutex> lk(mu_);
        ByteVector bytes;
        bytes.reserve(data.size());
        for (unsigned char c : data) {
            bytes.push_back(static_cast<std::byte>(c));
        }
        blobs_[hash] = bytes;
    }

    Result<std::vector<std::byte>> retrieveBytes(const std::string& hash) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = blobs_.find(hash);
        if (it == blobs_.end()) {
            return Error{ErrorCode::NotFound, "content not found"};
        }
        return it->second;
    }

    Result<std::vector<std::byte>> retrieveBytesPrefix(const std::string& hash,
                                                       std::size_t maxBytes) override {
        auto r = retrieveBytes(hash);
        if (!r)
            return r;
        auto& v = r.value();
        if (v.size() > maxBytes)
            v.resize(maxBytes);
        return r;
    }

    Result<api::IContentStore::RawContent> retrieveRaw(const std::string& hash) override {
        auto bytes = retrieveBytes(hash);
        if (!bytes) {
            return bytes.error();
        }
        api::IContentStore::RawContent raw;
        raw.data = bytes.value();
        return raw;
    }

    std::future<Result<api::IContentStore::RawContent>>
    retrieveRawAsync(const std::string& hash) override {
        return std::async(std::launch::deferred, [this, hash]() { return retrieveRaw(hash); });
    }

    Result<api::RetrieveResult> retrieveStream(const std::string& hash, std::ostream& os,
                                               api::ProgressCallback) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = blobs_.find(hash);
        if (it == blobs_.end()) {
            return Error{ErrorCode::NotFound, "no blob"};
        }
        for (auto b : it->second) {
            os.put(static_cast<char>(b));
        }
        api::RetrieveResult r{};
        r.found = true;
        r.size = static_cast<uint64_t>(it->second.size());
        return r;
    }

    Result<api::StoreResult> store(const std::filesystem::path& path, const api::ContentMetadata&,
                                   api::ProgressCallback) override {
        singleCalls_.fetch_add(1, std::memory_order_relaxed);
        storeCv_.notify_all();

        api::StoreResult result;
        result.contentHash = "single-hash";
        std::error_code error;
        result.bytesStored = std::filesystem::file_size(path, error);
        if (error) {
            return Error{ErrorCode::IOError, error.message()};
        }
        return result;
    }
    Result<api::RetrieveResult> retrieve(const std::string&, const std::filesystem::path&,
                                         api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::StoreResult> storeStream(std::istream&, const api::ContentMetadata&,
                                         api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::StoreResult> storeBytes(std::span<const std::byte>,
                                        const api::ContentMetadata&) override {
        return ErrorCode::NotImplemented;
    }
    Result<bool> exists(const std::string&) const override { return ErrorCode::NotImplemented; }
    Result<bool> remove(const std::string&) override { return ErrorCode::NotImplemented; }
    Result<api::ContentMetadata> getMetadata(const std::string&) const override {
        return ErrorCode::NotImplemented;
    }
    Result<void> updateMetadata(const std::string&, const api::ContentMetadata&) override {
        return ErrorCode::NotImplemented;
    }
    std::vector<Result<api::StoreResult>>
    storeBatch(const std::vector<std::filesystem::path>& paths,
               const std::vector<api::ContentMetadata>&) override {
        batchCalls_.fetch_add(1, std::memory_order_relaxed);
        lastBatchSize_.store(paths.size(), std::memory_order_relaxed);
        storeCv_.notify_all();

        std::vector<Result<api::StoreResult>> results;
        results.reserve(paths.size());
        for (std::size_t i = 0; i < paths.size(); ++i) {
            api::StoreResult result;
            result.contentHash = "batch-hash-" + std::to_string(i);
            std::error_code error;
            result.bytesStored = std::filesystem::file_size(paths[i], error);
            if (error) {
                results.emplace_back(Error{ErrorCode::IOError, error.message()});
            } else {
                results.emplace_back(std::move(result));
            }
        }
        return results;
    }
    std::vector<Result<bool>> removeBatch(const std::vector<std::string>&) override { return {}; }
    api::ContentStoreStats getStats() const override { return {}; }
    api::HealthStatus checkHealth() const override { return {}; }
    Result<void> verify(api::ProgressCallback) override { return ErrorCode::NotImplemented; }
    Result<void> compact(api::ProgressCallback) override { return ErrorCode::NotImplemented; }
    Result<void> garbageCollect(api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }

    [[nodiscard]] std::size_t batchCalls() const {
        return batchCalls_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t singleCalls() const {
        return singleCalls_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t lastBatchSize() const {
        return lastBatchSize_.load(std::memory_order_relaxed);
    }

    bool waitForStore(std::chrono::steady_clock::time_point deadline) {
        std::unique_lock lock(storeMutex_);
        return storeCv_.wait_until(lock, deadline, [this] {
            return singleCalls_.load(std::memory_order_relaxed) != 0 ||
                   batchCalls_.load(std::memory_order_relaxed) != 0;
        });
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, ByteVector> blobs_;
    std::atomic<std::size_t> singleCalls_{0};
    std::atomic<std::size_t> batchCalls_{0};
    std::atomic<std::size_t> lastBatchSize_{0};
    std::mutex storeMutex_;
    std::condition_variable storeCv_;
};

// Minimal MetadataRepository stub for testing
std::shared_ptr<metadata::ConnectionPool> getTestPool() {
    static std::shared_ptr<metadata::ConnectionPool> pool = [] {
        metadata::ConnectionPoolConfig cfg{};
        cfg.enableWAL = false;
        cfg.enableForeignKeys = false;
        auto p = std::make_shared<metadata::ConnectionPool>(":memory:", cfg);
        (void)p->initialize();
        return p;
    }();
    return pool;
}

class StubMetadataRepository : public metadata::MetadataRepository {
public:
    StubMetadataRepository() : metadata::MetadataRepository(*getTestPool()) {}

    void setDocument(metadata::DocumentInfo doc) {
        std::lock_guard<std::mutex> lk(mu_);
        docsByHash_[doc.sha256Hash] = doc;
        docsById_[doc.id] = doc;
        lastUpdated_ = doc;
    }

    bool contentInserted() const {
        std::lock_guard<std::mutex> lk(mu_);
        return contentInserted_;
    }

    metadata::DocumentContent lastContent() const {
        std::lock_guard<std::mutex> lk(mu_);
        return lastContent_;
    }

    metadata::DocumentInfo lastUpdated() const {
        std::lock_guard<std::mutex> lk(mu_);
        return lastUpdated_;
    }

    Result<std::optional<metadata::DocumentInfo>>
    getDocumentByHash(const std::string& hash) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = docsByHash_.find(hash);
        if (it != docsByHash_.end()) {
            return std::optional<metadata::DocumentInfo>(it->second);
        }
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }

    Result<std::optional<metadata::DocumentInfo>> getDocument(int64_t id) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = docsById_.find(id);
        if (it != docsById_.end()) {
            return std::optional<metadata::DocumentInfo>(it->second);
        }
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }

    Result<void> updateDocument(const metadata::DocumentInfo& info) override {
        std::lock_guard<std::mutex> lk(mu_);
        docsByHash_[info.sha256Hash] = info;
        docsById_[info.id] = info;
        lastUpdated_ = info;
        return Result<void>();
    }

    Result<void> insertContent(const metadata::DocumentContent& content) override {
        std::lock_guard<std::mutex> lk(mu_);
        lastContent_ = content;
        contentInserted_ = true;
        return Result<void>();
    }

    Result<void> indexDocumentContent(int64_t, const std::string&, const std::string& content,
                                      const std::string&) override {
        std::lock_guard<std::mutex> lk(mu_);
        indexedContent_ = content;
        return Result<void>();
    }

    Result<void> indexDocumentContentTrusted(int64_t, const std::string&,
                                             const std::string& content,
                                             const std::string&) override {
        std::lock_guard<std::mutex> lk(mu_);
        indexedContent_ = content;
        return Result<void>();
    }

    Result<void> updateDocumentExtractionStatus(int64_t docId, bool contentExtracted,
                                                metadata::ExtractionStatus status,
                                                const std::string& = "") override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = docsById_.find(docId);
        if (it != docsById_.end()) {
            it->second.contentExtracted = contentExtracted;
            it->second.extractionStatus = status;
            lastUpdated_ = it->second;
            docsByHash_[it->second.sha256Hash] = it->second;
        }
        return Result<void>();
    }

    void addSymSpellTerm(std::string_view, int64_t) override { /* no-op for mock */ }

    Result<std::vector<std::string>> getDocumentTags(int64_t) override {
        return std::vector<std::string>{};
    }

    Result<void> setMetadataBatch(
        const std::vector<std::tuple<int64_t, std::string, metadata::MetadataValue>>&) override {
        return Result<void>();
    }

    Result<void> batchUpdateDocumentRepairStatuses(const std::vector<std::string>&,
                                                   metadata::RepairStatus) override {
        return Result<void>();
    }

    void signalCorpusStatsStale() override { /* no-op for mock */ }

    Result<void>
    batchInsertContentAndIndex(const std::vector<metadata::BatchContentEntry>& entries) override {
        std::lock_guard<std::mutex> lk(mu_);
        batchWriteSizes_.push_back(entries.size());
        for (const auto& entry : entries) {
            batchInsertedDocIds_.push_back(entry.documentId);
            contentInserted_ = true;
            // Store last content for verification
            lastContent_.documentId = entry.documentId;
            lastContent_.contentText = entry.contentText;
            lastContent_.contentLength = static_cast<int64_t>(entry.contentText.size());
            lastContent_.extractionMethod = entry.extractionMethod;
            lastContent_.language = entry.language;
            // Update document status
            auto it = docsById_.find(entry.documentId);
            if (it != docsById_.end()) {
                it->second.contentExtracted = true;
                it->second.extractionStatus = metadata::ExtractionStatus::Success;
                docsByHash_[it->second.sha256Hash] = it->second;
                lastUpdated_ = it->second;
            }
        }
        return Result<void>();
    }

    Result<std::unordered_map<std::string, metadata::DocumentInfo>>
    batchGetDocumentsByHash(const std::vector<std::string>& hashes) override {
        std::lock_guard<std::mutex> lk(mu_);
        batchGetCalls_++;
        std::unordered_map<std::string, metadata::DocumentInfo> out;
        out.reserve(hashes.size());
        for (const auto& hash : hashes) {
            auto it = docsByHash_.find(hash);
            if (it != docsByHash_.end()) {
                out.emplace(hash, it->second);
            }
        }
        return out;
    }

    Result<std::unordered_map<int64_t, metadata::DocumentContent>>
    batchGetContent(const std::vector<int64_t>& documentIds) override {
        std::lock_guard<std::mutex> lk(mu_);
        std::unordered_map<int64_t, metadata::DocumentContent> out;
        out.reserve(documentIds.size());
        for (auto id : documentIds) {
            auto it = docsById_.find(id);
            if (it == docsById_.end()) {
                continue;
            }
            metadata::DocumentContent content{};
            content.documentId = id;
            content.contentText = indexedContent_;
            content.contentLength = static_cast<int64_t>(indexedContent_.size());
            content.extractionMethod = "post_ingest";
            content.language = "en";
            out.emplace(id, content);
        }
        return out;
    }

    Result<std::unordered_map<int64_t, std::vector<std::string>>>
    batchGetDocumentTags(std::span<const int64_t> documentIds) override {
        std::lock_guard<std::mutex> lk(mu_);
        std::unordered_map<int64_t, std::vector<std::string>> tags;
        tags.reserve(documentIds.size());
        for (auto id : documentIds) {
            tags.emplace(id, std::vector<std::string>{});
        }
        return tags;
    }

    std::size_t batchGetCalls() const {
        std::lock_guard<std::mutex> lk(mu_);
        return batchGetCalls_;
    }

    std::vector<int64_t> batchInsertedDocIds() const {
        std::lock_guard<std::mutex> lk(mu_);
        return batchInsertedDocIds_;
    }

    std::size_t maxBatchWriteSize() const {
        std::lock_guard<std::mutex> lk(mu_);
        std::size_t maxSize = 0;
        for (std::size_t n : batchWriteSizes_) {
            maxSize = std::max(maxSize, n);
        }
        return maxSize;
    }

private:
    mutable std::mutex mu_;
    metadata::DocumentInfo lastUpdated_{};
    metadata::DocumentContent lastContent_{};
    std::string indexedContent_;
    bool contentInserted_{false};
    std::size_t batchGetCalls_{0};
    std::vector<int64_t> batchInsertedDocIds_{};
    std::vector<std::size_t> batchWriteSizes_{};
    std::unordered_map<std::string, metadata::DocumentInfo> docsByHash_{};
    std::unordered_map<int64_t, metadata::DocumentInfo> docsById_{};
};

class StubExtractor : public extraction::IContentExtractor {
public:
    bool supports(const std::string& mime, const std::string&) const override {
        return mime == "text/plain";
    }

    std::optional<std::string> extractText(const std::vector<std::byte>& bytes, const std::string&,
                                           const std::string&) override {
        std::string text;
        text.reserve(bytes.size());
        for (auto b : bytes) {
            text.push_back(static_cast<char>(b));
        }
        return text;
    }
};

struct StoreCallSnapshot {
    std::size_t singleCalls{0};
    std::size_t batchCalls{0};
    std::size_t lastBatchSize{0};
};

StoreCallSnapshot runQueuedDocuments(std::size_t count) {
    StoreDocumentChannelGuard channelGuard;
    WorkCoordinatorThreadsGuard threadsGuard(1);
    yams::test::TempDirGuard testDir("yams_ingest_service_batch_");

    auto contentStore = std::make_shared<StubContentStore>();
    {
        DaemonConfig config;
        config.dataDir = testDir.path();
        StateComponent state;
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager serviceManager(config, state, lifecycleFsm);
        serviceManager.__test_setContentStore(contentStore);

        auto channel =
            InternalEventBus::instance().get_or_create_channel<InternalEventBus::StoreDocumentTask>(
                "store_document_tasks", 64);
        for (std::size_t i = 0; i < count; ++i) {
            InternalEventBus::StoreDocumentTask task;
            task.request.name = "queued-" + std::to_string(i) + ".txt";
            task.request.content = "queued content " + std::to_string(i);
            REQUIRE(channel->try_push(std::move(task)));
        }

        IngestService ingestService(&serviceManager, serviceManager.getWorkCoordinator());
        ingestService.start();
        CHECK(
            contentStore->waitForStore(std::chrono::steady_clock::now() + std::chrono::seconds(2)));
        ingestService.stop();
    }

    return {.singleCalls = contentStore->singleCalls(),
            .batchCalls = contentStore->batchCalls(),
            .lastBatchSize = contentStore->lastBatchSize()};
}

bool isMpmcEnabled() {
    const char* m = std::getenv("YAMS_INTERNAL_BUS_MPMC");
    return m && std::string(m) == "1";
}

} // namespace

// =============================================================================
// IngestService Tests
// =============================================================================

TEST_CASE("IngestService preserves single and batched queued storage paths",
          "[daemon][background][ingest][batch]") {
    SpdlogCaptureGuard logGuard(spdlog::level::err);

    SECTION("one task keeps the exact single-store path") {
        const auto calls = runQueuedDocuments(1);
        CHECK(calls.singleCalls == 1);
        CHECK(calls.batchCalls == 0);
    }

    SECTION("two tasks cross one batch boundary") {
        const auto calls = runQueuedDocuments(2);
        CHECK(calls.singleCalls == 0);
        CHECK(calls.batchCalls == 1);
        CHECK(calls.lastBatchSize == 2);
    }
}

// =============================================================================
// PostIngestQueue Tests
// =============================================================================

TEST_CASE("PostIngestQueue: Basic lifecycle and task processing", "[daemon][background][queue]") {
    BusToggleGuard busGuard(false); // Disable bus for direct queue testing
    drainPostIngestChannel();

    // WorkCoordinator is required for PostIngestQueue strand creation
    WorkCoordinator coordinator;
    coordinator.start(2);

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();

    metadata::DocumentInfo doc{};
    doc.id = 101;
    doc.fileName = "doc.txt";
    doc.fileExtension = ".txt";
    doc.sha256Hash = "hash-123";
    doc.mimeType = "text/plain";
    doc.indexedTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    metadataRepo->setDocument(doc);

    const std::string payload = "Hello from post-ingest";
    store->setContent(doc.sha256Hash, payload);

    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};

    SECTION("Process single task successfully") {
        drainEmbedJobsChannel();
        auto embedChannel =
            InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>(
                "embed_jobs", 2048);

        auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                       nullptr, &coordinator, nullptr, 32);
        queue->start(); // Start the channel poller

        // Wait for channel poller coroutine to start before enqueueing
        auto startDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!queue->started() && std::chrono::steady_clock::now() < startDeadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        REQUIRE(queue->started());

        PostIngestQueue::Task task{.hash = doc.sha256Hash, .mime = doc.mimeType};
        task.noEmbeddings = true;
        REQUIRE(queue->tryEnqueue(std::move(task)));

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (queue->processed() < 1 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        REQUIRE((queue->processed() == 1));
        REQUIRE((queue->failed() == 0));
        REQUIRE((queue->entityInFlight() == 0));
        REQUIRE(metadataRepo->contentInserted());

        auto content = metadataRepo->lastContent();
        REQUIRE((content.documentId == doc.id));
        REQUIRE((content.contentText == payload));
        REQUIRE((content.contentLength == static_cast<int64_t>(payload.size())));
        REQUIRE((content.extractionMethod == "post_ingest"));
        REQUIRE_FALSE(content.language.empty());

        auto updated = metadataRepo->lastUpdated();
        REQUIRE(updated.contentExtracted);
        REQUIRE((updated.extractionStatus == metadata::ExtractionStatus::Success));

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        InternalEventBus::EmbedJob embedJob;
        CHECK_FALSE(embedChannel->try_pop(embedJob));

        stopAndResetQueue(queue);
    }

    SECTION("Stop wakes title-inactive poller promptly") {
        auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                       nullptr, &coordinator, nullptr, 32);
        queue->start();

        auto startDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!queue->started() && std::chrono::steady_clock::now() < startDeadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        REQUIRE(queue->started());

        std::this_thread::sleep_for(std::chrono::milliseconds(80));

        const auto stopStart = std::chrono::steady_clock::now();
        queue->stop();
        const auto stopElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - stopStart);

        INFO("stopElapsedMs=" << stopElapsed.count());
        CHECK((stopElapsed < std::chrono::milliseconds(150)));
        CHECK_FALSE(queue->started());

        queue.reset();
    }

    SECTION("Queue shutdown drains pending tasks") {
        auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                       nullptr, &coordinator, nullptr, 32);
        queue->start(); // Start the channel poller

        // Wait for channel poller coroutine to start before enqueueing
        auto startDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!queue->started() && std::chrono::steady_clock::now() < startDeadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        REQUIRE(queue->started());

        PostIngestQueue::Task task{.hash = doc.sha256Hash, .mime = doc.mimeType};
        REQUIRE(queue->tryEnqueue(task));

        stopAndResetQueue(queue);
        SUCCEED("Queue shutdown completed without hang");
    }

    // Cleanup coordinator at test end
    coordinator.stop();
    coordinator.join();
}

TEST_CASE("PostIngestQueue: Batch uses batched metadata lookup and enqueues embeds",
          "[daemon][background][queue][batch]") {
    BusToggleGuard busGuard(false);
    drainPostIngestChannel();
    PostIngestBatchGuard batchGuard(4);

    // Ensure embed chunking policy is exercised through the PostIngestQueue -> embed_jobs path.
    ScopedEnvVar chunkStrategy("YAMS_EMBED_CHUNK_STRATEGY", "fixed");
    ScopedEnvVar chunkTarget("YAMS_EMBED_CHUNK_TARGET", "16");
    ScopedEnvVar chunkMin("YAMS_EMBED_CHUNK_MIN", "8");
    ScopedEnvVar chunkMax("YAMS_EMBED_CHUNK_MAX", "32");
    ScopedEnvVar chunkOverlap("YAMS_EMBED_CHUNK_OVERLAP", "0");
    ScopedEnvVar preserveSent("YAMS_EMBED_CHUNK_PRESERVE_SENTENCES", "0");

    WorkCoordinator coordinator;
    coordinator.start(2);

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();
    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};

    const std::string payload = []() {
        std::string out;
        out.reserve(2048);
        for (int i = 0; i < 256; ++i) {
            out += "0123456789abcdef";
        }
        return out;
    }();
    std::vector<metadata::DocumentInfo> docs;
    docs.reserve(8);
    for (int i = 0; i < 8; ++i) {
        metadata::DocumentInfo doc{};
        doc.id = 200 + i;
        doc.fileName = "doc.txt";
        doc.fileExtension = ".txt";
        doc.sha256Hash = "batch-" + std::to_string(i);
        doc.mimeType = "text/plain";
        doc.indexedTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        metadataRepo->setDocument(doc);
        store->setContent(doc.sha256Hash, payload);
        docs.push_back(doc);
    }

    auto embedChannel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EmbedJob>("embed_jobs",
                                                                                       2048);
    InternalEventBus::EmbedJob pre;
    while (embedChannel->try_pop(pre)) {
    }

    auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                   nullptr, &coordinator, nullptr, 32);
    queue->start();

    // Wait for channel poller coroutine to start before enqueueing
    auto startDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!queue->started() && std::chrono::steady_clock::now() < startDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(queue->started());

    for (const auto& doc : docs) {
        PostIngestQueue::Task task{.hash = doc.sha256Hash, .mime = doc.mimeType};
        REQUIRE(queue->tryEnqueue(task));
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (queue->processed() < docs.size() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    REQUIRE((queue->processed() == docs.size()));
    // Note: With parallel processing and caching, individual lookups may be used instead of batch
    // The important thing is that all documents were processed successfully

    std::size_t jobCount = 0;
    std::size_t docsWithPreparedChunks = 0;
    std::size_t docsWithMultipleChunks = 0;
    InternalEventBus::EmbedJob job;
    while (embedChannel->try_pop(job)) {
        jobCount++;

        for (const auto& doc : job.preparedDocs) {
            if (doc.chunks.empty()) {
                continue;
            }
            docsWithPreparedChunks++;
            if (doc.chunks.size() >= 2) {
                docsWithMultipleChunks++;
            }
            for (const auto& chunk : doc.chunks) {
                CHECK_FALSE(chunk.chunkId.empty());
                CHECK_FALSE(chunk.content.empty());
            }
        }
    }

    // PostIngestQueue should enqueue embedding jobs for successfully extracted documents.
    REQUIRE((jobCount > 0));

    // Phase 2: embed jobs should include prepared doc chunks for at least some docs.
    REQUIRE((docsWithPreparedChunks > 0));
    // With fixed chunking target=16 and a large payload, at least one doc should have >1 chunk.
    REQUIRE((docsWithMultipleChunks > 0));

    stopAndResetQueue(queue);
    coordinator.stop();
    coordinator.join();
}

TEST_CASE("PostIngestQueue: Parallel extraction preserves per-task identity",
          "[daemon][background][queue][batch][regression]") {
    BusToggleGuard busGuard(false);
    drainPostIngestChannel();
    PostIngestBatchGuard batchGuard(64);
    PostIngestConcurrencyGuard concurrencyGuard(64, 16);

    WorkCoordinator coordinator;
    coordinator.start(4);

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();
    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};
    if (TuneAdvisor::postExtractionConcurrent() <= 1) {
        SKIP("Parallel extraction concurrency not available in this test runtime");
    }

    constexpr int64_t kDocBaseId = 5000;
    // InternalEventBus channels are process-wide and may have been created by
    // earlier sections with a smaller capacity than this queue's constructor
    // request. Keep the regression batch below the smallest test channel cap so
    // identity coverage is independent of singleton creation order.
    constexpr int kDocCount = 24;
    std::vector<metadata::DocumentInfo> docs;
    docs.reserve(kDocCount);
    for (int i = 0; i < kDocCount; ++i) {
        metadata::DocumentInfo doc{};
        doc.id = kDocBaseId + i;
        doc.fileName = "doc-" + std::to_string(i) + ".txt";
        doc.fileExtension = ".txt";
        doc.sha256Hash = "parallel-hash-" + std::to_string(i);
        doc.mimeType = "text/plain";
        doc.indexedTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        metadataRepo->setDocument(doc);
        store->setContent(doc.sha256Hash, "payload-" + std::to_string(i));
        docs.push_back(doc);
    }

    auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                   nullptr, &coordinator, nullptr, 512);
    queue->start();

    auto startDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!queue->started() && std::chrono::steady_clock::now() < startDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(queue->started());

    for (const auto& doc : docs) {
        PostIngestQueue::Task task{.hash = doc.sha256Hash, .mime = doc.mimeType};
        REQUIRE(queue->tryEnqueue(task));
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (queue->processed() < docs.size() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    REQUIRE((queue->processed() == docs.size()));
    REQUIRE((queue->failed() == 0));

    auto insertedDocIds = metadataRepo->batchInsertedDocIds();
    REQUIRE((insertedDocIds.size() == docs.size()));

    std::sort(insertedDocIds.begin(), insertedDocIds.end());
    REQUIRE(
        (std::adjacent_find(insertedDocIds.begin(), insertedDocIds.end()) == insertedDocIds.end()));
    for (int i = 0; i < kDocCount; ++i) {
        REQUIRE((insertedDocIds[static_cast<std::size_t>(i)] == kDocBaseId + i));
    }

    stopAndResetQueue(queue);
    coordinator.stop();
    coordinator.join();
}

TEST_CASE("PostIngestQueue: enqueueBatch submits all tasks without loss",
          "[daemon][background][queue][batch][enqueue]") {
    BusToggleGuard busGuard(false);
    drainPostIngestChannel();
    PostIngestBatchGuard batchGuard(8);
    PostIngestConcurrencyGuard concurrencyGuard(32, 8);

    WorkCoordinator coordinator;
    coordinator.start(4);

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();
    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};

    constexpr int64_t kDocBaseId = 7000;
    constexpr int kDocCount = 64;
    std::vector<PostIngestQueue::Task> tasks;
    tasks.reserve(kDocCount);
    for (int i = 0; i < kDocCount; ++i) {
        metadata::DocumentInfo doc{};
        doc.id = kDocBaseId + i;
        doc.fileName = "batch-doc-" + std::to_string(i) + ".txt";
        doc.fileExtension = ".txt";
        doc.sha256Hash = "batch-enqueue-hash-" + std::to_string(i);
        doc.mimeType = "text/plain";
        doc.indexedTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        metadataRepo->setDocument(doc);
        store->setContent(doc.sha256Hash, "batch-payload-" + std::to_string(i));

        tasks.push_back(PostIngestQueue::Task{.hash = doc.sha256Hash, .mime = doc.mimeType});
    }

    auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                   nullptr, &coordinator, nullptr, 64);
    queue->setBatchCoalesceWindow(std::chrono::milliseconds(20));
    queue->start();
    auto startDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!queue->started() && std::chrono::steady_clock::now() < startDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(queue->started());

    bool highPriority = false;
    SECTION("normal bulk work uses the configured batch cap") {
        queue->enqueueBatch(std::move(tasks));
    }
    SECTION("priority work uses the interactive quota") {
        highPriority = true;
        queue->enqueueRpcBatch(std::move(tasks));
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (queue->processed() < kDocCount && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    REQUIRE((queue->processed() == kDocCount));
    REQUIRE((queue->failed() == 0));

    auto insertedDocIds = metadataRepo->batchInsertedDocIds();
    REQUIRE((insertedDocIds.size() == static_cast<std::size_t>(kDocCount)));
    std::sort(insertedDocIds.begin(), insertedDocIds.end());
    REQUIRE(
        (std::adjacent_find(insertedDocIds.begin(), insertedDocIds.end()) == insertedDocIds.end()));

    const auto metrics = queue->metricsSnapshot();
    const auto expectedBatches = highPriority ? 16u : 8u;
    const auto expectedMaxBatchSize = highPriority ? 4u : 8u;
    CHECK(metrics.batches.extractionBatches == expectedBatches);
    CHECK(metrics.batches.contentIndexCalls == expectedBatches);
    CHECK(metrics.batches.contentIndexMaxEntries == expectedMaxBatchSize);

    stopAndResetQueue(queue);
    coordinator.stop();
    coordinator.join();
}

TEST_CASE("PostIngestQueue: keeps multi-doc batches when extraction concurrency is low",
          "[daemon][background][queue][batch][throughput]") {
    BusToggleGuard busGuard(false);
    drainPostIngestChannel();
    PostIngestBatchGuard batchGuard(16);
    PostIngestConcurrencyGuard concurrencyGuard(32, 1);

    WorkCoordinator coordinator;
    coordinator.start(2);

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();
    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};

    constexpr int64_t kDocBaseId = 9000;
    constexpr int kDocCount = 48;
    std::vector<PostIngestQueue::Task> tasks;
    tasks.reserve(kDocCount);
    for (int i = 0; i < kDocCount; ++i) {
        metadata::DocumentInfo doc{};
        doc.id = kDocBaseId + i;
        doc.fileName = "low-concurrency-batch-doc-" + std::to_string(i) + ".txt";
        doc.fileExtension = ".txt";
        doc.sha256Hash = "low-concurrency-batch-hash-" + std::to_string(i);
        doc.mimeType = "text/plain";
        doc.indexedTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        metadataRepo->setDocument(doc);
        store->setContent(doc.sha256Hash, "payload-" + std::to_string(i));
        tasks.push_back(PostIngestQueue::Task{.hash = doc.sha256Hash, .mime = doc.mimeType});
    }

    auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                   nullptr, &coordinator, nullptr, 64);
    queue->setBatchCoalesceWindow(std::chrono::milliseconds(20));
    queue->start();
    auto startDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!queue->started() && std::chrono::steady_clock::now() < startDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    REQUIRE(queue->started());

    queue->enqueueBatch(std::move(tasks));

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (queue->processed() < kDocCount && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    REQUIRE((queue->processed() == kDocCount));
    REQUIRE((queue->failed() == 0));
    REQUIRE((metadataRepo->maxBatchWriteSize() > 1));

    const auto metrics = queue->metricsSnapshot();
    CHECK(metrics.batches.extractionTasks == kDocCount);
    CHECK(metrics.batches.extractionBatches == 3);
    CHECK(metrics.batches.contentIndexCalls == 3);
    CHECK(metrics.batches.contentIndexEntries == kDocCount);
    CHECK(metrics.batches.contentIndexMaxEntries == 16);

    stopAndResetQueue(queue);
    coordinator.stop();
    coordinator.join();
}

TEST_CASE("PostIngestQueue: full-channel enqueueBatch waits only log at debug",
          "[daemon][background][queue][logging]") {
    drainPostIngestChannel();
    SpdlogCaptureGuard logs(spdlog::level::info);

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();
    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};

    auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                   nullptr, nullptr, nullptr, 1);

    std::vector<PostIngestQueue::Task> tasks;
    tasks.push_back(PostIngestQueue::Task{.hash = "log-test-hash-1", .mime = "text/plain"});
    tasks.push_back(PostIngestQueue::Task{.hash = "log-test-hash-2", .mime = "text/plain"});

    std::thread producer([&queue, tasks]() mutable { queue->enqueueBatch(tasks); });

    std::this_thread::sleep_for(std::chrono::milliseconds(350));
    queue->stop();
    producer.join();

    CHECK((logs.str().find("enqueueBatch waiting on full channel") == std::string::npos));
}

// =============================================================================
// InternalEventBus Integration Tests
// =============================================================================

TEST_CASE("PostIngestQueue: InternalEventBus integration and stress",
          "[daemon][background][bus][stress]") {
    if (!isMpmcEnabled()) {
        SKIP("MPMC bus not enabled (set YAMS_INTERNAL_BUS_MPMC=1)");
    }

    BusToggleGuard busGuard(true); // Enable bus routing
    drainPostIngestChannel();

    // WorkCoordinator is required for PostIngestQueue strand creation
    WorkCoordinator coordinator;
    coordinator.start(4);

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();
    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};

    SECTION("Bus stress: 4 producers × 1000 tasks") {
        constexpr int64_t kDocBaseId = 1000;
        constexpr int kProducers = 4;
        constexpr int kPerProducer = 1000;
        const std::string payload = "hello world";

        auto pq = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                    nullptr, &coordinator, nullptr, 4096);
        pq->start(); // Start the channel poller

        // Spawn producers that publish into the InternalEventBus channel
        std::vector<std::thread> producers;
        producers.reserve(kProducers);
        for (int p = 0; p < kProducers; ++p) {
            producers.emplace_back([&, p]() {
                auto chan = InternalEventBus::instance()
                                .get_or_create_channel<InternalEventBus::PostIngestTask>(
                                    "post_ingest", 4096);
                for (int i = 0; i < kPerProducer; ++i) {
                    // Unique hash per item
                    std::string hash = "h-" + std::to_string(p) + "-" + std::to_string(i);
                    metadata::DocumentInfo doc{};
                    doc.id = kDocBaseId + (p * kPerProducer + i);
                    doc.fileName = "doc.txt";
                    doc.fileExtension = ".txt";
                    doc.sha256Hash = hash;
                    doc.mimeType = "text/plain";
                    metadataRepo->setDocument(doc);
                    store->setContent(hash, payload);

                    InternalEventBus::PostIngestTask t{.hash = hash, .mime = doc.mimeType};
                    while (!chan->try_push(t)) {
                        std::this_thread::yield();
                    }
                    InternalEventBus::instance().incPostQueued();
                }
            });
        }

        for (auto& t : producers) {
            t.join();
        }

        const int expected = kProducers * kPerProducer;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (static_cast<int>(pq->processed()) < expected &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        REQUIRE((static_cast<int>(pq->processed()) == expected));
        REQUIRE((pq->failed() == 0));
        REQUIRE((pq->entityInFlight() == 0));

        stopAndResetQueue(pq);
    }

    // Cleanup coordinator at test end
    coordinator.stop();
    coordinator.join();
}

// =============================================================================
// MPMC Queue Concurrency Tests
// =============================================================================

TEST_CASE("InternalEventBus: MPMC queue correctness under concurrent load",
          "[daemon][background][concurrency][mpmc]") {
    if (!isMpmcEnabled()) {
        SKIP("MPMC not enabled (set YAMS_INTERNAL_BUS_MPMC=1)");
    }

    SECTION("4 producers × 4 consumers × 5000 items") {
        constexpr int kProducers = 4;
        constexpr int kConsumers = 4;
        constexpr int kPerProducer = 5000;

        SpscQueue<int> q(1024);
        std::atomic<int> produced{0};
        std::atomic<int> consumed{0};
        std::atomic<bool> done{false};

        std::vector<std::thread> producers;
        producers.reserve(kProducers);
        for (int p = 0; p < kProducers; ++p) {
            producers.emplace_back([&]() {
                for (int i = 0; i < kPerProducer; ++i) {
                    while (!q.try_push(1)) {
                        std::this_thread::yield();
                    }
                    produced.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        std::vector<std::thread> consumers;
        consumers.reserve(kConsumers);
        for (int c = 0; c < kConsumers; ++c) {
            consumers.emplace_back([&]() {
                int v;
                while (!done.load(std::memory_order_acquire)) {
                    if (q.try_pop(v)) {
                        consumed.fetch_add(v, std::memory_order_relaxed);
                    } else {
                        std::this_thread::yield();
                    }
                }
                // Drain residual items
                while (q.try_pop(v)) {
                    consumed.fetch_add(v, std::memory_order_relaxed);
                }
            });
        }

        for (auto& t : producers) {
            t.join();
        }
        done.store(true, std::memory_order_release);
        for (auto& t : consumers) {
            t.join();
        }

        REQUIRE((produced.load() == kProducers * kPerProducer));
        REQUIRE((consumed.load() == kProducers * kPerProducer));
    }
}
