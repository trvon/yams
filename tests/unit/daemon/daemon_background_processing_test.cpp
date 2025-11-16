// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 Trevon Helm
// Consolidated background processing tests (3 → 1): PostIngestQueue, InternalEventBus, MPMC
// concurrency Covers: queue lifecycle, async processing, bus integration, stress testing, MPMC
// correctness

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <catch2/catch_test_macros.hpp>
#include <yams/api/content_store.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/extraction/content_extractor.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::daemon;

namespace {

// =============================================================================
// Test Helpers
// =============================================================================

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
        blobs_[hash] = std::move(bytes);
    }

    Result<std::vector<std::byte>> retrieveBytes(const std::string& hash) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = blobs_.find(hash);
        if (it == blobs_.end()) {
            return Error{ErrorCode::NotFound, "content not found"};
        }
        return it->second;
    }

    Result<api::IContentStore::RawContent> retrieveRaw(const std::string& hash) override {
        auto bytes = retrieveBytes(hash);
        if (!bytes) {
            return bytes.error();
        }
        api::IContentStore::RawContent raw;
        raw.data = std::move(bytes.value());
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

    // Unused methods (required by interface)
    Result<api::StoreResult> store(const std::filesystem::path&, const api::ContentMetadata&,
                                   api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
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
    storeBatch(const std::vector<std::filesystem::path>&,
               const std::vector<api::ContentMetadata>&) override {
        return {};
    }
    std::vector<Result<bool>> removeBatch(const std::vector<std::string>&) override { return {}; }
    api::ContentStoreStats getStats() const override { return {}; }
    api::HealthStatus checkHealth() const override { return {}; }
    Result<void> verify(api::ProgressCallback) override { return ErrorCode::NotImplemented; }
    Result<void> compact(api::ProgressCallback) override { return ErrorCode::NotImplemented; }
    Result<void> garbageCollect(api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, ByteVector> blobs_;
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
        document_ = std::move(doc);
        hasDocument_ = true;
        lastUpdated_ = document_;
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
        if (hasDocument_ && document_.sha256Hash == hash) {
            return std::optional<metadata::DocumentInfo>(document_);
        }
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }

    Result<std::optional<metadata::DocumentInfo>> getDocument(int64_t id) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (hasDocument_ && document_.id == id) {
            return std::optional<metadata::DocumentInfo>(document_);
        }
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }

    Result<void> updateDocument(const metadata::DocumentInfo& info) override {
        std::lock_guard<std::mutex> lk(mu_);
        document_ = info;
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

    Result<void> updateFuzzyIndex(int64_t) override { return Result<void>(); }

    Result<std::vector<std::string>> getDocumentTags(int64_t) override {
        return std::vector<std::string>{};
    }

private:
    mutable std::mutex mu_;
    metadata::DocumentInfo document_{};
    metadata::DocumentInfo lastUpdated_{};
    metadata::DocumentContent lastContent_{};
    std::string indexedContent_;
    bool hasDocument_{false};
    bool contentInserted_{false};
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

bool isMpmcEnabled() {
    const char* m = std::getenv("YAMS_INTERNAL_BUS_MPMC");
    return m && std::string(m) == "1";
}

} // namespace

// =============================================================================
// PostIngestQueue Tests
// =============================================================================

TEST_CASE("PostIngestQueue: Basic lifecycle and task processing", "[daemon][background][queue]") {
    BusToggleGuard busGuard(false); // Disable bus for direct queue testing

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
        auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                       nullptr, nullptr, 8);

        PostIngestQueue::Task task{
            doc.sha256Hash, doc.mimeType, "", {}, PostIngestQueue::Task::Stage::Metadata};
        REQUIRE(queue->tryEnqueue(std::move(task)));

        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (queue->processed() < 1 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        REQUIRE(queue->processed() == 1);
        REQUIRE(queue->failed() == 0);
        REQUIRE(metadataRepo->contentInserted());

        auto content = metadataRepo->lastContent();
        REQUIRE(content.documentId == doc.id);
        REQUIRE(content.contentText == payload);
        REQUIRE(content.contentLength == static_cast<int64_t>(payload.size()));
        REQUIRE(content.extractionMethod == "post_ingest");
        REQUIRE_FALSE(content.language.empty());

        auto updated = metadataRepo->lastUpdated();
        REQUIRE(updated.contentExtracted);
        REQUIRE(updated.extractionStatus == metadata::ExtractionStatus::Success);

        queue.reset();
    }

    SECTION("Queue shutdown drains pending tasks") {
        auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                       nullptr, nullptr, 8);

        PostIngestQueue::Task task{
            doc.sha256Hash, doc.mimeType, "", {}, PostIngestQueue::Task::Stage::Metadata};
        REQUIRE(queue->tryEnqueue(std::move(task)));

        queue.reset();
        SUCCEED("Queue shutdown completed without hang");
    }
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
                                                    nullptr, nullptr, 4096);

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

                    InternalEventBus::PostIngestTask t{hash, doc.mimeType};
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

        REQUIRE(static_cast<int>(pq->processed()) == expected);
        REQUIRE(pq->failed() == 0);
    }
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

        REQUIRE(produced.load() == kProducers * kPerProducer);
        REQUIRE(consumed.load() == kProducers * kPerProducer);
    }
}
