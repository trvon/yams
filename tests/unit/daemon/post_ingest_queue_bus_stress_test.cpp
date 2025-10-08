#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <yams/api/content_store.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/extraction/content_extractor.h>
#include <yams/metadata/metadata_repository.h>

namespace yams::daemon::test {

namespace {

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

class StubContentStore : public api::IContentStore {
public:
    void setContent(const std::string& hash, const std::string& data) {
        std::lock_guard<std::mutex> lk(mu_);
        yams::ByteVector bytes;
        bytes.reserve(data.size());
        for (unsigned char c : data) {
            bytes.push_back(static_cast<std::byte>(c));
        }
        blobs_[hash] = std::move(bytes);
    }
    Result<api::RetrieveResult> retrieveStream(const std::string& hash, std::ostream& os,
                                               api::ProgressCallback) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = blobs_.find(hash);
        if (it == blobs_.end())
            return Error{ErrorCode::NotFound, "no blob"};
        for (auto b : it->second)
            os.put(static_cast<char>(b));
        api::RetrieveResult r{};
        r.found = true;
        r.size = static_cast<uint64_t>(it->second.size());
        return r;
    }
    Result<std::vector<std::byte>> retrieveBytes(const std::string& hash) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = blobs_.find(hash);
        if (it == blobs_.end())
            return Error{ErrorCode::NotFound, "no blob"};
        return it->second;
    }
    Result<api::IContentStore::RawContent> retrieveRaw(const std::string& hash) override {
        auto bytes = retrieveBytes(hash);
        if (!bytes)
            return bytes.error();
        api::IContentStore::RawContent raw;
        raw.data = std::move(bytes.value());
        return raw;
    }
    std::future<Result<api::IContentStore::RawContent>>
    retrieveRawAsync(const std::string& hash) override {
        return std::async(std::launch::deferred, [this, hash]() { return retrieveRaw(hash); });
    }

    // Unused methods in this test
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
    std::unordered_map<std::string, yams::ByteVector> blobs_;
};

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
    Result<std::optional<metadata::DocumentInfo>>
    getDocumentByHash(const std::string& hash) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (hasDocument_ && document_.sha256Hash == hash)
            return std::optional<metadata::DocumentInfo>(document_);
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }
    Result<std::optional<metadata::DocumentInfo>> getDocument(int64_t id) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (hasDocument_ && document_.id == id)
            return std::optional<metadata::DocumentInfo>(document_);
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }
    Result<void> insertContent(const metadata::DocumentContent&) override { return Result<void>(); }
    Result<void> updateDocument(const metadata::DocumentInfo& info) override {
        std::lock_guard<std::mutex> lk(mu_);
        document_ = info;
        lastUpdated_ = info;
        return Result<void>();
    }
    Result<void> indexDocumentContent(int64_t, const std::string&, const std::string&,
                                      const std::string&) override {
        return Result<void>();
    }
    Result<void> updateFuzzyIndex(int64_t) override { return Result<void>(); }
    Result<std::vector<std::string>> getDocumentTags(int64_t) override {
        return std::vector<std::string>{};
    }

private:
    mutable std::mutex mu_;
    bool hasDocument_{false};
    metadata::DocumentInfo document_{};
    metadata::DocumentInfo lastUpdated_{};
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
        for (auto b : bytes)
            text.push_back(static_cast<char>(b));
        return text;
    }
};

} // namespace

TEST(PostIngestQueueBusStressTest, ManyProducersAndWorkersProcessAllTasks) {
    // This stress assumes an MPMC-capable internal bus. Skip unless explicitly enabled.
    if (const char* m = std::getenv("YAMS_INTERNAL_BUS_MPMC")) {
        if (std::string(m) != "1") {
            GTEST_SKIP() << "MPMC bus not enabled; skipping post-ingest MPMC stress.";
        }
    } else {
        GTEST_SKIP() << "MPMC bus not enabled; skipping post-ingest MPMC stress.";
    }
    BusToggleGuard busGuard(true); // force InternalEventBus path

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();
    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};

    // Seed a single document template reused by varying hashes
    constexpr int64_t kDocBaseId = 1000;
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 1000;
    const std::string payload = "hello world";

    auto pq = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr,
                                                /*threads*/ 4, /*capacity*/ 4096);

    // Spawn producers that publish into the InternalEventBus channel
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p]() {
            auto chan =
                InternalEventBus::instance()
                    .get_or_create_channel<InternalEventBus::PostIngestTask>("post_ingest", 4096);
            for (int i = 0; i < kPerProducer; ++i) {
                // Unique hash per item; seed store + metadata entry
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
                // Keep trying to push until accepted
                while (!chan->try_push(t)) {
                    std::this_thread::yield();
                }
                InternalEventBus::instance().incPostQueued();
                pq->notifyWorkers();
            }
        });
    }

    for (auto& t : producers)
        t.join();

    const int expected = kProducers * kPerProducer;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (static_cast<int>(pq->processed()) < expected &&
           std::chrono::steady_clock::now() < deadline) {
        pq->notifyWorkers();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_EQ(static_cast<int>(pq->processed()), expected);
    EXPECT_EQ(pq->failed(), 0u);
}

} // namespace yams::daemon::test
