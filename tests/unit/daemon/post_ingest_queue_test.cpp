#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <yams/api/content_store.h>
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
    Result<api::RetrieveResult> retrieveStream(const std::string&, std::ostream&,
                                               api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::StoreResult> storeBytes(std::span<const std::byte>,
                                        const api::ContentMetadata&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<std::byte>> retrieveBytes(const std::string& hash) override {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = blobs_.find(hash);
        if (it == blobs_.end()) {
            return Error{ErrorCode::NotFound, "content not found"};
        }
        return it->second;
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

    Result<int64_t> insertDocument(const metadata::DocumentInfo&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::optional<metadata::DocumentInfo>> getDocument(int64_t id) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (hasDocument_ && document_.id == id)
            return std::optional<metadata::DocumentInfo>(document_);
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }
    Result<std::optional<metadata::DocumentInfo>>
    getDocumentByHash(const std::string& hash) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (hasDocument_ && document_.sha256Hash == hash)
            return std::optional<metadata::DocumentInfo>(document_);
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }
    Result<void> updateDocument(const metadata::DocumentInfo& info) override {
        std::lock_guard<std::mutex> lk(mu_);
        document_ = info;
        lastUpdated_ = info;
        return Result<void>();
    }
    Result<void> deleteDocument(int64_t) override { return ErrorCode::NotImplemented; }
    Result<void> insertContent(const metadata::DocumentContent& content) override {
        std::lock_guard<std::mutex> lk(mu_);
        lastContent_ = content;
        contentInserted_ = true;
        return Result<void>();
    }
    Result<std::optional<metadata::DocumentContent>> getContent(int64_t) override {
        return ErrorCode::NotImplemented;
    }
    Result<void> updateContent(const metadata::DocumentContent&) override {
        return ErrorCode::NotImplemented;
    }
    Result<void> deleteContent(int64_t) override { return ErrorCode::NotImplemented; }
    Result<void> setMetadata(int64_t, const std::string&, const metadata::MetadataValue&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::optional<metadata::MetadataValue>> getMetadata(int64_t,
                                                               const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::unordered_map<std::string, metadata::MetadataValue>>
    getAllMetadata(int64_t) override {
        return ErrorCode::NotImplemented;
    }
    Result<void> removeMetadata(int64_t, const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<int64_t> insertRelationship(const metadata::DocumentRelationship&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<metadata::DocumentRelationship>> getRelationships(int64_t) override {
        return std::vector<metadata::DocumentRelationship>{};
    }
    Result<void> deleteRelationship(int64_t) override { return ErrorCode::NotImplemented; }
    Result<int64_t> insertSearchHistory(const metadata::SearchHistoryEntry&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<metadata::SearchHistoryEntry>> getRecentSearches(int) override {
        return std::vector<metadata::SearchHistoryEntry>{};
    }
    Result<int64_t> insertSavedQuery(const metadata::SavedQuery&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::optional<metadata::SavedQuery>> getSavedQuery(int64_t) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<metadata::SavedQuery>> getAllSavedQueries() override {
        return std::vector<metadata::SavedQuery>{};
    }
    Result<void> updateSavedQuery(const metadata::SavedQuery&) override {
        return ErrorCode::NotImplemented;
    }
    Result<void> deleteSavedQuery(int64_t) override { return ErrorCode::NotImplemented; }
    Result<void> indexDocumentContent(int64_t, const std::string&, const std::string& content,
                                      const std::string&) override {
        std::lock_guard<std::mutex> lk(mu_);
        indexedContent_ = content;
        return Result<void>();
    }
    Result<void> removeFromIndex(int64_t) override { return ErrorCode::NotImplemented; }
    Result<metadata::SearchResults>
    search(const std::string&, int, int,
           const std::optional<std::vector<int64_t>>& = std::nullopt) override {
        return ErrorCode::NotImplemented;
    }
    Result<metadata::SearchResults>
    fuzzySearch(const std::string&, float, int,
                const std::optional<std::vector<int64_t>>& = std::nullopt) override {
        return ErrorCode::NotImplemented;
    }
    Result<void> buildFuzzyIndex() override { return ErrorCode::NotImplemented; }
    Result<void> updateFuzzyIndex(int64_t) override { return Result<void>(); }
    Result<std::vector<metadata::DocumentInfo>> findDocumentsByPath(const std::string&) override {
        return std::vector<metadata::DocumentInfo>{};
    }
    Result<std::vector<metadata::DocumentInfo>>
    findDocumentsByExtension(const std::string&) override {
        return std::vector<metadata::DocumentInfo>{};
    }
    Result<std::vector<metadata::DocumentInfo>>
    findDocumentsModifiedSince(std::chrono::system_clock::time_point) override {
        return std::vector<metadata::DocumentInfo>{};
    }
    Result<std::vector<metadata::DocumentInfo>>
    findDocumentsByCollection(const std::string&) override {
        return std::vector<metadata::DocumentInfo>{};
    }
    Result<std::vector<metadata::DocumentInfo>>
    findDocumentsBySnapshot(const std::string&) override {
        return std::vector<metadata::DocumentInfo>{};
    }
    Result<std::vector<metadata::DocumentInfo>>
    findDocumentsBySnapshotLabel(const std::string&) override {
        return std::vector<metadata::DocumentInfo>{};
    }
    Result<std::vector<std::string>> getCollections() override {
        return std::vector<std::string>{};
    }
    Result<std::vector<std::string>> getSnapshots() override { return std::vector<std::string>{}; }
    Result<std::vector<std::string>> getSnapshotLabels() override {
        return std::vector<std::string>{};
    }
    Result<std::vector<metadata::DocumentInfo>> findDocumentsByTags(const std::vector<std::string>&,
                                                                    bool) override {
        return std::vector<metadata::DocumentInfo>{};
    }
    Result<std::vector<std::string>> getDocumentTags(int64_t) override {
        return std::vector<std::string>{};
    }
    Result<std::vector<std::string>> getAllTags() override { return std::vector<std::string>{}; }
    Result<int64_t> getDocumentCount() override { return ErrorCode::NotImplemented; }
    Result<int64_t> getIndexedDocumentCount() override { return ErrorCode::NotImplemented; }
    Result<int64_t> getContentExtractedDocumentCount() override {
        return ErrorCode::NotImplemented;
    }
    Result<std::unordered_map<std::string, int64_t>> getDocumentCountsByExtension() override {
        return std::unordered_map<std::string, int64_t>{};
    }
    Result<int64_t> getDocumentCountByExtractionStatus(metadata::ExtractionStatus) override {
        return ErrorCode::NotImplemented;
    }
    Result<void> updateDocumentEmbeddingStatus(int64_t, bool, const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<void> updateDocumentEmbeddingStatusByHash(const std::string&, bool,
                                                     const std::string&) override {
        return ErrorCode::NotImplemented;
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

} // namespace

TEST(PostIngestQueueStandardTest, ProcessesTaskAndPersistsContent) {
    BusToggleGuard busGuard(false);

    auto store = std::make_shared<StubContentStore>();
    auto metadataRepo = std::make_shared<StubMetadataRepository>();

    metadata::DocumentInfo doc{};
    doc.id = 101;
    doc.fileName = "doc.txt";
    doc.fileExtension = ".txt";
    doc.sha256Hash = "hash-123";
    doc.mimeType = "text/plain";
    doc.indexedTime = std::chrono::system_clock::now();
    metadataRepo->setDocument(doc);

    const std::string payload = "Hello from post-ingest";
    store->setContent(doc.sha256Hash, payload);

    auto extractor = std::make_shared<StubExtractor>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors{extractor};

    auto queue = std::make_unique<PostIngestQueue>(store, metadataRepo, extractors, nullptr, 1, 8);

    PostIngestQueue::Task task{
        doc.sha256Hash, doc.mimeType, "", {}, PostIngestQueue::Task::Stage::Metadata};
    ASSERT_TRUE(queue->tryEnqueue(task));

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (queue->processed() < 1 && std::chrono::steady_clock::now() < deadline) {
        queue->notifyWorkers();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_EQ(queue->processed(), 1u);
    EXPECT_EQ(queue->failed(), 0u);
    EXPECT_TRUE(metadataRepo->contentInserted());

    auto content = metadataRepo->lastContent();
    EXPECT_EQ(content.documentId, doc.id);
    EXPECT_EQ(content.contentText, payload);
    EXPECT_EQ(content.contentLength, static_cast<int64_t>(payload.size()));
    EXPECT_EQ(content.extractionMethod, "post_ingest");
    EXPECT_FALSE(content.language.empty());

    auto updated = metadataRepo->lastUpdated();
    EXPECT_TRUE(updated.contentExtracted);
    EXPECT_EQ(updated.extractionStatus, metadata::ExtractionStatus::Success);

    queue.reset();
}

} // namespace yams::daemon::test
