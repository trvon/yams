#include <gtest/gtest.h>

#include <filesystem>
#include <future>
#include <random>
#include <thread>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/PostIngestQueue.h>
#include <yams/extraction/content_extractor.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/vector_database.h>

namespace yams::repair::test {

namespace {
// Minimal stubs to run PostIngestQueue without touching storage backends
class NoopStore : public api::IContentStore {
public:
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
    Result<std::vector<std::byte>> retrieveBytes(const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::IContentStore::RawContent> retrieveRaw(const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    std::future<Result<api::IContentStore::RawContent>>
    retrieveRawAsync(const std::string&) override {
        return std::async(std::launch::deferred, [] {
            return Result<api::IContentStore::RawContent>(ErrorCode::NotImplemented);
        });
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
};

class InMemoryMetadata : public metadata::MetadataRepository {
public:
    InMemoryMetadata() : metadata::MetadataRepository(*getPool()) {}
    void add(const metadata::DocumentInfo& d) { docs_.push_back(d); }
    Result<std::vector<metadata::DocumentInfo>>
    queryDocuments(const metadata::DocumentQueryOptions&) override {
        return docs_;
    }
    Result<std::optional<metadata::DocumentInfo>>
    findDocumentByExactPath(const std::string& path) override {
        for (auto& d : docs_) {
            if (d.filePath == path)
                return std::optional<metadata::DocumentInfo>(d);
        }
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }
    Result<std::vector<std::string>> getDocumentTags(int64_t) override {
        return std::vector<std::string>{};
    }
    Result<std::optional<metadata::DocumentInfo>> getDocumentByHash(const std::string& h) override {
        for (auto& d : docs_)
            if (d.sha256Hash == h)
                return std::optional<metadata::DocumentInfo>(d);
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }

private:
    static std::shared_ptr<metadata::ConnectionPool> getPool() {
        static std::shared_ptr<metadata::ConnectionPool> p = [] {
            metadata::ConnectionPoolConfig cfg{};
            cfg.enableWAL = false;
            cfg.enableForeignKeys = false;
            auto pool = std::make_shared<metadata::ConnectionPool>(":memory:", cfg);
            (void)pool->initialize();
            return pool;
        }();
        return p;
    }
    std::vector<metadata::DocumentInfo> docs_;
};
} // namespace

TEST(RepairUtilScanTest, MissingEmbeddingsListStableUnderPostIngestLoad) {
    // Prepare temp dir for vector DB
    auto tmp = std::filesystem::temp_directory_path() /
               ("yams_repair_test_" + std::to_string(std::rand()));
    std::filesystem::create_directories(tmp);

    // Seed metadata with two docs; pre-insert one embedding
    auto repo = std::make_shared<InMemoryMetadata>();
    metadata::DocumentInfo d1{};
    d1.id = 1;
    d1.sha256Hash = "h1";
    d1.fileName = "a.txt";
    d1.mimeType = "text/plain";
    metadata::DocumentInfo d2{};
    d2.id = 2;
    d2.sha256Hash = "h2";
    d2.fileName = "b.txt";
    d2.mimeType = "text/plain";
    repo->add(d1);
    repo->add(d2);

    // Create vector DB and insert embedding for h1
    yams::vector::VectorDatabaseConfig cfg{};
    cfg.database_path = (tmp / "vectors.db").string();
    cfg.create_if_missing = true;
    cfg.embedding_dim = 384;
    auto vdb = std::make_unique<yams::vector::VectorDatabase>(cfg);
    ASSERT_TRUE(vdb->initialize());
    yams::vector::VectorRecord rec;
    rec.document_hash = "h1";
    rec.chunk_id = "c-1";
    rec.embedding = std::vector<float>(384, 0.1f);
    rec.content = "hello";
    ASSERT_TRUE(vdb->insertVector(rec));

    // Spin up PostIngestQueue to simulate concurrent work
    auto store = std::make_shared<NoopStore>();
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors;
    std::shared_ptr<metadata::MetadataRepository> baseRepo = repo;
    yams::daemon::PostIngestQueue pq(store, baseRepo, extractors, nullptr, 2, 64);

    for (int i = 0; i < 100; i++) {
        yams::daemon::InternalEventBus::PostIngestTask t{"random-" + std::to_string(i),
                                                         "text/plain"};
        auto ch = yams::daemon::InternalEventBus::instance()
                      .get_or_create_channel<yams::daemon::InternalEventBus::PostIngestTask>(
                          "post_ingest", 1024);
        (void)ch->try_push(t);
        pq.notifyWorkers();
    }

    // Call repair scan while queue is busy
    auto missing = getDocumentsMissingEmbeddings(repo, tmp);
    ASSERT_TRUE(missing);
    auto list = missing.value();
    // Only h2 should be missing
    ASSERT_EQ(list.size(), 1u);
    EXPECT_EQ(list[0], std::string("h2"));
}

} // namespace yams::repair::test
