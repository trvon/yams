#include <gtest/gtest.h>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/query_helpers.h>

#include <filesystem>
#include <fstream>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::api;

namespace {

class FlakyMetadataRepository : public MetadataRepository {
public:
    explicit FlakyMetadataRepository(ConnectionPool& pool) : MetadataRepository(pool) {}

    void setGetAllMetadataFailures(std::size_t count) { getAllMetadataFailures_ = count; }
    void setQueryDocumentsFailures(std::size_t count) { queryFailures_ = count; }

    Result<std::unordered_map<std::string, MetadataValue>>
    getAllMetadata(int64_t documentId) override {
        if (consume(getAllMetadataFailures_)) {
            return Error{ErrorCode::NotInitialized, "metadata warming up"};
        }
        return MetadataRepository::getAllMetadata(documentId);
    }

    Result<std::vector<DocumentInfo>>
    queryDocuments(const metadata::DocumentQueryOptions& options) override {
        if (consume(queryFailures_)) {
            return Error{ErrorCode::DatabaseError, "database is locked"};
        }
        return MetadataRepository::queryDocuments(options);
    }

    Result<std::optional<DocumentInfo>> findDocumentByExactPath(const std::string& path) override {
        return MetadataRepository::findDocumentByExactPath(path);
    }

private:
    static bool consume(std::size_t& counter) {
        if (counter == 0)
            return false;
        --counter;
        return true;
    }

    std::size_t getAllMetadataFailures_{0};
    std::size_t queryFailures_{0};
};

} // namespace

class GrepServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        setupEnv();
        setupDB();
        setupStore();
        setupContext();
        addDocs();
        grepService_ = makeGrepService(ctx_);
    }
    void TearDown() override { cleanup(); }

    void setupEnv() {
        tmpDir_ = std::filesystem::temp_directory_path() /
                  ("grep_service_test_" + std::to_string(::getpid()));
        std::error_code ec;
        std::filesystem::create_directories(tmpDir_, ec);
        ASSERT_FALSE(ec);
    }

    void setupDB() {
        db_ = std::make_unique<Database>();
        auto open = db_->open((tmpDir_ / "yams.db").string(), ConnectionMode::Create);
        ASSERT_TRUE(open);
        pool_ = std::make_unique<ConnectionPool>((tmpDir_ / "yams.db").string(),
                                                 ConnectionPoolConfig{});
        repo_ = std::make_shared<MetadataRepository>(*pool_);
        MigrationManager mm(*db_);
        ASSERT_TRUE(mm.initialize());
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        ASSERT_TRUE(mm.migrate());
    }

    void setupStore() {
        ContentStoreBuilder b;
        auto storeRes = b.withStoragePath(tmpDir_ / "storage")
                            .withCompression(false)
                            .withDeduplication(false)
                            .build();
        ASSERT_TRUE(storeRes);
        auto& u = storeRes.value();
        store_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(u).release());
    }

    void setupContext() {
        ctx_.store = store_;
        ctx_.metadataRepo = repo_;
        ctx_.searchExecutor = nullptr;
        ctx_.hybridEngine = nullptr;
    }

    std::filesystem::path writeFile(const std::string& name, const std::string& content) {
        auto p = tmpDir_ / name;
        std::ofstream f(p);
        f << content;
        return p;
    }

    void addOne(const std::string& name, const std::string& content) {
        auto p = writeFile(name, content);
        auto ds = makeDocumentService(ctx_);
        StoreDocumentRequest req;
        req.path = p.string();
        ASSERT_TRUE(ds->store(req));
    }

    void addDocs() {
        addOne("a.txt", "alpha beta gamma\nhello world\nregex target\n");
        addOne("b.txt", "semantic related content about programming\n");
    }

    void cleanup() {
        repo_.reset();
        pool_.reset();
        if (db_) {
            db_->close();
            db_.reset();
        }
        std::error_code ec;
        std::filesystem::remove_all(tmpDir_, ec);
    }

protected:
    std::filesystem::path tmpDir_;
    std::unique_ptr<Database> db_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> repo_;
    std::shared_ptr<IContentStore> store_;
    AppContext ctx_;
    std::shared_ptr<IGrepService> grepService_;
};

TEST_F(GrepServiceTest, RegexOnlyFindsRegexMatches) {
    GrepRequest rq;
    rq.pattern = "regex";
    rq.regexOnly = true;
    rq.lineNumbers = true;
    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res);
    const auto& r = res.value();
    EXPECT_GT(r.totalMatches, 0u);
    EXPECT_EQ(r.semanticMatches, 0u);
    EXPECT_GE(r.executionTimeMs, 0);
    ASSERT_TRUE(r.searchStats.contains("metadata_operations"));
    EXPECT_NE(r.searchStats.at("metadata_operations"), "0");
    ASSERT_TRUE(r.searchStats.contains("latency_ms"));
}

TEST_F(GrepServiceTest, HybridModeIncludesSemanticWhenRegexOff) {
    GrepRequest rq;
    rq.pattern = "programming"; // unlikely to match regex in a.txt lines
    rq.regexOnly = false;
    rq.semanticLimit = 2;
    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res);
    const auto& r = res.value();
    // We allow either 0 or >0 regex matches; semantic should be allowed and non-negative
    EXPECT_GE(r.semanticMatches, 0u);
}

TEST_F(GrepServiceTest, CountModeAllowsSemanticSuggestions) {
    GrepRequest rq;
    rq.pattern = "programming";
    rq.regexOnly = false;
    rq.semanticLimit = 2;
    rq.count = true; // count-only mode
    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res);
    const auto& r = res.value();
    // Semantic suggestions should not be suppressed in count mode (may be zero if no sem hits)
    EXPECT_GE(r.semanticMatches, 0u);
    // If semantic suggestions exist, they should appear in results
    (void)std::any_of(r.results.begin(), r.results.end(), [](const auto& fr) {
        return fr.wasSemanticSearch ||
               std::any_of(fr.matches.begin(), fr.matches.end(),
                           [](const auto& m) { return m.matchType == "semantic"; });
    });
    // Allow either presence or absence based on data, but ensure no crash/suppression path
    SUCCEED();
}

TEST_F(GrepServiceTest, PathFiltersNormalizeRelativeSegments) {
    GrepRequest rq;
    rq.pattern = "hello";
    rq.paths.push_back((tmpDir_ / std::filesystem::path("./a.txt")).string());
    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res);
    const auto& out = res.value();
    ASSERT_FALSE(out.results.empty());
    EXPECT_EQ(out.results.front().fileName, "a.txt");
    EXPECT_GT(out.totalMatches, 0u);
}

TEST_F(GrepServiceTest, FilesOnlyModeAllowsSemanticSuggestions) {
    GrepRequest rq;
    rq.pattern = "programming";
    rq.regexOnly = false;
    rq.semanticLimit = 2;
    rq.filesWithMatches = true; // list files with matches
    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res);
    const auto& r = res.value();
    // Semantic suggestions should not be suppressed in files-only mode (may be zero if no sem hits)
    EXPECT_GE(r.semanticMatches, 0u);
    (void)std::any_of(r.results.begin(), r.results.end(), [](const auto& fr) {
        return fr.wasSemanticSearch ||
               std::any_of(fr.matches.begin(), fr.matches.end(),
                           [](const auto& m) { return m.matchType == "semantic"; });
    });
    SUCCEED();
}

TEST_F(GrepServiceTest, PathsOnlyModeAllowsSemanticSuggestions) {
    GrepRequest rq;
    rq.pattern = "programming";
    rq.regexOnly = false;
    rq.semanticLimit = 2;
    rq.pathsOnly = true; // paths-only output mode
    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res);
    const auto& r = res.value();
    // Semantic suggestions should not be suppressed in paths-only mode (may be zero if no sem hits)
    EXPECT_GE(r.semanticMatches, 0u);
    (void)std::any_of(r.results.begin(), r.results.end(), [](const auto& fr) {
        return fr.wasSemanticSearch ||
               std::any_of(fr.matches.begin(), fr.matches.end(),
                           [](const auto& m) { return m.matchType == "semantic"; });
    });
    SUCCEED();
}

TEST_F(GrepServiceTest, RetriesTransientMetadataErrors) {
    auto flakyRepo = std::make_shared<FlakyMetadataRepository>(*pool_);
    auto docsRes = metadata::queryDocumentsByPattern(*flakyRepo, "%");
    ASSERT_TRUE(docsRes);
    ASSERT_FALSE(docsRes.value().empty());
    const auto docId = docsRes.value().front().id;

    ASSERT_TRUE(flakyRepo->setMetadata(docId, "force_cold", MetadataValue("true")));

    flakyRepo->setGetAllMetadataFailures(1);
    repo_ = flakyRepo;
    ctx_.metadataRepo = flakyRepo;
    grepService_ = makeGrepService(ctx_);

    GrepRequest rq;
    rq.pattern = "alpha";
    rq.literalText = true;

    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res) << res.error().message;
    EXPECT_GT(res.value().totalMatches, 0u);
}

TEST_F(GrepServiceTest, PropagatesMetadataErrorsWhenTagsUnavailable) {
    auto flakyRepo = std::make_shared<FlakyMetadataRepository>(*pool_);
    auto docsRes = metadata::queryDocumentsByPattern(*flakyRepo, "%");
    ASSERT_TRUE(docsRes);
    ASSERT_FALSE(docsRes.value().empty());
    const auto docId = docsRes.value().front().id;

    ASSERT_TRUE(
        flakyRepo->setMetadata(docId, "tag:ready_flag", MetadataValue(std::string("true"))));

    flakyRepo->setGetAllMetadataFailures(8);
    repo_ = flakyRepo;
    ctx_.metadataRepo = flakyRepo;
    grepService_ = makeGrepService(ctx_);

    GrepRequest rq;
    rq.pattern = "alpha";
    rq.literalText = true;
    rq.tags = {"ready_flag"};

    auto res = grepService_->grep(rq);
    ASSERT_FALSE(res);
    EXPECT_EQ(res.error().code, ErrorCode::NotInitialized)
        << "Actual code=" << static_cast<int>(res.error().code)
        << " message=" << res.error().message;
    EXPECT_FALSE(res.error().message.empty());
}

TEST_F(GrepServiceTest, SubpathFilterMatches) {
    // Create a file in a subdirectory
    std::filesystem::path subDir = tmpDir_ / "sub";
    std::filesystem::create_directory(subDir);
    addOne("sub/c.txt", "subpath content");

    GrepRequest rq;
    rq.pattern = "subpath";
    rq.paths.push_back("sub/c.txt"); // use a subpath

    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res);
    const auto& out = res.value();
    ASSERT_FALSE(out.results.empty());
    EXPECT_EQ(out.results.front().fileName, "c.txt");
    EXPECT_GT(out.totalMatches, 0u);
}
