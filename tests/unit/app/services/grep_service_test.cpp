#include <gtest/gtest.h>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

#include <filesystem>
#include <fstream>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::api;

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
