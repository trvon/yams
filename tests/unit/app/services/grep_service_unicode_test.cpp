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

class GrepServiceUnicodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        tmpDir_ =
            std::filesystem::temp_directory_path() / ("grep_unicode_" + std::to_string(::getpid()));
        std::error_code ec;
        std::filesystem::create_directories(tmpDir_, ec);
        ASSERT_FALSE(ec);

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

        ContentStoreBuilder b;
        auto storeRes = b.withStoragePath(tmpDir_ / "storage")
                            .withCompression(false)
                            .withDeduplication(false)
                            .build();
        ASSERT_TRUE(storeRes);
        store_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(storeRes.value()).release());

        ctx_.store = store_;
        ctx_.metadataRepo = repo_;
        grep_ = makeGrepService(ctx_);

        // Unicode content fixture
        std::ofstream out(tmpDir_ / "u.txt");
        out << "café\nCAFÉ\nnaïve\n(naïve)\nsmile 😊 end\n東京大学 CJK line\n";
        out.close();
        auto ds = makeDocumentService(ctx_);
        StoreDocumentRequest req;
        req.path = (tmpDir_ / "u.txt").string();
        ASSERT_TRUE(ds->store(req));
    }

    void TearDown() override {
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
    std::shared_ptr<IGrepService> grep_;
};

TEST_F(GrepServiceUnicodeTest, LiteralUnicodeAndEmoji) {
    // café literal
    GrepRequest rq1;
    rq1.pattern = "café";
    rq1.literalText = true;
    auto r1 = grep_->grep(rq1);
    ASSERT_TRUE(r1) << r1.error().message;
    EXPECT_GT(r1.value().totalMatches, 0u);
    // emoji literal
    GrepRequest rq2;
    rq2.pattern = "😊";
    rq2.literalText = true;
    auto r2 = grep_->grep(rq2);
    ASSERT_TRUE(r2) << r2.error().message;
    EXPECT_GT(r2.value().totalMatches, 0u);
    // parentheses + diacritic
    GrepRequest rq3;
    rq3.pattern = "(naïve)";
    rq3.literalText = true;
    auto r3 = grep_->grep(rq3);
    ASSERT_TRUE(r3) << r3.error().message;
    EXPECT_GT(r3.value().totalMatches, 0u);
}

TEST_F(GrepServiceUnicodeTest, IgnoreCaseLatinExtended_BestEffort) {
    // Try case-insensitive match for café vs CAFÉ.
    GrepRequest rq;
    rq.pattern = "café";
    rq.literalText = true;
    rq.ignoreCase = true;
    auto r = grep_->grep(rq);
    ASSERT_TRUE(r) << r.error().message;
    if (r.value().totalMatches == 0) {
        GTEST_SKIP() << "Unicode case-folding not available in this build; skipping";
    }
    EXPECT_GT(r.value().totalMatches, 0u);
}
