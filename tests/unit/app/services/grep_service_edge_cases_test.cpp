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

class GrepServiceEdgeCasesTest : public ::testing::Test {
protected:
    void SetUp() override {
        tmpDir_ =
            std::filesystem::temp_directory_path() / ("grep_edge_" + std::to_string(::getpid()));
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
        grepService_ = makeGrepService(ctx_);

        // Create a file with edge-case tokens
        auto p = tmpDir_ / "wb.txt";
        std::ofstream f(p);
        f << "foo bar\nfoo-bar\nfoo_bar\n(hello) and (foo)\nHELLO world\n";
        f.close();

        auto ds = makeDocumentService(ctx_);
        StoreDocumentRequest req;
        req.path = p.string();
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
    std::shared_ptr<IGrepService> grepService_;
};

TEST_F(GrepServiceEdgeCasesTest, WordBoundaryHyphenExcluded) {
    GrepRequest rq;
    rq.pattern = "foo";
    rq.word = true;
    rq.lineNumbers = true;
    rq.withFilename = false;
    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res) << res.error().message;
    const auto& out = res.value();
    ASSERT_GT(out.totalMatches, 0u);
    // Expect no matches on the hyphenated token line ("foo-bar").
    for (const auto& fr : out.results) {
        for (const auto& m : fr.matches) {
            // The match line should not be the hyphen case.
            ASSERT_EQ(m.line.find("foo-"), std::string::npos);
        }
    }
}

TEST_F(GrepServiceEdgeCasesTest, LiteralParenthesesNotRegex) {
    // literalText should produce at least one match when searching for parentheses literally
    GrepRequest literal;
    literal.pattern = "(foo)";
    literal.literalText = true;
    auto r1 = grepService_->grep(literal);
    ASSERT_TRUE(r1) << r1.error().message;
    EXPECT_GT(r1.value().totalMatches, 0u);

    // regex mode also allowed; expect at least one match
    GrepRequest regexRq;
    regexRq.pattern = "(foo)";
    regexRq.literalText = false;
    auto r2 = grepService_->grep(regexRq);
    ASSERT_TRUE(r2) << r2.error().message;
    EXPECT_GT(r2.value().totalMatches, 0u);
}

TEST_F(GrepServiceEdgeCasesTest, IgnoreCaseMatches) {
    GrepRequest rq;
    rq.pattern = "HELLO";
    rq.ignoreCase = true;
    auto res = grepService_->grep(rq);
    ASSERT_TRUE(res) << res.error().message;
    EXPECT_GT(res.value().totalMatches, 0u);
}
