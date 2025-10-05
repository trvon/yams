#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

#include "common/fixture_manager.h"
#include "common/test_data_generator.h"

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::api;

template <typename T> static yams::Result<T> runAwait(boost::asio::awaitable<yams::Result<T>> aw) {
    boost::asio::io_context ioc;
    auto wrapper =
        [aw = std::move(aw)]() mutable -> boost::asio::awaitable<std::optional<yams::Result<T>>> {
        try {
            co_return std::optional<yams::Result<T>>(co_await std::move(aw));
        } catch (...) {
            co_return std::optional<yams::Result<T>>{};
        }
    };
    auto fut = boost::asio::co_spawn(ioc, wrapper(), boost::asio::use_future);
    ioc.run();
    auto opt = fut.get();
    if (opt)
        return std::move(*opt);
    return yams::Result<T>(yams::Error{yams::ErrorCode::InternalError, "Awaitable failed"});
}

class SearchHashEdgeCasesTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto pidts = std::to_string(::getpid()) + "_" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root_ = std::filesystem::temp_directory_path() / ("search_hash_" + pidts);
        std::filesystem::create_directories(root_);

        // DB and repo
        database_ = std::make_unique<Database>();
        auto open = database_->open((root_ / "test.db").string(), ConnectionMode::Create);
        ASSERT_TRUE(open);
        pool_ =
            std::make_unique<ConnectionPool>((root_ / "test.db").string(), ConnectionPoolConfig{});
        repo_ = std::make_shared<MetadataRepository>(*pool_);
        MigrationManager mm(*database_);
        ASSERT_TRUE(mm.initialize());
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        ASSERT_TRUE(mm.migrate());

        // Store
        ContentStoreBuilder b;
        auto storeRes = b.withStoragePath(root_ / "storage")
                            .withCompression(false)
                            .withDeduplication(true)
                            .build();
        ASSERT_TRUE(storeRes);
        store_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(storeRes.value()).release());

        // Context/services
        ctx_.store = store_;
        ctx_.metadataRepo = repo_;
        search_ = makeSearchService(ctx_);
        auto ds = makeDocumentService(ctx_);

        // Add a couple of docs and record hashes
        yams::test::FixtureManager fm(root_ / "fixtures");
        yams::test::TestDataGenerator gen(7);
        for (int i = 0; i < 3; ++i) {
            std::string name = std::string("hdoc_") + std::to_string(i) + ".txt";
            auto fx = fm.createTextFixture(name, gen.generateTextDocument(512, "hash"), {"hash"});
            StoreDocumentRequest req;
            req.path = fx.path.string();
            auto sres = ds->store(req);
            ASSERT_TRUE(sres) << sres.error().message;
            hashes_.push_back(sres.value().hash);
            paths_.push_back(fx.path.string());
        }
    }

    void TearDown() override {
        if (database_) {
            database_->close();
            database_.reset();
        }
        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        repo_.reset();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

protected:
    std::filesystem::path root_;
    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> repo_;
    std::shared_ptr<IContentStore> store_;
    AppContext ctx_;
    std::shared_ptr<ISearchService> search_;
    std::vector<std::string> hashes_;
    std::vector<std::string> paths_;
};

TEST_F(SearchHashEdgeCasesTest, HashPrefixEightCharsPathsOnlyMixedCase) {
    ASSERT_FALSE(hashes_.empty());
    auto h = hashes_.front();
    ASSERT_GE(h.size(), 8u);
    std::string prefix = h.substr(0, 8);
    // Mix case artificially
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (i % 2 == 0)
            prefix[i] = static_cast<char>(::toupper(prefix[i]));
    }
    SearchRequest rq;
    rq.query = prefix;
    rq.pathsOnly = true;
    rq.limit = 10;
    auto r = runAwait(search_->search(rq));
    ASSERT_TRUE(r) << r.error().message;
    EXPECT_EQ(r.value().type, "hash");
    EXPECT_FALSE(r.value().paths.empty());
}

TEST_F(SearchHashEdgeCasesTest, HashPrefixTooShortFallsBack) {
    ASSERT_FALSE(hashes_.empty());
    auto h = hashes_.front();
    std::string shortp = h.substr(0, 7); // too short to be hash mode
    SearchRequest rq;
    rq.query = shortp;
    rq.limit = 5;
    auto r = runAwait(search_->search(rq));
    ASSERT_TRUE(r) << r.error().message;
    EXPECT_NE(r.value().type, std::string("hash"));
}

TEST_F(SearchHashEdgeCasesTest, HashPrefixAmbiguityNoCrash) {
    ASSERT_GE(hashes_.size(), 2u);
    // Use a short (but >=8) prefix that might match none or one; ensure no crash and valid type
    std::string prefix = hashes_.front().substr(0, 8);
    SearchRequest rq;
    rq.query = prefix;
    rq.limit = 1;
    rq.pathsOnly = false;
    auto r = runAwait(search_->search(rq));
    ASSERT_TRUE(r) << r.error().message;
    EXPECT_EQ(r.value().type, "hash");
    EXPECT_GE(r.value().total, 0u);
}

TEST_F(SearchHashEdgeCasesTest, MetadataRepositoryDirectHashPrefixLookup) {
    ASSERT_FALSE(hashes_.empty());
    auto prefix = hashes_.front().substr(0, 10);
    auto lookup = repo_->findDocumentsByHashPrefix(prefix, 5);
    ASSERT_TRUE(lookup) << lookup.error().message;
    ASSERT_FALSE(lookup.value().empty());
    EXPECT_EQ(lookup.value().front().sha256Hash, hashes_.front());
}
