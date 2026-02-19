// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Migrated from GTest: search_hash_edge_cases_test.cpp
// Tests hash-prefix search edge cases through SearchService.

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

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

namespace {

template <typename T> yams::Result<T> runAwait(boost::asio::awaitable<yams::Result<T>> aw) {
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

struct SearchHashFixture {
    SearchHashFixture() {
        auto pidts = std::to_string(::getpid()) + "_" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        root = std::filesystem::temp_directory_path() / ("search_hash_catch2_" + pidts);
        std::filesystem::create_directories(root);

        database = std::make_unique<Database>();
        auto open = database->open((root / "test.db").string(), ConnectionMode::Create);
        REQUIRE(open);
        pool =
            std::make_unique<ConnectionPool>((root / "test.db").string(), ConnectionPoolConfig{});
        repo = std::make_shared<MetadataRepository>(*pool);
        MigrationManager mm(*database);
        REQUIRE(mm.initialize());
        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        REQUIRE(mm.migrate());

        ContentStoreBuilder b;
        auto storeRes = b.withStoragePath(root / "storage")
                            .withCompression(false)
                            .withDeduplication(true)
                            .build();
        REQUIRE(storeRes);
        store = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(storeRes.value()).release());

        ctx.store = store;
        ctx.metadataRepo = repo;
        search = makeSearchService(ctx);
        auto ds = makeDocumentService(ctx);

        yams::test::FixtureManager fm(root / "fixtures");
        yams::test::TestDataGenerator gen(7);
        for (int i = 0; i < 3; ++i) {
            std::string name = std::string("hdoc_") + std::to_string(i) + ".txt";
            auto fx = fm.createTextFixture(name, gen.generateTextDocument(512, "hash"), {"hash"});
            StoreDocumentRequest req;
            req.path = fx.path.string();
            auto sres = ds->store(req);
            REQUIRE(sres);
            hashes.push_back(sres.value().hash);
            paths.push_back(fx.path.string());
        }
    }

    ~SearchHashFixture() {
        // Reset services/context BEFORE closing DB / shutting down pool
        search.reset();
        ctx.searchEngine.reset();
        ctx.store.reset();
        ctx.metadataRepo.reset();
        store.reset();
        repo.reset();

        if (database) {
            database->close();
            database.reset();
        }
        if (pool) {
            pool->shutdown();
            pool.reset();
        }
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }

    std::filesystem::path root;
    std::unique_ptr<Database> database;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    std::shared_ptr<IContentStore> store;
    AppContext ctx;
    std::shared_ptr<ISearchService> search;
    std::vector<std::string> hashes;
    std::vector<std::string> paths;
};

} // namespace

TEST_CASE("Hash edge cases: 8-char prefix paths-only mixed case",
          "[unit][services][search][hash]") {
    SearchHashFixture f;
    REQUIRE_FALSE(f.hashes.empty());
    auto h = f.hashes.front();
    REQUIRE(h.size() >= 8);
    std::string prefix = h.substr(0, 8);
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (i % 2 == 0)
            prefix[i] = static_cast<char>(::toupper(prefix[i]));
    }
    SearchRequest rq;
    rq.query = prefix;
    rq.pathsOnly = true;
    rq.limit = 10;
    auto r = runAwait(f.search->search(rq));
    REQUIRE(r);
    CHECK(r.value().type == "hash");
    CHECK_FALSE(r.value().paths.empty());
}

TEST_CASE("Hash edge cases: too-short prefix falls back", "[unit][services][search][hash]") {
    SearchHashFixture f;
    REQUIRE_FALSE(f.hashes.empty());
    auto h = f.hashes.front();
    std::string shortp = h.substr(0, 7);
    SearchRequest rq;
    rq.query = shortp;
    rq.limit = 5;
    auto r = runAwait(f.search->search(rq));
    REQUIRE(r);
    CHECK(r.value().type != std::string("hash"));
}

TEST_CASE("Hash edge cases: ambiguity no crash", "[unit][services][search][hash]") {
    SearchHashFixture f;
    REQUIRE(f.hashes.size() >= 2);
    std::string prefix = f.hashes.front().substr(0, 8);
    SearchRequest rq;
    rq.query = prefix;
    rq.limit = 1;
    rq.pathsOnly = false;
    auto r = runAwait(f.search->search(rq));
    REQUIRE(r);
    CHECK(r.value().type == "hash");
    CHECK(r.value().total >= 0);
}

TEST_CASE("Hash edge cases: direct hash prefix lookup via MetadataRepository",
          "[unit][services][search][hash]") {
    SearchHashFixture f;
    REQUIRE_FALSE(f.hashes.empty());
    auto prefix = f.hashes.front().substr(0, 10);
    auto lookup = f.repo->findDocumentsByHashPrefix(prefix, 5);
    REQUIRE(lookup);
    REQUIRE_FALSE(lookup.value().empty());
    CHECK(lookup.value().front().sha256Hash == f.hashes.front());
}
