#include <catch2/catch_test_macros.hpp>

#include <sqlite3.h>

#include <atomic>
#include <future>
#include <string_view>
#include <thread>
#include <vector>

#include <yams/search/symspell_search.h>

TEST_CASE("SymSpellSearch serializes SQLite-backed readers",
          "[search][symspell][concurrency][catch2]") {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open(":memory:", &db) == SQLITE_OK);

    {
        const auto schema = yams::search::SymSpellSearch::initializeSchema(db);
        REQUIRE(schema.has_value());

        yams::search::SymSpellSearch index(db);
        REQUIRE(index.addTermsBatch({{"alpha", 100}, {"beta", 80}, {"gamma", 60}}).has_value());

        constexpr std::size_t kThreadCount = 8;
        constexpr std::size_t kIterations = 256;
        std::promise<void> startPromise;
        const auto start = startPromise.get_future().share();
        std::atomic<std::size_t> ready{0};
        std::atomic<bool> failed{false};
        std::vector<std::thread> readers;
        readers.reserve(kThreadCount);

        for (std::size_t threadIndex = 0; threadIndex < kThreadCount; ++threadIndex) {
            readers.emplace_back([&, threadIndex] {
                ready.fetch_add(1, std::memory_order_release);
                start.wait();

                for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
                    const std::string_view expected =
                        (threadIndex + iteration) % 2 == 0 ? "alpha" : "beta";
                    const auto results = index.search(std::string(expected));
                    if (results.empty() || results.front().term != expected ||
                        !index.hasExactMatch(expected)) {
                        failed.store(true, std::memory_order_relaxed);
                        break;
                    }
                }
            });
        }

        while (ready.load(std::memory_order_acquire) != kThreadCount) {
            std::this_thread::yield();
        }
        startPromise.set_value();
        for (auto& reader : readers) {
            reader.join();
        }

        CHECK_FALSE(failed.load(std::memory_order_relaxed));
    }

    CHECK(sqlite3_close(db) == SQLITE_OK);
}

TEST_CASE("SymSpellSearch reports rejected SQLite writes",
          "[search][symspell][persistence][errors][catch2]") {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open(":memory:", &db) == SQLITE_OK);

    {
        REQUIRE(yams::search::SymSpellSearch::initializeSchema(db).has_value());
        char* error = nullptr;
        REQUIRE(sqlite3_exec(db,
                             "CREATE TRIGGER reject_symspell_term BEFORE INSERT ON symspell_terms "
                             "BEGIN SELECT RAISE(ABORT, 'forced term failure'); END",
                             nullptr, nullptr, &error) == SQLITE_OK);
        sqlite3_free(error);

        yams::search::SymSpellSearch index(db);
        SECTION("single term") {
            CHECK_FALSE(index.addTerm("alpha", 100).has_value());
        }
        SECTION("transactional batch") {
            CHECK_FALSE(index.addTermsBatch({{"alpha", 100}, {"beta", 80}}).has_value());
            CHECK_FALSE(index.hasExactMatch("beta"));
        }
        CHECK_FALSE(index.hasExactMatch("alpha"));
    }

    CHECK(sqlite3_close(db) == SQLITE_OK);
}

TEST_CASE("SymSpellSearch clear removes terms", "[search][symspell][persistence][catch2]") {
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open(":memory:", &db) == SQLITE_OK);

    {
        REQUIRE(yams::search::SymSpellSearch::initializeSchema(db).has_value());
        yams::search::SymSpellSearch index(db);
        REQUIRE(index.addTerm("alpha", 100).has_value());
        REQUIRE(index.hasExactMatch("alpha"));

        REQUIRE(index.clear().has_value());
        CHECK_FALSE(index.hasExactMatch("alpha"));
    }

    CHECK(sqlite3_close(db) == SQLITE_OK);
}
