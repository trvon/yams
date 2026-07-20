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
        index.addTermsBatch({{"alpha", 100}, {"beta", 80}, {"gamma", 60}});

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
