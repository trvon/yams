#include <catch2/catch_test_macros.hpp>

#include <yams/vector/static_cosine_ann_index.h>

#include <array>
#include <atomic>
#include <thread>
#include <vector>

using yams::vector::StaticCosineAnnIndex;

TEST_CASE("Static cosine ANN preserves external IDs and reports traversal work",
          "[unit][vector][ann][routing]") {
    const std::array<std::size_t, 4> ids{10, 20, 30, 40};
    const std::array<std::vector<float>, 4> vectors{
        std::vector<float>{1.0F, 0.0F},
        std::vector<float>{0.0F, 1.0F},
        std::vector<float>{-1.0F, 0.0F},
        std::vector<float>{0.0F, -1.0F},
    };

    auto built = StaticCosineAnnIndex::build(ids, vectors);
    REQUIRE(built.has_value());
    REQUIRE(built.value());
    CHECK(built.value()->size() == ids.size());
    CHECK(built.value()->dimension() == 2U);

    const std::array<float, 2> query{0.05F, 1.0F};
    auto result = built.value()->search(query, 2);
    REQUIRE(result.has_value());
    REQUIRE(result.value().hits.size() == 2U);
    CHECK(result.value().hits.front().id == 20U);
    CHECK(result.value().distanceEvaluations > 0U);
}

TEST_CASE("Static cosine ANN rejects inconsistent dimensions", "[unit][vector][ann][routing]") {
    const std::array<std::size_t, 2> ids{1, 2};
    const std::array<std::vector<float>, 2> vectors{
        std::vector<float>{1.0F, 0.0F},
        std::vector<float>{0.0F, 1.0F, 0.0F},
    };

    const auto built = StaticCosineAnnIndex::build(ids, vectors);
    REQUIRE_FALSE(built.has_value());
    CHECK(built.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE("Static cosine ANN supports concurrent immutable searches",
          "[unit][vector][ann][routing][concurrency]") {
    const std::array<std::size_t, 4> ids{10, 20, 30, 40};
    const std::array<std::vector<float>, 4> vectors{
        std::vector<float>{1.0F, 0.0F},
        std::vector<float>{0.0F, 1.0F},
        std::vector<float>{-1.0F, 0.0F},
        std::vector<float>{0.0F, -1.0F},
    };
    auto built = StaticCosineAnnIndex::build(ids, vectors);
    REQUIRE(built.has_value());
    const auto index = built.value();

    std::atomic<std::size_t> failures{0};
    std::vector<std::thread> readers;
    for (std::size_t reader = 0; reader < 8; ++reader) {
        readers.emplace_back([&, reader, index] {
            const std::array<float, 2> query = reader % 2 == 0 ? std::array<float, 2>{1.0F, 0.05F}
                                                               : std::array<float, 2>{0.05F, 1.0F};
            const std::size_t expectedId = reader % 2 == 0 ? 10U : 20U;
            for (std::size_t iteration = 0; iteration < 100; ++iteration) {
                auto result = index->search(query, 2);
                if (!result || result.value().hits.size() != 2U ||
                    result.value().hits.front().id != expectedId ||
                    result.value().distanceEvaluations == 0U) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& reader : readers) {
        reader.join();
    }
    CHECK(failures.load(std::memory_order_relaxed) == 0U);
}
