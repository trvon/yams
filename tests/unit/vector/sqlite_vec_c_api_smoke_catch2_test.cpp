// Catch2 tests for sqlite-vec C API smoke tests
// Migrated from GTest: sqlite_vec_c_api_smoke_test.cpp

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <cmath>
#include <cstddef>
#include <vector>

extern "C" {
int sqlite3_vec_distance_l2(const void* vec1, size_t size1, const void* vec2, size_t size2,
                            float* result);
int sqlite3_vec_distance_cosine(const void* vec1, size_t size1, const void* vec2, size_t size2,
                                float* result);
}

using Catch::Matchers::WithinAbs;

TEST_CASE("SqliteVecCApi L2 distance calculations", "[vector][sqlite-vec][c-api][catch2]") {
    SECTION("identical vectors have zero distance") {
        std::vector<float> a(16, 1.0f);
        std::vector<float> b(16, 1.0f);
        float out = -1.0f;

        int rc = sqlite3_vec_distance_l2(a.data(), a.size() * sizeof(float), b.data(),
                                         b.size() * sizeof(float), &out);
        REQUIRE(rc == 0);
        CHECK_THAT(out, WithinAbs(0.0f, 1e-6f));
    }

    SECTION("simple case calculates correct L2 distance") {
        std::vector<float> a = {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8};
        std::vector<float> b = {1, 2, 3, 4, 5, 6, 7, 8, 2, 3, 4, 5, 6, 7, 8, 9};
        float out = -1.0f;

        int rc = sqlite3_vec_distance_l2(a.data(), a.size() * sizeof(float), b.data(),
                                         b.size() * sizeof(float), &out);
        REQUIRE(rc == 0);
        // Difference only in last 8 elements: each diff = 1 => sum=8, sqrt=~2.828427
        CHECK_THAT(out, WithinAbs(std::sqrt(8.0f), 1e-5f));
    }
}

TEST_CASE("SqliteVecCApi cosine distance calculations", "[vector][sqlite-vec][c-api][catch2]") {
    SECTION("identical vectors have near-zero cosine distance") {
        std::vector<float> a(16, 0.5f);
        std::vector<float> b(16, 0.5f);
        float out = -1.0f;

        int rc = sqlite3_vec_distance_cosine(a.data(), a.size() * sizeof(float), b.data(),
                                             b.size() * sizeof(float), &out);
        REQUIRE(rc == 0);
        CHECK_THAT(out, WithinAbs(0.0f, 1e-5f));
    }
}

TEST_CASE("SqliteVecCApi dimension mismatch errors", "[vector][sqlite-vec][c-api][catch2]") {
    std::vector<float> a(8, 1.0f);
    std::vector<float> b(16, 1.0f);
    float out = -1.0f;

    int rc1 = sqlite3_vec_distance_l2(a.data(), a.size() * sizeof(float), b.data(),
                                      b.size() * sizeof(float), &out);
    int rc2 = sqlite3_vec_distance_cosine(a.data(), a.size() * sizeof(float), b.data(),
                                          b.size() * sizeof(float), &out);

    CHECK(rc1 != 0);
    CHECK(rc2 != 0);
}
