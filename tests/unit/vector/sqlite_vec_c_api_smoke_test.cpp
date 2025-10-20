#include <cmath>
#include <cstddef>
#include <vector>
#include <gtest/gtest.h>

extern "C" {
int sqlite3_vec_distance_l2(const void* vec1, size_t size1, const void* vec2, size_t size2,
                            float* result);
int sqlite3_vec_distance_cosine(const void* vec1, size_t size1, const void* vec2, size_t size2,
                                float* result);
}

namespace {

TEST(SqliteVecCApiSmokeTest, L2DistanceIdenticalIsZero) {
    std::vector<float> a(16, 1.0f);
    std::vector<float> b(16, 1.0f);
    float out = -1.0f;
    int rc = sqlite3_vec_distance_l2(a.data(), a.size() * sizeof(float), b.data(),
                                     b.size() * sizeof(float), &out);
    ASSERT_EQ(rc, 0);
    EXPECT_NEAR(out, 0.0f, 1e-6f);
}

TEST(SqliteVecCApiSmokeTest, L2DistanceSimpleCase) {
    std::vector<float> a = {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<float> b = {1, 2, 3, 4, 5, 6, 7, 8, 2, 3, 4, 5, 6, 7, 8, 9};
    float out = -1.0f;
    int rc = sqlite3_vec_distance_l2(a.data(), a.size() * sizeof(float), b.data(),
                                     b.size() * sizeof(float), &out);
    ASSERT_EQ(rc, 0);
    // difference only in last 8 elements: each diff = 1 => sum=8, sqrt=~2.828427
    EXPECT_NEAR(out, std::sqrt(8.0f), 1e-5f);
}

TEST(SqliteVecCApiSmokeTest, CosineDistanceIdenticalNearZero) {
    std::vector<float> a(16, 0.5f);
    std::vector<float> b(16, 0.5f);
    float out = -1.0f;
    int rc = sqlite3_vec_distance_cosine(a.data(), a.size() * sizeof(float), b.data(),
                                         b.size() * sizeof(float), &out);
    ASSERT_EQ(rc, 0);
    EXPECT_NEAR(out, 0.0f, 1e-5f);
}

TEST(SqliteVecCApiSmokeTest, DimensionMismatchErrors) {
    std::vector<float> a(8, 1.0f);
    std::vector<float> b(16, 1.0f);
    float out = -1.0f;
    int rc1 = sqlite3_vec_distance_l2(a.data(), a.size() * sizeof(float), b.data(),
                                      b.size() * sizeof(float), &out);
    int rc2 = sqlite3_vec_distance_cosine(a.data(), a.size() * sizeof(float), b.data(),
                                          b.size() * sizeof(float), &out);
    EXPECT_NE(rc1, 0);
    EXPECT_NE(rc2, 0);
}

} // namespace
