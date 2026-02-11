#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <yams/vector/embedding_service.h>
#include <yams/vector/embedding_service_test_hooks.h>

#include "../../common/test_helpers_catch2.h"

using Catch::Matchers::ContainsSubstring;

namespace yams::vector {

TEST_CASE("EmbeddingService returns NotFound when no models are available",
          "[vector][embedding][service][synthetic]") {
    const auto tempDir = yams::test::make_temp_dir("embed_service_no_models_");
    EmbeddingService svc(nullptr, nullptr, tempDir);

    auto result = svc.generateEmbeddingsForDocuments({"doc-a"});
    REQUIRE_FALSE(result);
    CHECK(result.error().code == ErrorCode::NotFound);
    CHECK(result.error().message == "No embedding models available");
}

TEST_CASE("EmbeddingService fails fast on model/db dimension mismatch",
          "[vector][embedding][service][synthetic]") {
    const auto tempDir = yams::test::make_temp_dir("embed_service_dim_mismatch_");

    // Make one discoverable model with MiniLM heuristic (384 dim).
    yams::test::write_file(tempDir / "models" / "all-MiniLM-L6-v2" / "model.onnx", "stub");

    // Force DB target dimension to conflict with runtime provider dimension.
    // Synthetic tests may use either mock or daemon-backed providers; choose a sentinel value
    // that does not match common embedding dimensions.
    yams::test::write_file(tempDir / "vectors_sentinel.json", R"({"embedding_dim":1536})");

    yams::test::ScopedEnvVar inDaemon{"YAMS_IN_DAEMON", std::string{"1"}};
    yams::test::ScopedEnvVar mockProvider{"YAMS_USE_MOCK_PROVIDER", std::string{"1"}};

    EmbeddingService svc(nullptr, nullptr, tempDir);
    auto result = svc.generateEmbeddingsForDocuments({});

    REQUIRE_FALSE(result);
    CHECK(result.error().code == ErrorCode::InvalidState);
    CHECK_THAT(result.error().message, ContainsSubstring("Embedding dimension mismatch"));
    CHECK_THAT(result.error().message, ContainsSubstring("DB expects 1536"));
}

TEST_CASE("Adaptive split recovers when only singleton batches succeed",
          "[vector][embedding][service][synthetic]") {
    int calls = 0;
    auto generator = [&](const std::vector<std::string>& batch) {
        ++calls;
        if (batch.size() > 1) {
            return std::vector<std::vector<float>>{};
        }
        return std::vector<std::vector<float>>{{static_cast<float>(batch[0].size())}};
    };

    const std::vector<std::string> texts{"a", "bb", "ccc", "dddd"};
    auto out = testing::generateEmbeddingsAdaptiveSplit(texts, generator);

    REQUIRE(out.size() == texts.size());
    CHECK(out[0][0] == 1.0f);
    CHECK(out[3][0] == 4.0f);
    CHECK(calls > 1);
}

TEST_CASE("Adaptive split returns empty when all retries fail",
          "[vector][embedding][service][synthetic]") {
    auto alwaysFail = [](const std::vector<std::string>&) {
        return std::vector<std::vector<float>>{};
    };

    auto out = testing::generateEmbeddingsAdaptiveSplit({"x", "y", "z"}, alwaysFail);
    CHECK(out.empty());
}

} // namespace yams::vector
