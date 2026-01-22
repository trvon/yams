// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <yams/plugins/model_provider_v1.h>

#ifdef _WIN32
static int setenv(const char* name, const char* value, int /*overwrite*/) {
    return _putenv_s(name, value);
}
static void unsetenv(const char* name) {
    _putenv_s(name, "");
}
#endif

extern "C" {
void yams_onnx_set_config_json(const char* json);
void yams_onnx_shutdown_provider();
yams_model_provider_v1* yams_onnx_get_model_provider();
#ifdef YAMS_TESTING
void yams_onnx_test_get_pool_config(std::size_t* max_loaded, std::size_t* hot_pool);
#endif
}

namespace yams::daemon::test {

struct OnnxProviderConfigGuard {
    OnnxProviderConfigGuard() { setenv("YAMS_TEST_MODE", "1", 1); }

    ~OnnxProviderConfigGuard() {
        yams_onnx_shutdown_provider();
        yams_onnx_set_config_json("{}");
        unsetenv("YAMS_TEST_MODE");
    }
};

TEST_CASE("ONNX plugin: JSON config sets pool sizing", "[daemon]") {
#ifndef YAMS_TESTING
    SUCCEED("Test hooks unavailable without YAMS_TESTING");
#else
    OnnxProviderConfigGuard guard;

    yams_onnx_set_config_json("{\"max_loaded_models\": 7, \"hot_pool_size\": 3}");
    auto* provider = yams_onnx_get_model_provider();
    REQUIRE(provider != nullptr);

    std::size_t max_loaded = 0;
    std::size_t hot_pool = 0;
    yams_onnx_test_get_pool_config(&max_loaded, &hot_pool);

    CHECK(max_loaded == 7);
    CHECK(hot_pool == 3);
#endif
}

TEST_CASE("ONNX plugin: JSON config selects ColBERT preferred model", "[daemon]") {
#ifndef YAMS_TESTING
    SUCCEED("Test hooks unavailable without YAMS_TESTING");
#else
    OnnxProviderConfigGuard guard;

    yams_onnx_set_config_json("{\"preferred_model\": \"mxbai-edge-colbert-v0-17m\"}");
    auto* provider = yams_onnx_get_model_provider();
    REQUIRE(provider != nullptr);

    std::vector<const char*> docs = {"first doc", "second doc"};
    float* scores = nullptr;
    size_t count = 0;

    auto rc = provider->score_documents(provider->self, nullptr, "query", docs.data(), docs.size(),
                                        &scores, &count);
    REQUIRE(rc == YAMS_OK);
    REQUIRE(scores != nullptr);
    REQUIRE(count == docs.size());

    provider->free_scores(provider->self, scores, count);

    float* vecs = nullptr;
    size_t out_batch = 0;
    size_t out_dim = 0;
    std::array<const uint8_t*, 2> docPtrs = {
        reinterpret_cast<const uint8_t*>(docs[0]),
        reinterpret_cast<const uint8_t*>(docs[1]),
    };
    std::array<size_t, 2> docLens = {std::strlen(docs[0]), std::strlen(docs[1])};
    auto embRc = provider->generate_embedding_batch(provider->self, "mxbai-edge-colbert-v0-17m",
                                                    docPtrs.data(), docLens.data(), docs.size(),
                                                    &vecs, &out_batch, &out_dim);
    REQUIRE(embRc == YAMS_OK);
    REQUIRE(out_batch == docs.size());
    REQUIRE(out_dim == 48);
    provider->free_embedding_batch(provider->self, vecs, out_batch, out_dim);
#endif
}

} // namespace yams::daemon::test
