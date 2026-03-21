// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>
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
const char* yams_onnx_get_health_json_cstr();
#ifdef YAMS_TESTING
void yams_onnx_test_get_pool_config(std::size_t* max_loaded, std::size_t* hot_pool);
void yams_onnx_test_reset_runtime_loader();
#endif
}

namespace yams::daemon::test {

struct OnnxProviderConfigGuard {
    OnnxProviderConfigGuard() {
        setenv("YAMS_TEST_MODE", "1", 1);
#ifdef YAMS_TESTING
        yams_onnx_test_reset_runtime_loader();
#endif
    }

    ~OnnxProviderConfigGuard() {
        yams_onnx_shutdown_provider();
        yams_onnx_set_config_json("{}");
        unsetenv("YAMS_TEST_MODE");
#ifdef YAMS_TESTING
        unsetenv("YAMS_ONNX_RUNTIME_LIB");
        yams_onnx_test_reset_runtime_loader();
#endif
    }
};

TEST_CASE("ONNX plugin: JSON config sets pool sizing", "[daemon]") {
#ifndef YAMS_TESTING
    SUCCEED("Test hooks unavailable without YAMS_TESTING");
#else
    OnnxProviderConfigGuard guard;

    yams_onnx_set_config_json("{\"max_loaded_models\": 7, \"hot_pool_size\": 3, "
                              "\"preferred_model\": \"mxbai-edge-colbert-v0-17m\"}");
    auto* provider = yams_onnx_get_model_provider();
    REQUIRE(provider != nullptr);

    std::size_t max_loaded = 0;
    std::size_t hot_pool = 0;
    yams_onnx_test_get_pool_config(&max_loaded, &hot_pool);

    CHECK(max_loaded == 7);
    CHECK(hot_pool == 3);

    std::vector<const char*> docs = {"first doc", "second doc"};
    float* scores = nullptr;
    size_t count = 0;
    auto rc = provider->score_documents(provider->self, nullptr, "query", docs.data(), docs.size(),
                                        &scores, &count);
    REQUIRE(rc == YAMS_OK);
    REQUIRE(scores != nullptr);
    REQUIRE(count == docs.size());
    provider->free_scores(provider->self, scores, count);
#endif
}

TEST_CASE("ONNX plugin: reports missing runtime without crashing", "[daemon]") {
#ifndef YAMS_TESTING
    SUCCEED("Test hooks unavailable without YAMS_TESTING");
#else
    OnnxProviderConfigGuard guard;

    setenv("YAMS_ONNX_RUNTIME_LIB", "/nonexistent/libonnxruntime.dylib", 1);
    yams_onnx_test_reset_runtime_loader();

    auto* provider = yams_onnx_get_model_provider();
    REQUIRE(provider != nullptr);

    const char* healthJson = yams_onnx_get_health_json_cstr();
    REQUIRE(healthJson != nullptr);

    auto health = nlohmann::json::parse(healthJson, nullptr, false);
    REQUIRE_FALSE(health.is_discarded());
    CHECK(health.value("status", "") == "unavailable");
    CHECK((health.value("reason", "") == "runtime_load_failed" ||
           health.value("reason", "") == "runtime_not_found"));
    CHECK_FALSE(health.value("runtime_status", "").empty());
    CHECK_FALSE(health.value("runtime_error", "").empty());

    bool loaded = true;
    CHECK(provider->is_model_loaded(provider->self, "e5-small", &loaded) == YAMS_ERR_INTERNAL);
    CHECK_FALSE(loaded);
#endif
}

} // namespace yams::daemon::test
