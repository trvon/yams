// Catch2 tests for GenAI embedding selection and fallback
// Migrated from GTest: embedding_genai_selection_test.cpp

#include <catch2/catch_test_macros.hpp>
#include <yams/vector/embedding_generator.h>

#include <cstdlib>
#include <filesystem>
#include <string>

using namespace yams::vector;
namespace fs = std::filesystem;

namespace {

bool hasModel(std::string* outMinilm = nullptr) {
    const char* home = std::getenv("HOME");
    if (!home) {
#ifdef _WIN32
        home = std::getenv("USERPROFILE");
        if (!home)
            return false;
#else
        return false;
#endif
    }
    fs::path p = fs::path(home) / ".yams" / "models" / "all-MiniLM-L6-v2" / "model.onnx";
    if (outMinilm)
        *outMinilm = p.string();
    return fs::exists(p);
}

} // namespace

TEST_CASE("EmbeddingGenAI uses fallback when GenAI unavailable",
          "[vector][embedding][genai][catch2]") {
#ifndef YAMS_USE_ONNX_RUNTIME
    SKIP("ONNX Runtime disabled in this build");
#endif

    std::string minilm;
    if (!hasModel(&minilm)) {
        SKIP("MiniLM model not found under ~/.yams/models; skipping");
    }

    // Request GenAI via env; adapter is a stub, so it should fall back to ORT path.
#ifdef _WIN32
    _putenv_s("YAMS_ONNX_USE_GENAI", "1");
#else
    ::setenv("YAMS_ONNX_USE_GENAI", "1", 1);
#endif

    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Local;
    cfg.model_name = "all-MiniLM-L6-v2";
    cfg.model_path = minilm;
    cfg.embedding_dim = 384;
    cfg.use_genai = true;

    EmbeddingGenerator gen(cfg);
    REQUIRE(gen.initialize());
    INFO("Generator error: " << gen.getLastError());

    auto v = gen.generateEmbedding("hello world");
    REQUIRE_FALSE(v.empty());
    CHECK(v.size() == gen.getEmbeddingDimension());
}
