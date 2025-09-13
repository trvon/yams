#include <gtest/gtest.h>
#include <yams/vector/embedding_generator.h>

#include <cstdlib>
#include <filesystem>
#include <string>

using namespace yams::vector;
namespace fs = std::filesystem;

static bool has_model(std::string* out_minilm = nullptr) {
    const char* home = std::getenv("HOME");
    if (!home)
        return false;
    fs::path p = fs::path(home) / ".yams" / "models" / "all-MiniLM-L6-v2" / "model.onnx";
    if (out_minilm)
        *out_minilm = p.string();
    return fs::exists(p);
}

TEST(EmbeddingGenAI, UsesFallbackWhenGenAIUnavailable) {
#ifndef YAMS_USE_ONNX_RUNTIME
    GTEST_SKIP() << "ONNX Runtime disabled in this build";
#endif
    std::string minilm;
    if (!has_model(&minilm)) {
        GTEST_SKIP() << "MiniLM model not found under ~/.yams/models; skipping";
    }
    // Request GenAI via env and config; adapter is a stub, so it should fall back to ORT path.
    ::setenv("YAMS_ONNX_USE_GENAI", "1", 1);

    EmbeddingConfig cfg;
    cfg.backend = EmbeddingConfig::Backend::Local;
    cfg.model_name = "all-MiniLM-L6-v2";
    cfg.model_path = minilm;
    cfg.embedding_dim = 384;
    cfg.use_genai = true;

    EmbeddingGenerator gen(cfg);
    ASSERT_TRUE(gen.initialize()) << gen.getLastError();
    auto v = gen.generateEmbedding("hello world");
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(v.size(), gen.getEmbeddingDimension());
}
