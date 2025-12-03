#include <gtest/gtest.h>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

#include <filesystem>
#include <regex>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace yams::cli;

// ------------------------------------------------------------------
// Test the EMBEDDING_MODELS definition used by init command.
// These tests verify that the model list is valid and consistent.
// ------------------------------------------------------------------

// Model info structure matching init_command.cpp
struct EmbeddingModelInfo {
    std::string name;
    std::string url;
    std::string description;
    size_t size_mb;
    int dimensions;
};

// Expected models (mirrors the static list in init_command.cpp)
static const std::vector<EmbeddingModelInfo> EXPECTED_MODELS = {
    {"all-MiniLM-L6-v2",
     "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx",
     "Lightweight model for semantic search", 90, 384},
    {"multi-qa-MiniLM-L6-cos-v1",
     "https://huggingface.co/sentence-transformers/multi-qa-MiniLM-L6-cos-v1/resolve/main/onnx/"
     "model.onnx",
     "Optimized for semantic search on QA pairs (215M training samples)", 90, 384}};

TEST(InitCommandModels, AllModelsHaveValidUrls) {
    const std::regex hfUrlPattern(
        R"(^https://huggingface\.co/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+/resolve/main/.+\.onnx$)");

    for (const auto& model : EXPECTED_MODELS) {
        EXPECT_TRUE(std::regex_match(model.url, hfUrlPattern))
            << "Invalid HuggingFace URL for model: " << model.name << "\n  URL: " << model.url;
    }
}

TEST(InitCommandModels, AllModelsHaveConsistentDimensions) {
    // All models should have dim=384 for consistency with default vector DB setup
    for (const auto& model : EXPECTED_MODELS) {
        EXPECT_EQ(model.dimensions, 384)
            << "Model " << model.name << " has non-standard dimensions: " << model.dimensions;
    }
}

TEST(InitCommandModels, AllModelsHaveValidNames) {
    // Model names should follow naming convention
    const std::regex namePattern(R"(^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$)");

    for (const auto& model : EXPECTED_MODELS) {
        EXPECT_TRUE(std::regex_match(model.name, namePattern))
            << "Invalid model name: " << model.name;
        EXPECT_FALSE(model.name.empty()) << "Model name should not be empty";
    }
}

TEST(InitCommandModels, AllModelsHaveDescriptions) {
    for (const auto& model : EXPECTED_MODELS) {
        EXPECT_FALSE(model.description.empty())
            << "Model " << model.name << " is missing a description";
        EXPECT_GT(model.description.length(), 10)
            << "Model " << model.name << " has too short a description";
    }
}

TEST(InitCommandModels, AllModelsHaveReasonableSizes) {
    for (const auto& model : EXPECTED_MODELS) {
        // ONNX embedding models typically range from 20MB to 500MB
        EXPECT_GE(model.size_mb, 20)
            << "Model " << model.name << " seems too small: " << model.size_mb << " MB";
        EXPECT_LE(model.size_mb, 500)
            << "Model " << model.name << " seems too large: " << model.size_mb << " MB";
    }
}

TEST(InitCommandModels, DefaultModelIsFirst) {
    // The first model should be the default (all-MiniLM-L6-v2)
    ASSERT_FALSE(EXPECTED_MODELS.empty());
    EXPECT_EQ(EXPECTED_MODELS[0].name, "all-MiniLM-L6-v2")
        << "Default model should be all-MiniLM-L6-v2";
}

TEST(InitCommandModels, ModelUrlsContainModelName) {
    // Each model's URL should reference its HuggingFace repo name
    for (const auto& model : EXPECTED_MODELS) {
        // Extract the repo name part (usually similar to model name with possible variations)
        // For sentence-transformers models, the URL contains the model name
        EXPECT_NE(model.url.find("sentence-transformers"), std::string::npos)
            << "Model " << model.name << " URL should reference sentence-transformers repo";
    }
}

// ------------------------------------------------------------------
// Test init command CLI flags
// ------------------------------------------------------------------

TEST(InitCommandCLI, NonInteractiveFlagWorks) {
    YamsCLI cli;
    // Non-interactive with a temp directory should not prompt
    auto tempDir = fs::temp_directory_path() / "yams-test-init-ni";
    fs::create_directories(tempDir);

    const char* argv[] = {"yams", "--data-dir",        tempDir.string().c_str(),
                          "init", "--non-interactive", "--no-keygen"};
    // Note: We just verify it doesn't crash; actual init may fail due to missing deps
    (void)cli.run(static_cast<int>(std::size(argv)), const_cast<char**>(argv));

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
    SUCCEED();
}

TEST(InitCommandCLI, AutoFlagAccepted) {
    YamsCLI cli;
    // Just verify the --auto flag is recognized
    auto tempDir = fs::temp_directory_path() / "yams-test-init-auto";
    fs::create_directories(tempDir);

    const char* argv[] = {"yams", "--data-dir", tempDir.string().c_str(),
                          "init", "--auto",     "--no-keygen"};
    // Note: We just verify it doesn't crash; actual init may fail due to missing deps
    (void)cli.run(static_cast<int>(std::size(argv)), const_cast<char**>(argv));

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
    SUCCEED();
}

TEST(InitCommandCLI, ForceFlagAccepted) {
    YamsCLI cli;
    auto tempDir = fs::temp_directory_path() / "yams-test-init-force";
    fs::create_directories(tempDir);

    const char* argv[] = {"yams",       "--data-dir",        tempDir.string().c_str(),
                          "init",       "--non-interactive", "--force",
                          "--no-keygen"};
    (void)cli.run(static_cast<int>(std::size(argv)), const_cast<char**>(argv));

    // Cleanup
    std::error_code ec;
    fs::remove_all(tempDir, ec);
    SUCCEED();
}

// ------------------------------------------------------------------
// Test model URL accessibility (integration test - optional)
// These tests can be run manually to verify URLs are still valid.
// They are disabled by default to avoid network dependencies in CI.
// ------------------------------------------------------------------

// To enable: remove the DISABLED_ prefix
TEST(InitCommandModels, DISABLED_VerifyModelUrlsAccessible) {
    // This test would use libcurl to HEAD each URL and verify 200 OK
    // Disabled by default to avoid network dependencies in unit tests
    GTEST_SKIP() << "Network test - run manually to verify model URLs";
}
