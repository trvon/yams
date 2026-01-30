// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 Trevon Helm
// Unit tests for OnnxRerankerSession with mock mode support

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/daemon/resource/onnx_reranker_session.h>

#include <cstdlib>
#include <filesystem>

#include <yams/compat/unistd.h>

namespace fs = std::filesystem;

namespace yams::daemon {
namespace {

// RAII environment variable guard
class EnvGuard {
public:
    explicit EnvGuard(const char* name, const char* value = nullptr) : name_(name) {
        if (const char* val = std::getenv(name)) {
            originalValue_ = val;
            hadValue_ = true;
        }
        if (value) {
            ::setenv(name_, value, 1);
        }
    }

    ~EnvGuard() {
        if (hadValue_) {
            ::setenv(name_, originalValue_.c_str(), 1);
        } else {
            ::unsetenv(name_);
        }
    }

private:
    const char* name_;
    std::string originalValue_;
    bool hadValue_{false};
};

} // namespace

// =============================================================================
// Mock Mode Tests (No ONNX model required)
// =============================================================================

TEST_CASE("OnnxRerankerSession: Mock mode basic functionality", "[daemon][reranker][mock]") {
    // Enable mock mode via environment variable
    EnvGuard testModeGuard("YAMS_TEST_MODE", "1");

    RerankerConfig config;
    config.model_path = "/tmp/fake_model.onnx";
    config.model_name = "test-reranker";
    config.max_sequence_length = 512;

    OnnxRerankerSession session(config.model_path, config.model_name, config);

    SECTION("Session is valid in mock mode") {
        REQUIRE(session.isValid());
        REQUIRE(session.getName() == "test-reranker");
        REQUIRE(session.getMaxSequenceLength() == 512);
    }

    SECTION("Score returns word overlap based score") {
        // Query with words present in document should score higher
        auto result1 = session.score("hello world", "hello world test");
        REQUIRE(result1.has_value());
        CHECK(result1.value() > 0.5f);

        // Query with no overlap should score 0
        auto result2 = session.score("apple banana", "car truck boat");
        REQUIRE(result2.has_value());
        CHECK_THAT(result2.value(), Catch::Matchers::WithinAbs(0.0f, 0.001f));

        // Partial overlap
        auto result3 = session.score("hello goodbye", "hello world");
        REQUIRE(result3.has_value());
        CHECK_THAT(result3.value(), Catch::Matchers::WithinAbs(0.5f, 0.001f));
    }

    SECTION("ScoreBatch returns scores for all documents") {
        std::vector<std::string> documents = {"hello world", "goodbye world", "apple orange",
                                              "hello there"};

        auto result = session.scoreBatch("hello", documents);
        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 4);

        // "hello world" and "hello there" should have score 1.0 (full query match)
        CHECK_THAT(result.value()[0], Catch::Matchers::WithinAbs(1.0f, 0.001f)); // "hello world"
        CHECK_THAT(result.value()[3], Catch::Matchers::WithinAbs(1.0f, 0.001f)); // "hello there"

        // "goodbye world" and "apple orange" have no overlap with "hello"
        CHECK_THAT(result.value()[1], Catch::Matchers::WithinAbs(0.0f, 0.001f));
        CHECK_THAT(result.value()[2], Catch::Matchers::WithinAbs(0.0f, 0.001f));
    }

    SECTION("Rerank returns documents sorted by score") {
        std::vector<std::string> documents = {"apple banana", "hello world test", "hello",
                                              "xyz abc"};

        // Use multi-word query to get differentiated scores
        auto result = session.rerank("hello world", documents);
        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 4);

        // Check ordering: highest scores first
        for (size_t i = 1; i < result.value().size(); ++i) {
            CHECK(result.value()[i - 1].score >= result.value()[i].score);
        }

        // "hello world test" (index 1) should be first (both "hello" and "world" match = 1.0)
        CHECK(result.value()[0].document_index == 1);
        CHECK_THAT(result.value()[0].score, Catch::Matchers::WithinAbs(1.0f, 0.001f));

        // "hello" (index 2) should be second (only "hello" matches = 0.5)
        CHECK(result.value()[1].document_index == 2);
        CHECK_THAT(result.value()[1].score, Catch::Matchers::WithinAbs(0.5f, 0.001f));
    }

    SECTION("Rerank with topK limits results") {
        std::vector<std::string> documents = {"a", "b", "c", "d", "e"};

        auto result = session.rerank("test", documents, 3);
        REQUIRE(result.has_value());
        REQUIRE(result.value().size() == 3);
    }
}

TEST_CASE("OnnxRerankerSession: Edge cases", "[daemon][reranker][edge]") {
    EnvGuard testModeGuard("YAMS_TEST_MODE", "1");

    RerankerConfig config;
    config.model_path = "/tmp/fake_model.onnx";
    config.model_name = "edge-test-reranker";

    OnnxRerankerSession session(config.model_path, config.model_name, config);

    SECTION("Empty query returns zero scores") {
        auto result = session.score("", "some document");
        REQUIRE(result.has_value());
        REQUIRE(result.value() == 0.0f);
    }

    SECTION("Empty document returns zero score") {
        auto result = session.score("some query", "");
        REQUIRE(result.has_value());
        REQUIRE(result.value() == 0.0f);
    }

    SECTION("Empty batch returns empty results") {
        std::vector<std::string> emptyDocs;
        auto result = session.scoreBatch("query", emptyDocs);
        REQUIRE(result.has_value());
        REQUIRE(result.value().empty());
    }

    SECTION("Rerank with empty documents returns empty results") {
        std::vector<std::string> emptyDocs;
        auto result = session.rerank("query", emptyDocs);
        REQUIRE(result.has_value());
        REQUIRE(result.value().empty());
    }

    SECTION("Case insensitive scoring") {
        auto result1 = session.score("HELLO", "hello world");
        auto result2 = session.score("hello", "HELLO WORLD");
        REQUIRE(result1.has_value());
        REQUIRE(result2.has_value());
        REQUIRE(result1.value() == result2.value());
    }

    SECTION("Punctuation handling") {
        auto result = session.score("hello, world!", "hello world");
        REQUIRE(result.has_value());
        REQUIRE(result.value() == 1.0f); // Both have "hello" and "world"
    }
}

TEST_CASE("OnnxRerankerSession: Config parsing", "[daemon][reranker][config]") {
    EnvGuard testModeGuard("YAMS_TEST_MODE", "1");

    SECTION("Default max sequence length") {
        RerankerConfig config;
        config.model_path = "/tmp/fake_model.onnx";
        config.model_name = "config-test";
        // Default should be 512

        OnnxRerankerSession session(config.model_path, config.model_name, config);
        REQUIRE(session.getMaxSequenceLength() == 512);
    }

    SECTION("Custom max sequence length") {
        RerankerConfig config;
        config.model_path = "/tmp/fake_model.onnx";
        config.model_name = "config-test";
        config.max_sequence_length = 256;

        OnnxRerankerSession session(config.model_path, config.model_name, config);
        REQUIRE(session.getMaxSequenceLength() == 256);
    }
}

// =============================================================================
// Real Model Tests (Optional - requires ONNX model)
// =============================================================================

TEST_CASE("OnnxRerankerSession: Real model integration",
          "[daemon][reranker][integration][!mayfail]") {
    // Check if real model is available
    const char* modelPath = std::getenv("TEST_RERANKER_MODEL_PATH");
    if (!modelPath || !fs::exists(modelPath)) {
        SKIP("Reranker model not available (set TEST_RERANKER_MODEL_PATH)");
    }

    // Ensure mock mode is not active
    EnvGuard testModeGuard("YAMS_TEST_MODE");      // Unset
    EnvGuard mockGuard("YAMS_USE_MOCK_PROVIDER");  // Unset
    EnvGuard skipGuard("YAMS_SKIP_MODEL_LOADING"); // Unset

    RerankerConfig config;
    config.model_path = modelPath;
    config.model_name = "bge-reranker-test";
    config.max_sequence_length = 512;
    config.num_threads = 2;

    OnnxRerankerSession session(config.model_path, config.model_name, config);

    REQUIRE(session.isValid());

    SECTION("Score relevant vs irrelevant pairs") {
        // Relevant pair should score higher
        auto relevantScore = session.score("what is machine learning",
                                           "Machine learning is a subset of artificial "
                                           "intelligence that enables systems to learn.");
        auto irrelevantScore = session.score(
            "what is machine learning", "The weather today is sunny with a high of 75 degrees.");

        REQUIRE(relevantScore.has_value());
        REQUIRE(irrelevantScore.has_value());
        REQUIRE(relevantScore.value() > irrelevantScore.value());
    }

    SECTION("Rerank puts relevant documents first") {
        std::vector<std::string> documents = {
            "The quick brown fox jumps over the lazy dog.",
            "Python is a popular programming language for data science.",
            "Machine learning algorithms can learn patterns from data.",
            "Today's lunch special is pizza with extra cheese."};

        auto results = session.rerank("What are machine learning algorithms?", documents);
        REQUIRE(results.has_value());

        // The ML document (index 2) should be ranked first
        REQUIRE(results.value()[0].document_index == 2);
    }
}

} // namespace yams::daemon
