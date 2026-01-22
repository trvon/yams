// SPDX-License-Identifier: Apache-2.0
// Unit tests for OnnxColbertSession with mock mode support

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/daemon/resource/onnx_colbert_session.h>

#include <cmath>
#include <cstdlib>

namespace yams::daemon {
namespace {

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
    std::string originalValue_{};
    bool hadValue_{false};
};

} // namespace

TEST_CASE("OnnxColbertSession: Mock mode basic behavior", "[daemon][colbert][mock]") {
    EnvGuard testModeGuard("YAMS_TEST_MODE", "1");

    ColbertConfig config;
    config.model_path = "/tmp/fake_colbert.onnx";
    config.model_name = "test-colbert";
    config.max_sequence_length = 64;
    config.token_dim = 8;
    config.query_marker_id = 11;
    config.doc_marker_id = 22;

    OnnxColbertSession session(config.model_path, config.model_name, config);

    SECTION("Session reports valid in mock mode") {
        REQUIRE(session.isValid());
        REQUIRE(session.getName() == "test-colbert");
        REQUIRE(session.getMaxSequenceLength() == 64);
        REQUIRE(session.getTokenDim() == 8);
    }

    SECTION("encodeQuery and encodeDocument include marker tokens") {
        auto query = session.encodeQuery("hello");
        REQUIRE(query.has_value());
        REQUIRE_FALSE(query.value().token_ids.empty());
        REQUIRE(query.value().token_ids.front() == 11);

        auto doc = session.encodeDocument("world");
        REQUIRE(doc.has_value());
        REQUIRE_FALSE(doc.value().token_ids.empty());
        REQUIRE(doc.value().token_ids.front() == 22);
    }

    SECTION("computeMaxSim returns finite score") {
        auto query = session.encodeQuery("hello");
        auto doc = session.encodeDocument("hello world");
        REQUIRE(query.has_value());
        REQUIRE(doc.has_value());

        float score = session.computeMaxSim(query.value(), doc.value());
        CHECK(std::isfinite(score));
    }

    SECTION("encodeDocumentEmbedding returns normalized max-pooled vector") {
        auto embedding = session.encodeDocumentEmbedding("hello world");
        REQUIRE(embedding.has_value());
        REQUIRE(embedding.value().size() == 8);

        double norm = 0.0;
        for (float v : embedding.value()) {
            norm += static_cast<double>(v) * static_cast<double>(v);
        }
        norm = std::sqrt(norm);
        CHECK_THAT(norm, Catch::Matchers::WithinAbs(1.0, 1e-4));
    }
}

} // namespace yams::daemon
