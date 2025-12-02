// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for ConfigResolver component

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/daemon.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

namespace fs = std::filesystem;
using namespace yams::daemon;

namespace yams::daemon::test {

class ConfigResolverTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = fs::temp_directory_path() /
                   ("config_test_" + std::to_string(getpid()) + "_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(testDir_);
        configPath_ = testDir_ / "config.toml";
    }

    void TearDown() override {
        if (fs::exists(testDir_)) {
            std::error_code ec;
            fs::remove_all(testDir_, ec);
        }
    }

    void writeConfig(const std::string& content) {
        std::ofstream out(configPath_);
        out << content;
    }

    fs::path testDir_;
    fs::path configPath_;
};

TEST_F(ConfigResolverTest, ParseSimpleTomlFlat_EmptyFile) {
    writeConfig("");
    auto result = ConfigResolver::parseSimpleTomlFlat(configPath_);
    EXPECT_TRUE(result.empty());
}

TEST_F(ConfigResolverTest, ParseSimpleTomlFlat_SimpleKeyValue) {
    writeConfig("key = \"value\"\n");
    auto result = ConfigResolver::parseSimpleTomlFlat(configPath_);
    EXPECT_EQ(result["key"], "value");
}

TEST_F(ConfigResolverTest, ParseSimpleTomlFlat_SectionedKeyValue) {
    writeConfig("[embeddings]\npreferred_model = \"all-MiniLM-L6-v2\"\n");
    auto result = ConfigResolver::parseSimpleTomlFlat(configPath_);
    EXPECT_EQ(result["embeddings.preferred_model"], "all-MiniLM-L6-v2");
}

TEST_F(ConfigResolverTest, ParseSimpleTomlFlat_MultipleValues) {
    writeConfig(R"(
[embeddings]
preferred_model = "test-model"
embedding_dim = 384

[vector_database]
max_elements = 50000
)");
    auto result = ConfigResolver::parseSimpleTomlFlat(configPath_);
    EXPECT_EQ(result["embeddings.preferred_model"], "test-model");
    EXPECT_EQ(result["embeddings.embedding_dim"], "384");
    EXPECT_EQ(result["vector_database.max_elements"], "50000");
}

TEST_F(ConfigResolverTest, ParseSimpleTomlFlat_SkipsComments) {
    writeConfig(R"(
# This is a comment
key = "value"
# Another comment
[section]
# Comment in section
nested = "data"
)");
    auto result = ConfigResolver::parseSimpleTomlFlat(configPath_);
    EXPECT_EQ(result.size(), 2u);
    EXPECT_EQ(result["key"], "value");
    EXPECT_EQ(result["section.nested"], "data");
}

TEST_F(ConfigResolverTest, ResolveDefaultConfigPath_ReturnsPath) {
    auto path = ConfigResolver::resolveDefaultConfigPath();
    // Should return a path (may or may not exist depending on environment)
    // Just verify it doesn't throw
    SUCCEED();
}

TEST_F(ConfigResolverTest, DetectEmbeddingPreloadFlag_DefaultFalse) {
    DaemonConfig config;
    bool result = ConfigResolver::detectEmbeddingPreloadFlag(config);
    // Default should be false unless explicitly enabled
    EXPECT_FALSE(result);
}

TEST_F(ConfigResolverTest, ReadVectorMaxElements_DefaultValue) {
    size_t result = ConfigResolver::readVectorMaxElements();
    EXPECT_EQ(result, 100000u); // Default value
}

TEST_F(ConfigResolverTest, EnvTruthy_NullIsFalse) {
    EXPECT_FALSE(ConfigResolver::envTruthy(nullptr));
}

TEST_F(ConfigResolverTest, EnvTruthy_EmptyIsFalse) {
    EXPECT_FALSE(ConfigResolver::envTruthy(""));
}

TEST_F(ConfigResolverTest, EnvTruthy_ZeroIsFalse) {
    EXPECT_FALSE(ConfigResolver::envTruthy("0"));
}

TEST_F(ConfigResolverTest, EnvTruthy_FalseIsFalse) {
    EXPECT_FALSE(ConfigResolver::envTruthy("false"));
    EXPECT_FALSE(ConfigResolver::envTruthy("FALSE"));
}

TEST_F(ConfigResolverTest, EnvTruthy_OffIsFalse) {
    EXPECT_FALSE(ConfigResolver::envTruthy("off"));
    EXPECT_FALSE(ConfigResolver::envTruthy("OFF"));
}

TEST_F(ConfigResolverTest, EnvTruthy_NoIsFalse) {
    EXPECT_FALSE(ConfigResolver::envTruthy("no"));
    EXPECT_FALSE(ConfigResolver::envTruthy("NO"));
}

TEST_F(ConfigResolverTest, EnvTruthy_OneIsTrue) {
    EXPECT_TRUE(ConfigResolver::envTruthy("1"));
}

TEST_F(ConfigResolverTest, EnvTruthy_TrueIsTrue) {
    EXPECT_TRUE(ConfigResolver::envTruthy("true"));
    EXPECT_TRUE(ConfigResolver::envTruthy("TRUE"));
}

TEST_F(ConfigResolverTest, EnvTruthy_YesIsTrue) {
    EXPECT_TRUE(ConfigResolver::envTruthy("yes"));
    EXPECT_TRUE(ConfigResolver::envTruthy("YES"));
}

TEST_F(ConfigResolverTest, EnvTruthy_OnIsTrue) {
    EXPECT_TRUE(ConfigResolver::envTruthy("on"));
    EXPECT_TRUE(ConfigResolver::envTruthy("ON"));
}

TEST_F(ConfigResolverTest, EnvTruthy_ArbitraryValueIsTrue) {
    EXPECT_TRUE(ConfigResolver::envTruthy("anything"));
    EXPECT_TRUE(ConfigResolver::envTruthy("enabled"));
}

} // namespace yams::daemon::test
