// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <string_view>

namespace yams::metadata {

// Helper function to check if file is code (extracted for testing)
inline bool isCodeFile(std::string_view fileName, std::string_view mimeType) noexcept {
    // Use MIME type patterns aligned with magic_numbers.hpp
    if (mimeType.starts_with("text/x-") || mimeType.starts_with("application/x-") ||
        mimeType == "application/javascript" || mimeType == "application/typescript") {
        return true;
    }

    // Fallback: check file extension
    auto dotPos = fileName.rfind('.');
    if (dotPos == std::string_view::npos) {
        return false;
    }

    auto ext = fileName.substr(dotPos);

    // Common code extensions (consistent with magic_numbers.hpp patterns)
    return ext == ".c" || ext == ".cpp" || ext == ".cc" || ext == ".cxx" || ext == ".h" ||
           ext == ".hpp" || ext == ".hxx" || ext == ".hh" || ext == ".py" || ext == ".pyx" ||
           ext == ".pyi" || ext == ".rs" || ext == ".go" || ext == ".js" || ext == ".ts" ||
           ext == ".jsx" || ext == ".tsx" || ext == ".mjs" || ext == ".java" || ext == ".kt" ||
           ext == ".kts" || ext == ".scala" || ext == ".rb" || ext == ".php" || ext == ".swift" ||
           ext == ".cs" || ext == ".fs" || ext == ".vb" || ext == ".sh" || ext == ".bash" ||
           ext == ".zsh" || ext == ".lua" || ext == ".pl" || ext == ".r" || ext == ".jl";
}

// Tests for fuzzy index prioritization logic
class FuzzyIndexPrioritizationTest : public ::testing::Test {
protected:
    // Score computation formula
    double computeScore(int tagCount, int entityCount, int recencyDays, bool isCode) {
        double recencyScore = recencyDays < 7     ? 4.0
                              : recencyDays < 30  ? 3.0
                              : recencyDays < 90  ? 2.0
                              : recencyDays < 180 ? 1.0
                                                  : 0.0;

        return (tagCount * 3.0) + (entityCount * 2.0) + recencyScore + (isCode ? 2.0 : 0.0);
    }
};

// Code file detection tests
TEST_F(FuzzyIndexPrioritizationTest, DetectCodeFileByMimeType) {
    EXPECT_TRUE(isCodeFile("unknown.txt", "text/x-c"));
    EXPECT_TRUE(isCodeFile("unknown.txt", "text/x-c++"));
    EXPECT_TRUE(isCodeFile("unknown.txt", "text/x-python"));
    EXPECT_TRUE(isCodeFile("unknown.txt", "text/x-shellscript"));
    EXPECT_TRUE(isCodeFile("unknown.txt", "application/javascript"));
    EXPECT_TRUE(isCodeFile("unknown.txt", "application/typescript"));
    EXPECT_TRUE(isCodeFile("unknown.txt", "application/x-ruby"));
}

TEST_F(FuzzyIndexPrioritizationTest, DetectCodeFileByExtension) {
    // C/C++
    EXPECT_TRUE(isCodeFile("test.c", ""));
    EXPECT_TRUE(isCodeFile("test.cpp", ""));
    EXPECT_TRUE(isCodeFile("test.hpp", ""));
    EXPECT_TRUE(isCodeFile("test.h", ""));
    EXPECT_TRUE(isCodeFile("test.cc", ""));
    EXPECT_TRUE(isCodeFile("test.cxx", ""));
    EXPECT_TRUE(isCodeFile("test.hxx", ""));

    // Python
    EXPECT_TRUE(isCodeFile("script.py", ""));
    EXPECT_TRUE(isCodeFile("stub.pyi", ""));

    // Rust
    EXPECT_TRUE(isCodeFile("main.rs", ""));

    // Go
    EXPECT_TRUE(isCodeFile("main.go", ""));

    // JavaScript/TypeScript
    EXPECT_TRUE(isCodeFile("app.js", ""));
    EXPECT_TRUE(isCodeFile("app.ts", ""));
    EXPECT_TRUE(isCodeFile("App.jsx", ""));
    EXPECT_TRUE(isCodeFile("App.tsx", ""));
    EXPECT_TRUE(isCodeFile("module.mjs", ""));

    // Other languages
    EXPECT_TRUE(isCodeFile("Main.java", ""));
    EXPECT_TRUE(isCodeFile("script.rb", ""));
    EXPECT_TRUE(isCodeFile("index.php", ""));
    EXPECT_TRUE(isCodeFile("App.swift", ""));
    EXPECT_TRUE(isCodeFile("Program.cs", ""));
    EXPECT_TRUE(isCodeFile("script.sh", ""));
    EXPECT_TRUE(isCodeFile("script.lua", ""));
}

TEST_F(FuzzyIndexPrioritizationTest, RejectNonCodeFiles) {
    EXPECT_FALSE(isCodeFile("doc.txt", "text/plain"));
    EXPECT_FALSE(isCodeFile("doc.md", "text/markdown"));
    EXPECT_FALSE(isCodeFile("image.png", "image/png"));
    EXPECT_FALSE(isCodeFile("video.mp4", "video/mp4"));
    EXPECT_FALSE(isCodeFile("data.json", "application/json"));
    EXPECT_FALSE(isCodeFile("data.xml", "application/xml"));
    EXPECT_FALSE(isCodeFile("README", ""));
    EXPECT_FALSE(isCodeFile("Makefile", ""));
}

// Score computation tests
TEST_F(FuzzyIndexPrioritizationTest, ScoreTaggedDocument) {
    // Tagged document with 5 tags should score high
    double score = computeScore(5, 0, 365, false);
    EXPECT_EQ(score, 15.0); // 5 * 3.0
}

TEST_F(FuzzyIndexPrioritizationTest, ScoreKGConnectedDocument) {
    // Document with 10 entities should score high
    double score = computeScore(0, 10, 365, false);
    EXPECT_EQ(score, 20.0); // 10 * 2.0
}

TEST_F(FuzzyIndexPrioritizationTest, ScoreRecentDocument) {
    // Very recent (3 days)
    EXPECT_EQ(computeScore(0, 0, 3, false), 4.0);

    // Recent (2 weeks)
    EXPECT_EQ(computeScore(0, 0, 14, false), 3.0);

    // Somewhat recent (2 months)
    EXPECT_EQ(computeScore(0, 0, 60, false), 2.0);

    // Older (4 months)
    EXPECT_EQ(computeScore(0, 0, 120, false), 1.0);

    // Old (1 year)
    EXPECT_EQ(computeScore(0, 0, 365, false), 0.0);
}

TEST_F(FuzzyIndexPrioritizationTest, ScoreCodeFile) {
    // Code file gets boost
    EXPECT_EQ(computeScore(0, 0, 365, true), 2.0);
    EXPECT_EQ(computeScore(0, 0, 365, false), 0.0);
}

TEST_F(FuzzyIndexPrioritizationTest, ScoreCompositeHigh) {
    // Tagged, KG-connected, recent code file
    double score = computeScore(3, 5, 3, true);
    // = (3*3) + (5*2) + 4 + 2 = 9 + 10 + 4 + 2 = 25
    EXPECT_EQ(score, 25.0);
}

TEST_F(FuzzyIndexPrioritizationTest, ScoreCompositeModerate) {
    // Some tags, entities, older
    double score = computeScore(2, 3, 60, false);
    // = (2*3) + (3*2) + 2 + 0 = 6 + 6 + 2 = 14
    EXPECT_EQ(score, 14.0);
}

TEST_F(FuzzyIndexPrioritizationTest, ScoreCompositeLow) {
    // No tags, no entities, old, not code
    double score = computeScore(0, 0, 365, false);
    EXPECT_EQ(score, 0.0);
}

// Prioritization order tests
TEST_F(FuzzyIndexPrioritizationTest, PrioritizationOrder) {
    // Tagged should beat KG-connected (3x weight vs 2x weight)
    double tagged = computeScore(2, 0, 365, false); // 6.0
    double kg = computeScore(0, 2, 365, false);     // 4.0
    EXPECT_GT(tagged, kg);

    // KG-connected should beat recent
    kg = computeScore(0, 2, 365, false);          // 4.0
    double recent = computeScore(0, 0, 3, false); // 4.0
    EXPECT_GE(kg, recent);                        // Equal weight

    // Recent should beat code boost
    recent = computeScore(0, 0, 3, false);       // 4.0
    double code = computeScore(0, 0, 365, true); // 2.0
    EXPECT_GT(recent, code);
}

// Edge cases
TEST_F(FuzzyIndexPrioritizationTest, EdgeCases) {
    // Very high tag count
    double score = computeScore(100, 0, 365, false);
    EXPECT_EQ(score, 300.0);

    // Very high entity count
    score = computeScore(0, 100, 365, false);
    EXPECT_EQ(score, 200.0);

    // Negative days (future) - should be treated as very recent
    score = computeScore(0, 0, -1, false);
    EXPECT_EQ(score, 4.0);

    // Empty filename
    EXPECT_FALSE(isCodeFile("", ""));

    // Filename without extension
    EXPECT_FALSE(isCodeFile("README", ""));
}

} // namespace yams::metadata
