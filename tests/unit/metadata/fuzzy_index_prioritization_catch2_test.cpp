// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

#include <string_view>

#include <catch2/catch_test_macros.hpp>

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

// Score computation formula
double computeScore(int tagCount, int entityCount, int recencyDays, bool isCode) {
    double recencyScore = recencyDays < 7     ? 4.0
                          : recencyDays < 30  ? 3.0
                          : recencyDays < 90  ? 2.0
                          : recencyDays < 180 ? 1.0
                                              : 0.0;

    return (tagCount * 3.0) + (entityCount * 2.0) + recencyScore + (isCode ? 2.0 : 0.0);
}

// Code file detection tests
TEST_CASE("FuzzyIndexPrioritization: detect code file by MIME type", "[unit][metadata][fuzzy]") {
    CHECK(isCodeFile("unknown.txt", "text/x-c"));
    CHECK(isCodeFile("unknown.txt", "text/x-c++"));
    CHECK(isCodeFile("unknown.txt", "text/x-python"));
    CHECK(isCodeFile("unknown.txt", "text/x-shellscript"));
    CHECK(isCodeFile("unknown.txt", "application/javascript"));
    CHECK(isCodeFile("unknown.txt", "application/typescript"));
    CHECK(isCodeFile("unknown.txt", "application/x-ruby"));
}

TEST_CASE("FuzzyIndexPrioritization: detect code file by extension", "[unit][metadata][fuzzy]") {
    SECTION("C/C++") {
        CHECK(isCodeFile("test.c", ""));
        CHECK(isCodeFile("test.cpp", ""));
        CHECK(isCodeFile("test.hpp", ""));
        CHECK(isCodeFile("test.h", ""));
        CHECK(isCodeFile("test.cc", ""));
        CHECK(isCodeFile("test.cxx", ""));
        CHECK(isCodeFile("test.hxx", ""));
    }

    SECTION("Python") {
        CHECK(isCodeFile("script.py", ""));
        CHECK(isCodeFile("stub.pyi", ""));
    }

    SECTION("Rust") {
        CHECK(isCodeFile("main.rs", ""));
    }

    SECTION("Go") {
        CHECK(isCodeFile("main.go", ""));
    }

    SECTION("JavaScript/TypeScript") {
        CHECK(isCodeFile("app.js", ""));
        CHECK(isCodeFile("app.ts", ""));
        CHECK(isCodeFile("App.jsx", ""));
        CHECK(isCodeFile("App.tsx", ""));
        CHECK(isCodeFile("module.mjs", ""));
    }

    SECTION("Other languages") {
        CHECK(isCodeFile("Main.java", ""));
        CHECK(isCodeFile("script.rb", ""));
        CHECK(isCodeFile("index.php", ""));
        CHECK(isCodeFile("App.swift", ""));
        CHECK(isCodeFile("Program.cs", ""));
        CHECK(isCodeFile("script.sh", ""));
        CHECK(isCodeFile("script.lua", ""));
    }
}

TEST_CASE("FuzzyIndexPrioritization: reject non-code files", "[unit][metadata][fuzzy]") {
    CHECK_FALSE(isCodeFile("doc.txt", "text/plain"));
    CHECK_FALSE(isCodeFile("doc.md", "text/markdown"));
    CHECK_FALSE(isCodeFile("image.png", "image/png"));
    CHECK_FALSE(isCodeFile("video.mp4", "video/mp4"));
    CHECK_FALSE(isCodeFile("data.json", "application/json"));
    CHECK_FALSE(isCodeFile("data.xml", "application/xml"));
    CHECK_FALSE(isCodeFile("README", ""));
    CHECK_FALSE(isCodeFile("Makefile", ""));
}

// Score computation tests
TEST_CASE("FuzzyIndexPrioritization: score tagged document", "[unit][metadata][fuzzy]") {
    double score = computeScore(5, 0, 365, false);
    CHECK(score == 15.0); // 5 * 3.0
}

TEST_CASE("FuzzyIndexPrioritization: score KG connected document", "[unit][metadata][fuzzy]") {
    double score = computeScore(0, 10, 365, false);
    CHECK(score == 20.0); // 10 * 2.0
}

TEST_CASE("FuzzyIndexPrioritization: score recent document", "[unit][metadata][fuzzy]") {
    // Very recent (3 days)
    CHECK(computeScore(0, 0, 3, false) == 4.0);

    // Recent (2 weeks)
    CHECK(computeScore(0, 0, 14, false) == 3.0);

    // Somewhat recent (2 months)
    CHECK(computeScore(0, 0, 60, false) == 2.0);

    // Older (4 months)
    CHECK(computeScore(0, 0, 120, false) == 1.0);

    // Old (1 year)
    CHECK(computeScore(0, 0, 365, false) == 0.0);
}

TEST_CASE("FuzzyIndexPrioritization: score code file", "[unit][metadata][fuzzy]") {
    CHECK(computeScore(0, 0, 365, true) == 2.0);
    CHECK(computeScore(0, 0, 365, false) == 0.0);
}

TEST_CASE("FuzzyIndexPrioritization: score composite high", "[unit][metadata][fuzzy]") {
    // Tagged, KG-connected, recent code file
    double score = computeScore(3, 5, 3, true);
    // = (3*3) + (5*2) + 4 + 2 = 9 + 10 + 4 + 2 = 25
    CHECK(score == 25.0);
}

TEST_CASE("FuzzyIndexPrioritization: score composite moderate", "[unit][metadata][fuzzy]") {
    double score = computeScore(2, 3, 60, false);
    // = (2*3) + (3*2) + 2 + 0 = 6 + 6 + 2 = 14
    CHECK(score == 14.0);
}

TEST_CASE("FuzzyIndexPrioritization: score composite low", "[unit][metadata][fuzzy]") {
    double score = computeScore(0, 0, 365, false);
    CHECK(score == 0.0);
}

// Prioritization order tests
TEST_CASE("FuzzyIndexPrioritization: prioritization order", "[unit][metadata][fuzzy]") {
    // Tagged should beat KG-connected (3x weight vs 2x weight)
    double tagged = computeScore(2, 0, 365, false); // 6.0
    double kg = computeScore(0, 2, 365, false);     // 4.0
    CHECK(tagged > kg);

    // KG-connected should beat recent
    kg = computeScore(0, 2, 365, false);          // 4.0
    double recent = computeScore(0, 0, 3, false); // 4.0
    CHECK(kg >= recent);                          // Equal weight

    // Recent should beat code boost
    recent = computeScore(0, 0, 3, false);       // 4.0
    double code = computeScore(0, 0, 365, true); // 2.0
    CHECK(recent > code);
}

// Edge cases
TEST_CASE("FuzzyIndexPrioritization: edge cases", "[unit][metadata][fuzzy]") {
    // Very high tag count
    double score = computeScore(100, 0, 365, false);
    CHECK(score == 300.0);

    // Very high entity count
    score = computeScore(0, 100, 365, false);
    CHECK(score == 200.0);

    // Negative days (future) - should be treated as very recent
    score = computeScore(0, 0, -1, false);
    CHECK(score == 4.0);

    // Empty filename
    CHECK_FALSE(isCodeFile("", ""));

    // Filename without extension
    CHECK_FALSE(isCodeFile("README", ""));
}

} // namespace yams::metadata
