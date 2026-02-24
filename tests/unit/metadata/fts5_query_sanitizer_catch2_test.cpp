#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <string>
#include <vector>

#include <yams/metadata/database.h>

namespace yams::metadata {
std::string sanitizeFTS5Query(const std::string& query);
}

TEST_CASE("FTS5 sanitize - simple tokens and reserved operators", "[unit][metadata][fts5]") {
    using yams::metadata::sanitizeFTS5Query;

    CHECK(sanitizeFTS5Query("sentence") == "sentence");
    CHECK(sanitizeFTS5Query("hello world") == "hello world");
    CHECK(sanitizeFTS5Query("AND") == "\"AND\"");
    CHECK(sanitizeFTS5Query("and") == "\"and\"");
    CHECK(sanitizeFTS5Query("OR") == "\"OR\"");
    CHECK(sanitizeFTS5Query("or") == "\"or\"");
    CHECK(sanitizeFTS5Query("NOT") == "\"NOT\"");
    CHECK(sanitizeFTS5Query("not") == "\"not\"");
    CHECK(sanitizeFTS5Query("foo*") == "foo*");
}

TEST_CASE("FTS5 sanitize - punctuation and hyphens", "[unit][metadata][fts5]") {
    using yams::metadata::sanitizeFTS5Query;

    CHECK(sanitizeFTS5Query("hello-world") == "\"hello-world\"");
    CHECK(sanitizeFTS5Query("hello-world*") == "\"hello-world\"*");
    CHECK(sanitizeFTS5Query("(hello)") == "hello");
    CHECK(sanitizeFTS5Query("hello, world.") == "hello world");
}

TEST_CASE("FTS5 sanitize - preserves UTF-8 terms", "[unit][metadata][fts5]") {
    using yams::metadata::sanitizeFTS5Query;

    const std::string cafe = "caf\xC3\xA9";
    const std::string resume = "r\xC3\xA9sum\xC3\xA9";

    CHECK(sanitizeFTS5Query(cafe + ".") == cafe);
    CHECK(sanitizeFTS5Query(cafe + " " + resume + ".") == (cafe + " " + resume));
}

TEST_CASE("FTS5 sanitize - advanced operators passthrough", "[unit][metadata][fts5]") {
    using yams::metadata::sanitizeFTS5Query;

    CHECK(sanitizeFTS5Query("title:hello") == "title:hello");
    CHECK(sanitizeFTS5Query("hello NEAR/2 world") == "hello NEAR/2 world");
    CHECK(sanitizeFTS5Query("\"exact phrase\"") == "\"exact phrase\"");
    CHECK(sanitizeFTS5Query("(foo OR bar) NOT baz") == "(foo OR bar) NOT baz");
}

// Test cases derived from BEIR scifact benchmark queries that caused SQL logic errors
TEST_CASE("FTS5 sanitize - benchmark scientific queries", "[unit][metadata][fts5][benchmark]") {
    using yams::metadata::sanitizeFTS5Query;

    SECTION("Parenthetical abbreviations like (DCs)") {
        // Query: "Crosstalk between dendritic cells (DCs) and innate lymphoid cells (ILCs)"
        auto result = sanitizeFTS5Query("dendritic cells (DCs)");
        INFO("Sanitized: " << result);
        // Should not contain unbalanced parens or produce empty tokens
        CHECK(!result.empty());
        CHECK(result.find("()") == std::string::npos); // No empty parens
    }

    SECTION("Single quotes around phrases") {
        // Query: "'Commelina yellow mottle virus' (ComYMV)"
        auto result = sanitizeFTS5Query("'Commelina yellow mottle virus' (ComYMV)");
        INFO("Sanitized: " << result);
        CHECK(!result.empty());
        // Should strip single quotes as punctuation
        CHECK(result.find('\'') == std::string::npos);
    }

    SECTION("Complex scientific query with parens and quotes") {
        auto result = sanitizeFTS5Query(
            "Crosstalk between dendritic cells (DCs) and innate lymphoid cells (ILCs)");
        INFO("Sanitized: " << result);
        CHECK(!result.empty());
    }

    SECTION("Embedded hyphens in scientific terms") {
        // Terms like "sugar-sweetened" should be preserved
        auto result = sanitizeFTS5Query("sugar-sweetened beverages");
        INFO("Sanitized: " << result);
        CHECK(result.find("sugar") != std::string::npos);
    }

    SECTION("Numbers and units") {
        auto result = sanitizeFTS5Query("p53 gene expression BRCA1");
        INFO("Sanitized: " << result);
        CHECK(result.find("p53") != std::string::npos);
        CHECK(result.find("BRCA1") != std::string::npos);
    }

    SECTION("Query with trailing punctuation") {
        auto result = sanitizeFTS5Query("What causes cancer?");
        INFO("Sanitized: " << result);
        // Question mark should be stripped
        CHECK(result.find('?') == std::string::npos);
    }

    SECTION("Query ending with period") {
        auto result = sanitizeFTS5Query("India. is a country.");
        INFO("Sanitized: " << result);
        CHECK(result.find("India") != std::string::npos);
    }

    SECTION("Possessive apostrophe") {
        auto result = sanitizeFTS5Query("Parkinson's disease treatment");
        INFO("Sanitized: " << result);
        // Parkinson's should become Parkinson (stripped apostrophe s)
        // or be quoted to preserve
        CHECK(!result.empty());
    }

    SECTION("Multiple parenthetical groups") {
        auto result = sanitizeFTS5Query("(HSC) differentiation (CD34+) cells");
        INFO("Sanitized: " << result);
        CHECK(!result.empty());
        CHECK(result.find("HSC") != std::string::npos);
    }
}

// Test that sanitized queries can actually execute in FTS5 (no SQL errors)
TEST_CASE("FTS5 sanitize - output is valid FTS5 syntax", "[unit][metadata][fts5][benchmark]") {
    using yams::metadata::sanitizeFTS5Query;

    // These are actual queries from BEIR scifact that caused failures
    std::vector<std::string> problematicQueries = {
        "Crosstalk between dendritic cells (DCs) and innate lymphoid cells (ILCs)",
        "'Commelina yellow mottle virus' (ComYMV)",
        "p53 tumor suppressor gene",
        "BRCA1 mutations breast cancer risk",
        "Polymeal nutrition reduces cardiovascular mortality",
        "Gene expression analysis revealed differential markers",
        "The p53 pathway dysregulation leads to tumor progression",
        "(CD34+) hematopoietic stem cells",
        "IL-6 and TNF-alpha cytokines",
        "mRNA vaccine efficacy COVID-19",
    };

    for (const auto& query : problematicQueries) {
        auto result = sanitizeFTS5Query(query);
        INFO("Original: " << query);
        INFO("Sanitized: " << result);

        // Basic validity checks
        CHECK(!result.empty());

        // Count quotes - must be balanced
        int quoteCount = 0;
        for (char c : result) {
            if (c == '"')
                quoteCount++;
        }
        CHECK((quoteCount % 2) == 0); // Even number of quotes

        // No unmatched parentheses in simple output
        // (advanced operators preserve parens, but simple sanitization strips them)
    }
}

// Actually execute sanitized queries against FTS5 to catch SQL errors
TEST_CASE("FTS5 sanitize - execute against real FTS5", "[unit][metadata][fts5][benchmark]") {
    using yams::metadata::sanitizeFTS5Query;
    using namespace yams::metadata;

    // Create temp database
    auto tempDir = std::filesystem::temp_directory_path() / "yams_fts5_sanitize_test";
    std::filesystem::create_directories(tempDir);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto dbPath = tempDir / ("fts5_test_" + std::to_string(ts) + ".db");

    Database db;
    auto openResult = db.open(dbPath.string(), ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    auto hasFts5 = db.hasFTS5();
    if (!hasFts5.has_value() || !hasFts5.value()) {
        std::filesystem::remove_all(tempDir);
        SKIP("FTS5 not available");
    }

    // Create FTS5 table
    auto createResult = db.execute("CREATE VIRTUAL TABLE test_fts USING fts5("
                                   "content, tokenize='porter unicode61')");
    REQUIRE(createResult.has_value());

    // Insert some test content
    auto ins = db.prepare("INSERT INTO test_fts(rowid, content) VALUES (?, ?)");
    REQUIRE(ins.has_value());
    ins.value().bind(1, 1);
    ins.value().bind(2, "BRCA1 mutations increase breast cancer risk. "
                        "The p53 tumor suppressor gene regulates cell cycle. "
                        "Dendritic cells DCs interact with innate lymphoid cells ILCs.");
    REQUIRE(ins.value().execute().has_value());

    // Test queries from benchmark
    std::vector<std::string> testQueries = {
        "Crosstalk between dendritic cells (DCs) and innate lymphoid cells (ILCs)",
        "'Commelina yellow mottle virus' (ComYMV)",
        "p53 tumor suppressor gene",
        "BRCA1 mutations breast cancer risk",
        "(CD34+) hematopoietic stem cells",
        "IL-6 and TNF-alpha cytokines",
        "Parkinson's disease treatment",
    };

    for (const auto& query : testQueries) {
        auto sanitized = sanitizeFTS5Query(query);
        INFO("Original: " << query);
        INFO("Sanitized: " << sanitized);

        // Prepare and execute - should NOT cause SQL error
        auto stmt = db.prepare("SELECT rowid FROM test_fts WHERE test_fts MATCH ?");
        REQUIRE(stmt.has_value());

        auto bindResult = stmt.value().bind(1, sanitized);
        REQUIRE(bindResult.has_value());

        // Execute - this is where SQL errors would occur
        auto stepResult = stmt.value().step();
        REQUIRE(stepResult.has_value()); // Should not be an error
        // We don't require results, just that it doesn't error
    }

    // Cleanup
    std::error_code ec;
    std::filesystem::remove_all(tempDir, ec);
}

#if defined(YAMS_TESTING)
namespace yams::metadata::test {
std::string buildNaturalLanguageFts5QueryForTest(std::string_view query, bool useOr,
                                                 bool autoPrefix, bool autoPhrase);
bool isLikelyNaturalLanguageQueryForTest(std::string_view query);
} // namespace yams::metadata::test

TEST_CASE("FTS5 NL builder - stopwords and phrase", "[unit][metadata][fts5]") {
    using yams::metadata::test::buildNaturalLanguageFts5QueryForTest;
    CHECK(buildNaturalLanguageFts5QueryForTest("the quick brown", false, true, true) ==
          "\"quick brown\"");
}

TEST_CASE("FTS5 NL builder - prefix gating", "[unit][metadata][fts5]") {
    using yams::metadata::test::buildNaturalLanguageFts5QueryForTest;
    CHECK(buildNaturalLanguageFts5QueryForTest("fast car engine", false, true, false) ==
          "fast* car engine*");
}

TEST_CASE("FTS5 smart mode detection", "[unit][metadata][fts5]") {
    using yams::metadata::test::isLikelyNaturalLanguageQueryForTest;
    CHECK(isLikelyNaturalLanguageQueryForTest("how to install sqlite fts5"));
    CHECK(isLikelyNaturalLanguageQueryForTest("quick brown fox jumps"));
    CHECK(isLikelyNaturalLanguageQueryForTest("caf\xC3\xA9 recipes guide"));
    CHECK_FALSE(isLikelyNaturalLanguageQueryForTest("title:report"));
    CHECK_FALSE(isLikelyNaturalLanguageQueryForTest("path/to/file"));
    CHECK_FALSE(isLikelyNaturalLanguageQueryForTest("foo_bar baz"));
    CHECK_FALSE(isLikelyNaturalLanguageQueryForTest("foo::bar baz"));
    CHECK_FALSE(isLikelyNaturalLanguageQueryForTest("status=200 logs"));
}
#endif
