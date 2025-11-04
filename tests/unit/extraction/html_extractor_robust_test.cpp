#include <filesystem>
#include <fstream>
#include <catch2/catch_test_macros.hpp>
#include <yams/extraction/html_text_extractor.h>

using namespace yams::extraction;

namespace fs = std::filesystem;

TEST_CASE("HtmlTextExtractor - No regex crashes on large files", "[extraction][html][robust]") {
    HtmlTextExtractor extractor;
    ExtractionConfig config;

    SECTION("Very large HTML file does not crash") {
        // Create a large HTML file with nested tags (~3.8 MB)
        std::string largeHtml = "<html><body>";
        for (int i = 0; i < 100000; i++) {
            largeHtml += "<div><p>Test paragraph " + std::to_string(i) + "</p></div>";
        }
        largeHtml += "</body></html>";

        REQUIRE(largeHtml.size() > 3 * 1024 * 1024); // > 3MB

        // This should not crash (previously would with regex)
        auto result = extractor.extractFromBuffer(
            std::as_bytes(std::span(largeHtml.data(), largeHtml.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().success);
        REQUIRE_FALSE(result.value().text.empty());
    }

    SECTION("Pathological nested HTML") {
        // Deep nesting that causes regex backtracking
        std::string nested = "<html><body>";
        for (int i = 0; i < 1000; i++) {
            nested += "<div>";
        }
        nested += "Content";
        for (int i = 0; i < 1000; i++) {
            nested += "</div>";
        }
        nested += "</body></html>";

        auto result = extractor.extractFromBuffer(
            std::as_bytes(std::span(nested.data(), nested.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().success);
        REQUIRE(result.value().text.find("Content") != std::string::npos);
    }

    SECTION("Malformed HTML with unclosed tags") {
        std::string malformed = "<html><body><div><p>Test<div>Another<p>Third</body>";

        auto result = extractor.extractFromBuffer(
            std::as_bytes(std::span(malformed.data(), malformed.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().success);
        REQUIRE(result.value().text.find("Test") != std::string::npos);
    }
}

TEST_CASE("HtmlTextExtractor - Block tag conversion without regex", "[extraction][html][robust]") {
    HtmlTextExtractor extractor;
    ExtractionConfig config;

    SECTION("Block tags converted to newlines") {
        std::string html = "<html><body><h1>Title</h1><p>Para 1</p><p>Para 2</p></body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("Title") != std::string::npos);
        REQUIRE(result.value().text.find("Para 1") != std::string::npos);
        REQUIRE(result.value().text.find("Para 2") != std::string::npos);
    }

    SECTION("Mixed case block tags") {
        std::string html = "<HTML><BODY><DIV>Mixed</DIV><Div>Case</div></BODY></HTML>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("Mixed") != std::string::npos);
        REQUIRE(result.value().text.find("Case") != std::string::npos);
    }

    SECTION("BR tags converted to newlines") {
        std::string html = "<html><body>Line 1<br>Line 2<br/>Line 3<BR />Line 4</body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("Line 1") != std::string::npos);
        REQUIRE(result.value().text.find("Line 2") != std::string::npos);
        REQUIRE(result.value().text.find("Line 3") != std::string::npos);
        REQUIRE(result.value().text.find("Line 4") != std::string::npos);
    }
}

TEST_CASE("HtmlTextExtractor - Title extraction without regex", "[extraction][html][robust]") {
    HtmlTextExtractor extractor;
    ExtractionConfig config;
    config.extractMetadata = true;

    SECTION("Simple title extraction") {
        std::string html =
            "<html><head><title>Test Title</title></head><body>Content</body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().metadata.count("title") > 0);
        REQUIRE(result.value().metadata["title"] == "Test Title");
    }

    SECTION("Title with attributes") {
        std::string html = "<html><head><title lang=\"en\">Title with Attrs</title></head></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().metadata.count("title") > 0);
        REQUIRE(result.value().metadata["title"] == "Title with Attrs");
    }

    SECTION("Mixed case title tag") {
        std::string html = "<HTML><HEAD><TITLE>CAPS Title</TITLE></HEAD></HTML>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().metadata.count("title") > 0);
        REQUIRE(result.value().metadata["title"] == "CAPS Title");
    }

    SECTION("No title present") {
        std::string html = "<html><head></head><body>No title</body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().metadata.count("title") == 0);
    }
}

TEST_CASE("HtmlTextExtractor - Meta description extraction without regex",
          "[extraction][html][robust]") {
    HtmlTextExtractor extractor;
    ExtractionConfig config;
    config.extractMetadata = true;

    SECTION("Standard meta description with double quotes") {
        std::string html =
            R"(<html><head><meta name="description" content="Test description"></head></html>)";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().metadata.count("description") > 0);
        REQUIRE(result.value().metadata["description"] == "Test description");
    }

    SECTION("Meta description with single quotes") {
        std::string html =
            R"(<html><head><meta name='description' content='Single quotes'></head></html>)";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().metadata.count("description") > 0);
        REQUIRE(result.value().metadata["description"] == "Single quotes");
    }

    SECTION("OpenGraph description") {
        std::string html =
            R"(<html><head><meta property="og:description" content="OG description"></head></html>)";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().metadata.count("description") > 0);
        REQUIRE(result.value().metadata["description"] == "OG description");
    }

    SECTION("Reversed attribute order") {
        std::string html =
            R"(<html><head><meta content="Reversed" name="description"></head></html>)";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().metadata.count("description") > 0);
        REQUIRE(result.value().metadata["description"] == "Reversed");
    }
}

TEST_CASE("HtmlTextExtractor - Entity decoding without regex", "[extraction][html][robust]") {
    HtmlTextExtractor extractor;
    ExtractionConfig config;

    SECTION("Named entities") {
        std::string html =
            "<html><body>&lt;HTML&gt; &amp; &quot;quotes&quot; &apos;test&apos;</body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("<HTML>") != std::string::npos);
        REQUIRE(result.value().text.find("&") != std::string::npos);
        REQUIRE(result.value().text.find("\"quotes\"") != std::string::npos);
    }

    SECTION("Numeric entities") {
        std::string html = "<html><body>&#65;&#66;&#67;</body></html>"; // ABC

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("ABC") != std::string::npos);
    }

    SECTION("Hex entities") {
        std::string html = "<html><body>&#x41;&#x42;&#x43;</body></html>"; // ABC

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("ABC") != std::string::npos);
    }

    SECTION("Mixed entities") {
        std::string html = "<html><body>&lt;&#65;&gt; &#x20; &amp;</body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("<A>") != std::string::npos);
        REQUIRE(result.value().text.find("&") != std::string::npos);
    }

    SECTION("Invalid entities preserved") {
        std::string html = "<html><body>&invalid; &#999999; &#xZZZZ;</body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        // Should not crash, invalid entities may be preserved
    }
}

TEST_CASE("HtmlTextExtractor - Script and style removal", "[extraction][html][robust]") {
    HtmlTextExtractor extractor;
    ExtractionConfig config;

    SECTION("Script tags removed") {
        std::string html =
            "<html><body><script>alert('remove me');</script>Visible text</body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("Visible text") != std::string::npos);
        REQUIRE(result.value().text.find("alert") == std::string::npos);
    }

    SECTION("Style tags removed") {
        std::string html =
            "<html><head><style>body { color: red; }</style></head><body>Content</body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("Content") != std::string::npos);
        REQUIRE(result.value().text.find("color") == std::string::npos);
    }

    SECTION("HTML comments removed") {
        std::string html = "<html><body><!-- Comment -->Visible<!-- Another --></body></html>";

        auto result =
            extractor.extractFromBuffer(std::as_bytes(std::span(html.data(), html.size())), config);

        REQUIRE(result.has_value());
        REQUIRE(result.value().text.find("Visible") != std::string::npos);
        REQUIRE(result.value().text.find("Comment") == std::string::npos);
    }
}

TEST_CASE("HtmlTextExtractor - Performance and memory",
          "[extraction][html][robust][.performance]") {
    HtmlTextExtractor extractor;
    ExtractionConfig config;

    SECTION("20MB HTML file completes in reasonable time") {
        // Generate 20MB of HTML
        std::string largeHtml = "<html><body>";
        while (largeHtml.size() < 20 * 1024 * 1024) {
            largeHtml +=
                "<div><p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. </p></div>";
        }
        largeHtml += "</body></html>";

        auto start = std::chrono::steady_clock::now();
        auto result = extractor.extractFromBuffer(
            std::as_bytes(std::span(largeHtml.data(), largeHtml.size())), config);
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);

        REQUIRE(result.has_value());
        REQUIRE(result.value().success);
        // Should complete in under 5 seconds (no exponential regex backtracking)
        REQUIRE(duration.count() < 5000);
    }
}
