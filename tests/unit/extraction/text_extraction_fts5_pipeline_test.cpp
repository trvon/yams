// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors
//
// Test: Full pipeline from text file detection → extraction → FTS5 indexing → search
// Purpose: Isolate issues in the text extraction and indexing pipeline

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>

#include <yams/detection/file_type_detector.h>
#include <yams/extraction/plain_text_extractor.h>
#include <yams/extraction/text_extractor.h>
#include <yams/metadata/database.h>

using namespace yams;
using Catch::Matchers::ContainsSubstring;

namespace {

struct PipelineTestFixture {
    std::filesystem::path tempDir;
    std::filesystem::path dbPath;
    std::filesystem::path txtFile;

    PipelineTestFixture() {
        tempDir = std::filesystem::temp_directory_path() / "yams_pipeline_test";
        std::filesystem::create_directories(tempDir);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath = tempDir / ("pipeline_test_" + std::to_string(ts) + ".db");
        txtFile = tempDir / "test_document.txt";
    }

    ~PipelineTestFixture() {
        std::error_code ec;
        std::filesystem::remove_all(tempDir, ec);
    }

    void createTextFile(const std::string& content) {
        std::ofstream out(txtFile);
        out << content;
        out.close();
    }
};

} // namespace

TEST_CASE("Pipeline: MIME type detection for .txt files", "[unit][pipeline][detection]") {
    PipelineTestFixture fix;

    SECTION("Extension-based detection returns text/plain for .txt") {
        auto mime = detection::FileTypeDetector::getMimeTypeFromExtension(".txt");
        REQUIRE(mime == "text/plain");
        spdlog::info("✓ .txt extension maps to text/plain");
    }

    SECTION("isTextMimeType returns true for text/plain") {
        auto& detector = detection::FileTypeDetector::instance();
        REQUIRE(detector.isTextMimeType("text/plain"));
        spdlog::info("✓ text/plain is recognized as text MIME type");
    }

    SECTION("File content detection for plain text without BOM") {
        fix.createTextFile("This is plain text content without any BOM marker.");

        auto& detector = detection::FileTypeDetector::instance();
        auto result = detector.detectFromFile(fix.txtFile);

        REQUIRE(result.has_value());
        auto& sig = result.value();

        // Plain text without BOM should be detected as text/plain
        INFO("Detected MIME: " << sig.mimeType);
        INFO("Is binary: " << sig.isBinary);
        INFO("File type: " << sig.fileType);

        REQUIRE_FALSE(sig.isBinary);
        // May be text/plain or fall back based on detection
        REQUIRE(detector.isTextMimeType(sig.mimeType));
        spdlog::info("✓ Plain text file detected as text type: {}", sig.mimeType);
    }
}

TEST_CASE("Pipeline: PlainTextExtractor extracts .txt content", "[unit][pipeline][extraction]") {
    PipelineTestFixture fix;

    const std::string testContent = "Scientific claims about gene expression.\n"
                                    "BRCA1 mutations are associated with breast cancer.\n"
                                    "The p53 tumor suppressor gene regulates cell cycle.";

    fix.createTextFile(testContent);

    SECTION("Extract from file path") {
        extraction::PlainTextExtractor extractor;

        REQUIRE(extractor.canExtract(fix.txtFile));

        extraction::ExtractionConfig cfg;
        cfg.preserveFormatting = true;

        auto result = extractor.extract(fix.txtFile, cfg);
        REQUIRE(result.has_value());

        auto& extracted = result.value();
        REQUIRE(extracted.isSuccess());
        REQUIRE_FALSE(extracted.text.empty());

        INFO("Extracted text length: " << extracted.text.size());
        INFO("First 100 chars: " << extracted.text.substr(0, 100));

        REQUIRE_THAT(extracted.text, ContainsSubstring("BRCA1"));
        REQUIRE_THAT(extracted.text, ContainsSubstring("tumor suppressor"));
        spdlog::info("✓ PlainTextExtractor successfully extracted {} bytes", extracted.text.size());
    }

    SECTION("Extract from buffer") {
        extraction::PlainTextExtractor extractor;

        // Read file into buffer
        std::ifstream in(fix.txtFile, std::ios::binary);
        std::vector<char> charBuf((std::istreambuf_iterator<char>(in)),
                                  std::istreambuf_iterator<char>());
        std::vector<std::byte> buffer;
        buffer.reserve(charBuf.size());
        for (char c : charBuf) {
            buffer.push_back(static_cast<std::byte>(c));
        }

        extraction::ExtractionConfig cfg;
        auto result = extractor.extractFromBuffer(
            std::span<const std::byte>(buffer.data(), buffer.size()), cfg);

        REQUIRE(result.has_value());
        auto& extracted = result.value();
        REQUIRE(extracted.isSuccess());
        REQUIRE_THAT(extracted.text, ContainsSubstring("BRCA1"));
        spdlog::info("✓ PlainTextExtractor buffer extraction works");
    }
}

TEST_CASE("Pipeline: TextExtractorFactory returns extractor for .txt",
          "[unit][pipeline][factory]") {
    auto& factory = extraction::TextExtractorFactory::instance();

    SECTION("Factory supports .txt extension") {
        REQUIRE(factory.isSupported(".txt"));
        spdlog::info("✓ TextExtractorFactory supports .txt");
    }

    SECTION("Factory creates extractor for .txt") {
        auto extractor = factory.create(".txt");
        REQUIRE(extractor != nullptr);
        INFO("Extractor name: " << extractor->name());
        spdlog::info("✓ Factory created extractor: {}", extractor->name());
    }

    SECTION("Factory handles case-insensitive extensions") {
        REQUIRE(factory.isSupported(".TXT"));
        REQUIRE(factory.isSupported(".Txt"));
        auto extractor = factory.create(".TXT");
        REQUIRE(extractor != nullptr);
        spdlog::info("✓ Case-insensitive extension lookup works");
    }
}

TEST_CASE("Pipeline: FTS5 indexes and searches extracted text", "[unit][pipeline][fts5]") {
    PipelineTestFixture fix;

    const std::string scientificContent =
        "BRCA1 mutations significantly increase breast cancer risk.\n"
        "The study found that p53 pathway dysregulation leads to tumor progression.\n"
        "Gene expression analysis revealed differential markers.";

    fix.createTextFile(scientificContent);

    // Step 1: Extract text
    extraction::PlainTextExtractor extractor;
    extraction::ExtractionConfig cfg;
    cfg.preserveFormatting = true;
    auto extractResult = extractor.extract(fix.txtFile, cfg);
    REQUIRE(extractResult.has_value());
    REQUIRE(extractResult.value().isSuccess());
    std::string extractedText = extractResult.value().text;

    INFO("Extracted text: " << extractedText);

    // Step 2: Create FTS5 table and index
    metadata::Database db;
    auto openResult = db.open(fix.dbPath.string(), metadata::ConnectionMode::Create);
    REQUIRE(openResult.has_value());

    auto hasFts5 = db.hasFTS5();
    REQUIRE(hasFts5.has_value());
    if (!hasFts5.value()) {
        SKIP("FTS5 not available");
    }

    // Create FTS5 table matching YAMS schema
    auto createResult = db.execute("CREATE VIRTUAL TABLE documents_fts USING fts5("
                                   "content, title, "
                                   "tokenize='porter unicode61 tokenchars ''_-'''"
                                   ")");
    REQUIRE(createResult.has_value());

    // Insert extracted content
    auto insertStmt =
        db.prepare("INSERT INTO documents_fts(rowid, content, title) VALUES (?, ?, ?)");
    REQUIRE(insertStmt.has_value());
    auto& stmt = insertStmt.value();
    stmt.bind(1, 12345); // Document ID
    stmt.bind(2, extractedText);
    stmt.bind(3, "test_document.txt");
    auto insertResult = stmt.execute();
    REQUIRE(insertResult.has_value());

    spdlog::info("✓ Indexed {} bytes into FTS5", extractedText.size());

    SECTION("Search for exact term: BRCA1") {
        auto searchStmt =
            db.prepare("SELECT rowid, snippet(documents_fts, 0, '<b>', '</b>', '...', 16) "
                       "FROM documents_fts WHERE documents_fts MATCH 'BRCA1'");
        REQUIRE(searchStmt.has_value());

        auto stepResult = searchStmt.value().step();
        REQUIRE(stepResult.has_value());
        REQUIRE(stepResult.value()); // Has results
        spdlog::info("✓ FTS5 search for 'BRCA1' returned results");
    }

    SECTION("Search for stemmed term: mutations") {
        auto searchStmt =
            db.prepare("SELECT rowid FROM documents_fts WHERE documents_fts MATCH 'mutation'");
        REQUIRE(searchStmt.has_value());

        auto stepResult = searchStmt.value().step();
        REQUIRE(stepResult.has_value());
        INFO("Step result: " << stepResult.value());
        // Porter stemmer should match 'mutations' with 'mutation'
        REQUIRE(stepResult.value());
        spdlog::info("✓ FTS5 porter stemmer matches 'mutation' -> 'mutations'");
    }

    SECTION("Search for phrase: breast cancer") {
        auto searchStmt = db.prepare(
            "SELECT rowid FROM documents_fts WHERE documents_fts MATCH '\"breast cancer\"'");
        REQUIRE(searchStmt.has_value());

        auto stepResult = searchStmt.value().step();
        REQUIRE(stepResult.has_value());
        REQUIRE(stepResult.value());
        spdlog::info("✓ FTS5 phrase search works");
    }

    SECTION("Search for multiple terms (AND): BRCA1 cancer") {
        auto searchStmt =
            db.prepare("SELECT rowid FROM documents_fts WHERE documents_fts MATCH 'BRCA1 cancer'");
        REQUIRE(searchStmt.has_value());

        auto stepResult = searchStmt.value().step();
        REQUIRE(stepResult.has_value());
        REQUIRE(stepResult.value());
        spdlog::info("✓ FTS5 multi-term AND search works");
    }

    SECTION("Search returns no results for non-existent term") {
        auto searchStmt = db.prepare(
            "SELECT rowid FROM documents_fts WHERE documents_fts MATCH 'xyznonexistent'");
        REQUIRE(searchStmt.has_value());

        auto stepResult = searchStmt.value().step();
        REQUIRE(stepResult.has_value());
        REQUIRE_FALSE(stepResult.value()); // No results expected
        spdlog::info("✓ FTS5 correctly returns no results for non-existent term");
    }
}

TEST_CASE("Pipeline: Scientific query patterns", "[unit][pipeline][scientific]") {
    PipelineTestFixture fix;

    // Content similar to scifact BEIR dataset
    const std::string content =
        "Polymeal nutrition has been shown to reduce cardiovascular mortality "
        "in multiple clinical trials. The combination of wine, fish, dark chocolate, "
        "fruits, vegetables, garlic, and almonds provides synergistic benefits.";

    fix.createTextFile(content);

    // Extract
    extraction::PlainTextExtractor extractor;
    auto extractResult = extractor.extract(fix.txtFile, {});
    REQUIRE(extractResult.has_value());
    std::string text = extractResult.value().text;

    // Index
    metadata::Database db;
    REQUIRE(db.open(fix.dbPath.string(), metadata::ConnectionMode::Create).has_value());
    auto hasFts = db.hasFTS5();
    if (!hasFts.has_value() || !hasFts.value())
        SKIP("FTS5 not available");

    REQUIRE(db.execute("CREATE VIRTUAL TABLE docs USING fts5(content, tokenize='porter unicode61')")
                .has_value());

    auto ins = db.prepare("INSERT INTO docs(rowid, content) VALUES (1, ?)");
    REQUIRE(ins.has_value());
    ins.value().bind(1, text);
    REQUIRE(ins.value().execute().has_value());

    SECTION("Query: Polymeal nutrition reduces cardiovascular mortality") {
        // This is a typical scifact query - test different formulations

        // Full query (may fail due to too many terms)
        auto q1 = db.prepare("SELECT rowid FROM docs WHERE docs MATCH 'polymeal nutrition "
                             "cardiovascular mortality'");
        REQUIRE(q1.has_value());
        auto r1 = q1.value().step();
        INFO("Full query result: " << (r1.has_value() ? (r1.value() ? "FOUND" : "NOT FOUND")
                                                      : "ERROR"));

        // Single key term
        auto q2 = db.prepare("SELECT rowid FROM docs WHERE docs MATCH 'polymeal'");
        REQUIRE(q2.has_value());
        auto r2 = q2.value().step();
        REQUIRE(r2.has_value());
        REQUIRE(r2.value()); // Must find 'polymeal'
        spdlog::info("✓ Single term 'polymeal' found");

        // Two terms
        auto q3 = db.prepare("SELECT rowid FROM docs WHERE docs MATCH 'cardiovascular mortality'");
        REQUIRE(q3.has_value());
        auto r3 = q3.value().step();
        REQUIRE(r3.has_value());
        REQUIRE(r3.value());
        spdlog::info("✓ Two terms 'cardiovascular mortality' found");
    }
}
