#include "../../../src/cli/commands/file_history_json.h"
#include "../../../src/cli/commands/get_response_json.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/result_renderer.h>

namespace {
struct CaptureOutput {
    std::ostringstream stream;
    std::streambuf* previous = std::cout.rdbuf(stream.rdbuf());
    ~CaptureOutput() { std::cout.rdbuf(previous); }
};

yams::daemon::GetResponse textResponse() {
    yams::daemon::GetResponse response;
    response.hash = std::string(64, 'a');
    response.fileName = "evidence.md";
    response.path = "/notes/evidence.md";
    response.mimeType = "text/markdown";
    response.content = "Quote: \"measured\"\nsecond line\tvalue\\unit\n";
    response.hasContent = true;
    response.size = response.content.size();
    response.metadata = {{"task", "report"}, {"phase", "checkpoint"}};
    return response;
}
} // namespace

TEST_CASE("Get JSON preserves full identity, metadata and escaped content", "[cli][get][json]") {
    const auto response = textResponse();
    const auto serialized = yams::cli::serializeGetResponse(response, false);
    REQUIRE(serialized);
    const auto document = nlohmann::json::parse(serialized.value());
    CHECK(document.at("schema_version") == 1);
    CHECK(document.at("hash") == response.hash);
    CHECK(document.at("name") == response.fileName);
    CHECK(document.at("path") == response.path);
    CHECK(document.at("content") == response.content);
    CHECK(document.at("metadata") == response.metadata);
    CHECK(document.at("encoding") == "utf-8");
    CHECK(document.at("representation") == "original");
    CHECK(document.at("truncated") == false);
}

TEST_CASE("Get JSON distinguishes absent content from an empty document", "[cli][get][json]") {
    auto response = textResponse();
    SECTION("Missing content is an error") {
        response.hasContent = false;
        CHECK_FALSE(yams::cli::serializeGetResponse(response, false));
    }
    SECTION("Empty content is valid when present") {
        response.content.clear();
        response.size = 0;
        const auto result = yams::cli::serializeGetResponse(response, false);
        REQUIRE(result);
        CHECK(nlohmann::json::parse(result.value()).at("content") == "");
    }
    SECTION("Metadata-only output deliberately omits content") {
        response.hasContent = false;
        const auto result = yams::cli::serializeGetResponse(response, true);
        REQUIRE(result);
        const auto document = nlohmann::json::parse(result.value());
        CHECK(document.at("content").is_null());
        CHECK(document.at("has_content") == false);
        CHECK(document.at("encoding").is_null());
    }
}

TEST_CASE("Get JSON refuses incomplete or non-text evidence", "[cli][get][json]") {
    auto response = textResponse();
    SECTION("A full source hash is required") {
        response.hash.resize(12);
    }
    SECTION("A source hash must be hexadecimal") {
        response.hash.assign(64, 'z');
    }
    SECTION("Partial bytes are not reported as a complete artifact") {
        ++response.size;
    }
    SECTION("Invalid UTF-8 is not silently replaced") {
        response.content[0] = '\xff';
    }
    CHECK_FALSE(yams::cli::serializeGetResponse(response, false));
}

TEST_CASE("JSON results expose content identity and hydration independent of display flags",
          "[cli][retrieval][json]") {
    yams::metadata::DocumentInfo doc;
    doc.id = 42;
    doc.fileName = "evidence.txt";
    doc.sha256Hash = std::string(64, 'a');
    std::vector<yams::metadata::DocumentInfo> documents{doc};
    CaptureOutput output;
    auto renderer = yams::cli::createDocumentRenderer("", yams::cli::OutputFormat::JSON, false);
    renderer.render("evidence", "list", documents);
    const auto result = nlohmann::json::parse(output.stream.str()).at("results").at(0);
    CHECK(result.at("hash") == doc.sha256Hash);
    REQUIRE(result.contains("hydration"));
    CHECK(result.at("hydration").at("hash") == doc.sha256Hash);
    CHECK(result.at("hydration").at("method") == "cat");
}

TEST_CASE("JSON results explicitly report missing content identity", "[cli][retrieval][json]") {
    yams::metadata::DocumentInfo doc;
    doc.fileName = "unindexed.txt";
    std::vector<yams::metadata::DocumentInfo> documents{doc};
    CaptureOutput output;
    yams::cli::createDocumentRenderer("", yams::cli::OutputFormat::JSON)
        .render("unindexed", "list", documents);
    const auto result = nlohmann::json::parse(output.stream.str()).at("results").at(0);
    REQUIRE(result.contains("hash"));
    CHECK(result.at("hash").is_null());
    CHECK(result.at("hydration").is_null());
}

TEST_CASE("File history uses a document envelope for empty and populated responses",
          "[cli][retrieval][json]") {
    yams::daemon::FileHistoryResponse history;
    history.filepath = "/evidence/note.txt";
    auto empty = yams::cli::fileHistoryToJson(history);
    CHECK(empty.at("documents").is_array());
    CHECK(empty.at("documents").empty());
    CHECK(empty.at("total") == 0);
    CHECK(empty.at("found") == false);
    history.found = true;
    history.totalVersions = 3;
    history.versions.push_back({"snapshot-1", std::string(64, 'b'), 123, 456});
    const auto populated = yams::cli::fileHistoryToJson(history);
    CHECK(populated.at("total") == 1);
    CHECK(populated.at("total_versions") == 3);
    const auto& doc = populated.at("documents").at(0);
    CHECK(doc.at("hash") == std::string(64, 'b'));
    CHECK(doc.at("hydration").at("hash") == doc.at("hash"));
    CHECK(doc.at("snapshot_id") == "snapshot-1");
    CHECK(doc.at("indexed") == 456);
}
