#include "../../../src/cli/commands/file_history_json.h"
#include <catch2/catch_test_macros.hpp>
#include <yams/cli/result_renderer.h>

namespace {
struct CaptureOutput {
    std::ostringstream stream;
    std::streambuf* previous = std::cout.rdbuf(stream.rdbuf());
    ~CaptureOutput() { std::cout.rdbuf(previous); }
};
} // namespace

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
