// Focused unit coverage for raw text XML helpers and fuzzy matching.

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/api/raw_text_storage.h>

#include <string>
#include <vector>

namespace yams::api::test {

TEST_CASE("RawTextStorage XML escaping preserves literal entities", "[api][raw-text][xml]") {
    const std::string original = R"(Tom & Jerry <tag attr="value">it's &lt; literal</tag>)";

    const auto escaped = xml::escapeXML(original);
    CHECK(escaped.find("&amp;") != std::string::npos);
    CHECK(escaped.find("&lt;tag") != std::string::npos);
    CHECK(escaped.find("&amp;lt; literal") != std::string::npos);

    CHECK(xml::unescapeXML(escaped) == original);
}

TEST_CASE("RawTextStorage entry XML round-trips multiline content and metadata",
          "[api][raw-text][xml]") {
    RawTextMetadata metadata;
    metadata.id = "entry-1";
    metadata.source = "claude & gpt";
    metadata.chatTitle = "Fix <parser>";
    metadata.sessionId = "session-42";
    metadata.userId = "user-a";
    metadata.contentType = "conversation";
    metadata.language = "en";
    metadata.codebase = "tools/yams";
    metadata.filePath = "src/api/raw_text_storage.cpp";
    metadata.userActivity = "testing";
    metadata.llmTask = "round-trip XML";
    metadata.tags = {"coverage", "xml<tag>"};
    metadata.parentId = "parent-0";
    metadata.relatedIds = {"related-1", "related&2"};
    metadata.keywords = {"raw", "storage"};
    metadata.importance = 0.75f;
    metadata.customFields["prompt"] = "fix <parser> & keep entities";
    metadata.customFields["literal"] = "&lt;not-a-tag&gt;";

    RawTextEntry entry;
    entry.content = "line one\nline <two> & data";
    entry.metadata = metadata;
    entry.contentHash = "hash-123";
    entry.contentSize = entry.content.size();

    const auto parsed = RawTextEntry::fromXML(entry.toXML());

    CHECK(parsed.content == entry.content);
    CHECK(parsed.contentHash == entry.contentHash);
    CHECK(parsed.contentSize == entry.contentSize);
    CHECK(parsed.metadata.id == metadata.id);
    CHECK(parsed.metadata.source == metadata.source);
    CHECK(parsed.metadata.chatTitle == metadata.chatTitle);
    CHECK(parsed.metadata.sessionId == metadata.sessionId);
    CHECK(parsed.metadata.userId == metadata.userId);
    CHECK(parsed.metadata.contentType == metadata.contentType);
    CHECK(parsed.metadata.language == metadata.language);
    CHECK(parsed.metadata.codebase == metadata.codebase);
    CHECK(parsed.metadata.filePath == metadata.filePath);
    CHECK(parsed.metadata.userActivity == metadata.userActivity);
    CHECK(parsed.metadata.llmTask == metadata.llmTask);
    CHECK(parsed.metadata.tags == metadata.tags);
    CHECK(parsed.metadata.parentId == metadata.parentId);
    CHECK(parsed.metadata.relatedIds == metadata.relatedIds);
    CHECK(parsed.metadata.keywords == metadata.keywords);
    CHECK(parsed.metadata.importance == Catch::Approx(metadata.importance));
    CHECK(parsed.metadata.customFields == metadata.customFields);
}

TEST_CASE("RawTextStorage fuzzy helpers rank and limit matches", "[api][raw-text][fuzzy]") {
    CHECK(fuzzy::levenshteinDistance("kitten", "sitting") == 3);
    CHECK(fuzzy::levenshteinDistance("", "raw") == 3);
    CHECK(fuzzy::similarityScore("storage", "storage") == Catch::Approx(1.0f));
    CHECK(fuzzy::similarityScore("", "storage") == Catch::Approx(0.0f));

    const std::vector<std::string> candidates = {"search", "storage", "store", "raw"};
    const auto matches = fuzzy::findBestMatches("storag", candidates, 0.5f, 2);

    REQUIRE(matches.size() == 2);
    CHECK(matches[0].second == "storage");
    CHECK(matches[0].first >= matches[1].first);
}

} // namespace yams::api::test
