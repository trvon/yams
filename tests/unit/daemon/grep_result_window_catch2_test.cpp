#include <catch2/catch_test_macros.hpp>
#include <any>

#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/grep_result_window.h>

// This focused executable does not link the daemon client's runtime. App service headers
// transitively instantiate the global initializer, so provide inert lifecycle definitions.
namespace yams::daemon {
GlobalIOContextInitializer::GlobalIOContextInitializer() = default;
GlobalIOContextInitializer::~GlobalIOContextInitializer() = default;
} // namespace yams::daemon

namespace {

// Exercise the legacy field contract without depending on the protobuf codec.
struct RecordedFields {
    std::vector<std::any> values;
    std::size_t offset{0};
    template <typename T> RecordedFields& operator<<(const T& value) {
        values.emplace_back(value);
        return *this;
    }
    template <typename T> yams::Result<T> read() {
        if (offset >= values.size())
            return yams::ErrorCode::InvalidData;
        const auto* value = std::any_cast<T>(&values[offset]);
        if (!value)
            return yams::ErrorCode::InvalidData;
        ++offset;
        return *value;
    }
    yams::Result<std::string> readString() { return read<std::string>(); }
    yams::Result<std::vector<std::string>> readStringVector() {
        return read<std::vector<std::string>>();
    }
    yams::Result<std::map<std::string, std::string>> readStringMap() {
        return read<std::map<std::string, std::string>>();
    }
};

yams::app::services::GrepMatch makeMatch(std::size_t lineNumber, std::string line) {
    yams::app::services::GrepMatch match;
    match.lineNumber = lineNumber;
    match.line = std::move(line);
    match.matchType = "regex";
    return match;
}

yams::app::services::GrepFileResult
makeFileResult(std::string file, std::vector<yams::app::services::GrepMatch> matches) {
    yams::app::services::GrepFileResult result;
    result.file = std::move(file);
    result.matches = std::move(matches);
    result.matchCount = result.matches.size();
    return result;
}

} // namespace

TEST_CASE("Legacy grep response appends identity without changing match layout",
          "[daemon][grep][identity][serialization]") {
    yams::daemon::GrepResponse response;
    yams::daemon::GrepMatch match;
    match.file = "evidence.txt";
    match.hash = std::string(64, 'a');
    response.matches = {match, match};
    RecordedFields fields;
    response.serialize(fields);
    bool legacy = false;
    SECTION("New response roundtrips hashes") {}
    SECTION("Older response remains readable without hashes") {
        fields.values.pop_back(); // Old response ends at pathsOnly.
        legacy = true;
    }
    const auto result = yams::daemon::GrepResponse::deserialize(fields);
    REQUIRE(result);
    REQUIRE(result.value().matches.size() == 2);
    for (const auto& decoded : result.value().matches) {
        CHECK(decoded.file == match.file);
        CHECK(decoded.hash == (legacy ? std::string{} : match.hash));
    }
}

TEST_CASE("Grep result window preserves immutable revisions at the same path",
          "[daemon][grep][result-window][identity]") {
    auto first = makeFileResult("evidence.txt", {makeMatch(1, "same content")});
    first.hash = std::string(64, 'a');
    auto second = first;
    second.hash = std::string(64, 'b');
    const auto selected = yams::daemon::grep_result_window::select({first, second, first}, 0);
    REQUIRE(selected.matches.size() == 2);
    CHECK(selected.matches[0].hash == first.hash);
    CHECK(selected.matches[1].hash == second.hash);
}

TEST_CASE("Grep result window deduplicates identities before applying its total cap",
          "[daemon][grep][result-window]") {
    const std::vector fileResults{
        makeFileResult("a.cpp", {makeMatch(10, "  alpha   beta "), makeMatch(10, "alpha beta"),
                                 makeMatch(11, "second")}),
        makeFileResult("b.cpp", {makeMatch(4, "third"), makeMatch(5, "fourth")}),
    };

    const auto selection = yams::daemon::grep_result_window::select(fileResults, 3);

    CHECK(selection.stats.inputMatches == 5);
    CHECK(selection.stats.uniqueMatches == 4);
    CHECK(selection.stats.emittedMatches == 3);
    REQUIRE(selection.matches.size() == 3);
    CHECK(selection.matches[0].file == "a.cpp");
    CHECK(selection.matches[0].lineNumber == 10);
    CHECK(selection.matches[0].line == "  alpha   beta ");
    CHECK(selection.matches[1].lineNumber == 11);
    CHECK(selection.matches[2].file == "b.cpp");
    CHECK(selection.matches[2].lineNumber == 4);
}

TEST_CASE("Grep result window retains equal text at distinct evidence locations",
          "[daemon][grep][result-window]") {
    const std::vector fileResults{
        makeFileResult("a.cpp", {makeMatch(10, "same"), makeMatch(11, "same")}),
        makeFileResult("b.cpp", {makeMatch(10, "same")}),
    };

    const auto selection = yams::daemon::grep_result_window::select(fileResults, 0);

    CHECK(selection.stats.inputMatches == 3);
    CHECK(selection.stats.uniqueMatches == 3);
    CHECK(selection.stats.emittedMatches == 3);
    CHECK(selection.matches.size() == 3);
}

TEST_CASE("Grep result window rejects binary-like indexed lines", "[daemon][grep][result-window]") {
    std::string binaryLike{"prefix"};
    binaryLike.push_back('\0');
    binaryLike += "suffix";
    const std::vector fileResults{
        makeFileResult("generated.sarif", {makeMatch(1, std::move(binaryLike))}),
        makeFileResult("source.cpp", {makeMatch(2, "readable evidence")}),
    };

    const auto selection = yams::daemon::grep_result_window::select(fileResults, 20);

    CHECK(selection.stats.inputMatches == 2);
    REQUIRE(selection.matches.size() == 1);
    CHECK(selection.matches.front().file == "source.cpp");
}
