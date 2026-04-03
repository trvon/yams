#include <catch2/catch_test_macros.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <nlohmann/json.hpp>
#include <yams/mcp/mcp_server.h>

namespace {

using json = nlohmann::json;
using yams::mcp::MCPServer;

class NullTransport : public yams::mcp::ITransport {
public:
    void send(const json&) override {}
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NotImplemented, "Null transport"};
    }
    bool isConnected() const override { return false; }
    void close() override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Disconnected;
    }
};

yams::mcp::MCPSearchResponse::Result makeResult(std::string id, std::string hash, std::string title,
                                                std::string path, float score,
                                                std::string snippet = {}) {
    yams::mcp::MCPSearchResponse::Result result;
    result.id = std::move(id);
    result.hash = std::move(hash);
    result.title = std::move(title);
    result.path = std::move(path);
    result.score = score;
    result.snippet = std::move(snippet);
    return result;
}

} // namespace

TEST_CASE("suggest_context ranks snapshot groups by accumulated evidence",
          "[mcp][suggest_context][catch2]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    server->testSetSuggestContextOverrides(
        {MCPServer::SuggestContextOverride{
             .snapshotId = "snap-a",
             .label = "auth rollout",
             .directoryPath = "/repo/auth",
             .supportingResults = {makeResult("1", "hash-1", "Auth guide", "/repo/auth/guide.md",
                                              1.1f, "login tokens and refresh flow"),
                                   makeResult("2", "hash-2", "Auth ADR", "/repo/auth/adr.md", 0.8f,
                                              "session and token design")}},
         MCPServer::SuggestContextOverride{
             .snapshotId = "snap-b",
             .label = "ui polish",
             .directoryPath = "/repo/ui",
             .supportingResults = {
                 makeResult("3", "hash-3", "Button spacing", "/repo/ui/buttons.md", 0.5f)}}});

    boost::asio::io_context io;
    yams::mcp::MCPSuggestContextRequest req;
    req.query = "auth token session";
    req.limit = 5;

    auto future =
        boost::asio::co_spawn(io, server->testHandleSuggestContext(req), boost::asio::use_future);
    io.run();

    auto result = future.get();
    REQUIRE(result.has_value());
    REQUIRE(result.value().suggestions.size() == 2);
    CHECK(result.value().suggestions[0].snapshotId == "snap-a");
    CHECK(result.value().suggestions[0].supportingResultCount == 2);
    CHECK(result.value().suggestions[0].score > result.value().suggestions[1].score);
}

TEST_CASE("suggest_context respects limit in response", "[mcp][suggest_context][catch2]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    server->testSetSuggestContextOverrides(
        {MCPServer::SuggestContextOverride{
             .snapshotId = "snap-a",
             .label = "alpha",
             .directoryPath = "/repo/a",
             .supportingResults = {makeResult("1", "hash-1", "Alpha", "/repo/a.md", 1.0f)}},
         MCPServer::SuggestContextOverride{
             .snapshotId = "snap-b",
             .label = "beta",
             .directoryPath = "/repo/b",
             .supportingResults = {makeResult("2", "hash-2", "Beta", "/repo/b.md", 0.9f)}}});

    boost::asio::io_context io;
    yams::mcp::MCPSuggestContextRequest req;
    req.query = "alpha beta";
    req.limit = 1;

    auto future =
        boost::asio::co_spawn(io, server->testHandleSuggestContext(req), boost::asio::use_future);
    io.run();

    auto result = future.get();
    REQUIRE(result.has_value());
    CHECK(result.value().total == 1);
    REQUIRE(result.value().suggestions.size() == 1);
}

TEST_CASE("suggest_context rejects empty query", "[mcp][suggest_context][catch2]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

    boost::asio::io_context io;
    yams::mcp::MCPSuggestContextRequest req;
    req.query = "   ";

    auto future =
        boost::asio::co_spawn(io, server->testHandleSuggestContext(req), boost::asio::use_future);
    io.run();

    auto result = future.get();
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().message == "query is required");
}
