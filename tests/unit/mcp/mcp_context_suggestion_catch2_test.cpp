#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <filesystem>
#include <memory>

#include <nlohmann/json.hpp>
#include "../common/test_helpers_catch2.h"
#include <yams/mcp/mcp_server.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

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

TEST_CASE("suggest_context suppresses repeated snapshots from the latest matching session event",
          "[mcp][suggest_context][catch2]") {
    auto tempDir = yams::test::make_temp_dir("yams_mcp_suggest_context_suppress_");
    std::filesystem::create_directories(tempDir);

    yams::metadata::ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;
    auto pool = std::make_shared<yams::metadata::ConnectionPool>((tempDir / "yams.db").string(),
                                                                 poolConfig);
    auto init = pool->initialize();
    REQUIRE(init.has_value());

    yams::metadata::MetadataRepository repo(*pool);
    yams::metadata::FeedbackEvent event;
    event.eventId = "evt-1";
    event.traceId = "trace-1";
    event.createdAt =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    event.source = "mcp";
    event.eventType = "suggest_context_served";
    event.payloadJson = json{{"session_name", "session-1"},
                             {"normalized_query", "auth token session"},
                             {"snapshot_ids", json::array({"snap-a"})},
                             {"served_result_ids", json::array({"1", "2"})}}
                            .dump();
    REQUIRE(repo.insertFeedbackEvent(event).has_value());

    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    yams::daemon::ClientConfig cfg;
    cfg.dataDir = tempDir;
    server->testConfigureDaemonClient(cfg);
    server->testSetSuggestContextOverrides(
        {MCPServer::SuggestContextOverride{
             .snapshotId = "snap-a",
             .label = "auth rollout",
             .directoryPath = "/repo/auth",
             .supportingResults = {makeResult("1", "hash-1", "Auth guide", "/repo/auth/guide.md",
                                              1.1f, "login tokens and refresh flow")}},
         MCPServer::SuggestContextOverride{
             .snapshotId = "snap-b",
             .label = "ui polish",
             .directoryPath = "/repo/ui",
             .supportingResults = {
                 makeResult("3", "hash-3", "Button spacing", "/repo/ui/buttons.md", 0.5f)}}});

    boost::asio::io_context io;
    yams::mcp::MCPSuggestContextRequest req;
    req.query = "Auth token session";
    req.limit = 5;
    req.useSession = true;
    req.sessionName = "session-1";

    auto future =
        boost::asio::co_spawn(io, server->testHandleSuggestContext(req), boost::asio::use_future);
    io.run();

    auto result = future.get();
    REQUIRE(result.has_value());
    REQUIRE(result.value().suggestions.size() == 1);
    CHECK(result.value().suggestions[0].snapshotId == "snap-b");

    std::error_code ec;
    std::filesystem::remove_all(tempDir, ec);
}

TEST_CASE("suggest_context records served suggestions and suppresses an immediate repeat",
          "[mcp][suggest_context][catch2]") {
    auto tempDir = yams::test::make_temp_dir("yams_mcp_suggest_context_record_");
    std::filesystem::create_directories(tempDir);

    yams::metadata::ConnectionPoolConfig poolConfig;
    poolConfig.minConnections = 1;
    poolConfig.maxConnections = 2;
    auto pool = std::make_shared<yams::metadata::ConnectionPool>((tempDir / "yams.db").string(),
                                                                 poolConfig);
    auto init = pool->initialize();
    REQUIRE(init.has_value());

    yams::metadata::MetadataRepository repo(*pool);

    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    yams::daemon::ClientConfig cfg;
    cfg.dataDir = tempDir;
    server->testConfigureDaemonClient(cfg);
    server->testSetSuggestContextOverrides({MCPServer::SuggestContextOverride{
        .snapshotId = "snap-a",
        .label = "auth rollout",
        .directoryPath = "/repo/auth",
        .supportingResults = {makeResult("1", "hash-1", "Auth guide", "/repo/auth/guide.md", 1.1f,
                                         "login tokens and refresh flow")}}});

    yams::mcp::MCPSuggestContextRequest req;
    req.query = "auth token session";
    req.limit = 5;
    req.useSession = true;
    req.sessionName = "session-1";

    {
        boost::asio::io_context io;
        auto future = boost::asio::co_spawn(io, server->testHandleSuggestContext(req),
                                            boost::asio::use_future);
        io.run();

        auto result = future.get();
        REQUIRE(result.has_value());
        REQUIRE(result.value().suggestions.size() == 1);
        CHECK(result.value().suggestions[0].snapshotId == "snap-a");
    }

    auto events = repo.getRecentFeedbackEvents(5);
    REQUIRE(events.has_value());
    auto it = std::find_if(events.value().begin(), events.value().end(), [](const auto& event) {
        return event.eventType == "suggest_context_served" && event.source == "mcp";
    });
    REQUIRE(it != events.value().end());
    CHECK(it->payloadJson.find("snap-a") != std::string::npos);

    {
        boost::asio::io_context io;
        auto future = boost::asio::co_spawn(io, server->testHandleSuggestContext(req),
                                            boost::asio::use_future);
        io.run();

        auto result = future.get();
        REQUIRE(result.has_value());
        CHECK(result.value().suggestions.empty());
        CHECK(result.value().total == 0);
    }

    std::error_code ec;
    std::filesystem::remove_all(tempDir, ec);
}
