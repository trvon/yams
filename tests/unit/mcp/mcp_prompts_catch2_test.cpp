// MCP prompt subsystem behavior-lock tests (WS1 / TI2)

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_set>

#include <nlohmann/json.hpp>
#include <yams/core/uuid.h>
#include <yams/mcp/mcp_server.h>

using namespace yams::mcp;
using json = nlohmann::json;

namespace {

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

class TempDirGuard {
public:
    TempDirGuard() {
        auto base = std::filesystem::temp_directory_path();
        dir_ = base / ("yams-mcp-prompts-test-" + yams::core::generateUUID());
        std::filesystem::create_directories(dir_);
    }

    ~TempDirGuard() {
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }

    const std::filesystem::path& path() const { return dir_; }

private:
    std::filesystem::path dir_;
};

} // namespace

TEST_CASE("MCP prompts/list includes codex repo session prompt", "[mcp][prompts][list][catch2]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

    json req = {
        {"jsonrpc", "2.0"}, {"id", 1}, {"method", "prompts/list"}, {"params", json::object()}};
    auto mr = server->handleRequestPublic(req);
    REQUIRE(mr.has_value());
    json out = mr.value();
    REQUIRE(out.is_object());
    REQUIRE(out.contains("result"));
    auto result = out["result"];

    REQUIRE(result.is_object());
    REQUIRE(result.contains("prompts"));
    REQUIRE(result["prompts"].is_array());

    std::unordered_set<std::string> names;
    for (const auto& p : result["prompts"]) {
        if (p.is_object() && p.contains("name") && p["name"].is_string()) {
            names.insert(p["name"].get<std::string>());
        }
    }

    CHECK(names.count("search_codebase") == 1);
    CHECK(names.count("session/codex_repo") == 1);
}

TEST_CASE("MCP prompts/get builtins return MCP message schema",
          "[mcp][prompts][get][schema][catch2]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

    json req = {{"jsonrpc", "2.0"},
                {"id", 2},
                {"method", "prompts/get"},
                {"params",
                 {{"name", "search_codebase"},
                  {"arguments", {{"pattern", "listPrompts"}, {"file_type", "cpp"}}}}}};

    auto mr = server->handleRequestPublic(req);
    REQUIRE(mr.has_value());
    json out = mr.value();
    REQUIRE(out.is_object());
    REQUIRE(out.contains("result"));
    auto result = out["result"];
    REQUIRE(result.is_object());
    REQUIRE(result.contains("messages"));
    REQUIRE(result["messages"].is_array());
    REQUIRE(result["messages"].size() >= 2);

    const auto& first = result["messages"][0];
    const auto& second = result["messages"][1];
    REQUIRE(first["role"].is_string());
    REQUIRE(second["role"].is_string());
    CHECK(first["role"] == "assistant");
    CHECK(second["role"] == "user");
    REQUIRE(first.contains("content"));
    REQUIRE(first["content"].is_object());
    CHECK(first["content"]["type"] == "text");
    REQUIRE(second.contains("content"));
    REQUIRE(second["content"].is_object());
    CHECK(second["content"]["type"] == "text");
}

TEST_CASE("MCP prompts/get session/codex_repo references eng_codex and AGENTS",
          "[mcp][prompts][get][codex][catch2]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

    json req = {{"jsonrpc", "2.0"},
                {"id", 3},
                {"method", "prompts/get"},
                {"params", {{"name", "session/codex_repo"}, {"arguments", json::object()}}}};

    auto mr = server->handleRequestPublic(req);
    REQUIRE(mr.has_value());
    json out = mr.value();
    REQUIRE(out.is_object());
    REQUIRE(out.contains("result"));
    auto result = out["result"];
    REQUIRE(result["messages"].is_array());
    REQUIRE(result["messages"].size() >= 2);

    const auto& userMessage = result["messages"][1]["content"]["text"];
    REQUIRE(userMessage.is_string());
    const std::string text = userMessage.get<std::string>();
    CHECK(text.find("eng_codex") != std::string::npos);
    CHECK(text.find("AGENTS.md") != std::string::npos);
}

TEST_CASE("MCP prompts/get supports file-backed prompt fallback",
          "[mcp][prompts][filebacked][catch2]") {
    TempDirGuard tmp;
    const auto promptPath = tmp.path() / "PROMPT-foo-bar.md";

    {
        std::ofstream out(promptPath);
        REQUIRE(out.good());
        out << "# Test Prompt\n\nUse this file-backed prompt.\n";
    }

    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());
    server->testSetPromptsDir(tmp.path());

    json listReq = {
        {"jsonrpc", "2.0"}, {"id", 4}, {"method", "prompts/list"}, {"params", json::object()}};
    auto listMr = server->handleRequestPublic(listReq);
    REQUIRE(listMr.has_value());
    json listOut = listMr.value();
    REQUIRE(listOut.is_object());
    REQUIRE(listOut.contains("result"));
    auto listResult = listOut["result"];
    REQUIRE(listResult["prompts"].is_array());

    bool foundFilePrompt = false;
    for (const auto& p : listResult["prompts"]) {
        if (p.is_object() && p.value("name", "") == "foo_bar") {
            foundFilePrompt = true;
            CHECK(p.value("description", "") == "Test Prompt");
            break;
        }
    }
    CHECK(foundFilePrompt);

    json getReq = {{"jsonrpc", "2.0"},
                   {"id", 5},
                   {"method", "prompts/get"},
                   {"params", {{"name", "foo_bar"}, {"arguments", json::object()}}}};
    auto getMr = server->handleRequestPublic(getReq);
    REQUIRE(getMr.has_value());
    json getOut = getMr.value();
    REQUIRE(getOut.is_object());
    REQUIRE(getOut.contains("result"));
    auto getResult = getOut["result"];
    REQUIRE(getResult["messages"].is_array());
    REQUIRE(getResult["messages"].size() == 1);
    CHECK(getResult["messages"][0]["role"] == "assistant");
    CHECK(getResult["messages"][0]["content"]["type"] == "text");
    const std::string text = getResult["messages"][0]["content"]["text"].get<std::string>();
    CHECK(text.find("Use this file-backed prompt.") != std::string::npos);
}
