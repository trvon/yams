#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/mcp/mcp_server.h>

#if defined(_WIN32)
#include <cstdlib>
static int setenv(const char* name, const char* value, int) {
    return _putenv_s(name, value);
}
static int unsetenv(const char* name) {
    return _putenv_s(name, "");
}
#else
#include <cstdlib>
#endif

using json = nlohmann::json;
using yams::ErrorCode;
using yams::mcp::ITransport;
using yams::mcp::MCPServer;
using yams::mcp::MessageResult;
using yams::mcp::TransportState;

namespace {

class NullTransport : public ITransport {
public:
    void send(const json&) override {}
    MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NotImplemented, "Null transport"};
    }
    bool isConnected() const override { return false; }
    void close() override {}
    TransportState getState() const override { return TransportState::Disconnected; }
};

class EnvGuard {
public:
    EnvGuard(const char* key, const char* value) : key_(key) {
        if (const char* existing = std::getenv(key_)) {
            previous_ = std::string(existing);
        }
        if (value) {
            (void)::setenv(key_, value, 1);
        } else {
            (void)::unsetenv(key_);
        }
    }

    ~EnvGuard() {
        if (previous_) {
            (void)::setenv(key_, previous_->c_str(), 1);
        } else {
            (void)::unsetenv(key_);
        }
    }

private:
    const char* key_;
    std::optional<std::string> previous_;
};

class SpdlogLevelGuard {
public:
    SpdlogLevelGuard() : previous_(spdlog::get_level()) {}
    ~SpdlogLevelGuard() { spdlog::set_level(previous_); }

private:
    spdlog::level::level_enum previous_;
};

} // namespace

TEST_CASE("MCP routing notifications return notification sentinel errors",
          "[mcp][routing][notifications][catch2]") {
    auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

    SECTION("notifications/initialized") {
        json req = {{"jsonrpc", "2.0"},
                    {"method", "notifications/initialized"},
                    {"params", json::object()}};
        auto mr = server->handleRequestPublic(req);
        REQUIRE_FALSE(mr.has_value());
        CHECK(mr.error().code == ErrorCode::Success);
        CHECK(mr.error().message == "notification");
    }

    SECTION("notifications/cancelled accepts requestId") {
        json req = {{"jsonrpc", "2.0"},
                    {"method", "notifications/cancelled"},
                    {"params", {{"requestId", 123}}}};
        auto mr = server->handleRequestPublic(req);
        REQUIRE_FALSE(mr.has_value());
        CHECK(mr.error().code == ErrorCode::Success);
        CHECK(mr.error().message == "notification");
    }

    SECTION("notifications/cancelled rejects missing id") {
        json req = {
            {"jsonrpc", "2.0"}, {"method", "notifications/cancelled"}, {"params", json::object()}};
        auto mr = server->handleRequestPublic(req);
        REQUIRE_FALSE(mr.has_value());
        CHECK(mr.error().code == ErrorCode::InvalidArgument);
    }
}

TEST_CASE("MCP routing logging/setLevel honors extension gating",
          "[mcp][routing][logging][extensions][catch2]") {
    SpdlogLevelGuard levelGuard;

    SECTION("enabled by default returns success result") {
        EnvGuard clearExt("YAMS_DISABLE_EXTENSIONS", nullptr);
        auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

        json req = {{"jsonrpc", "2.0"},
                    {"id", 7},
                    {"method", "logging/setLevel"},
                    {"params", {{"level", "debug"}}}};
        auto mr = server->handleRequestPublic(req);
        REQUIRE(mr.has_value());
        auto out = mr.value();
        REQUIRE(out.is_object());
        CHECK(out["jsonrpc"] == "2.0");
        CHECK(out["id"] == 7);
        REQUIRE(out.contains("result"));
        CHECK(out["result"].is_object());
    }

    SECTION("disabled returns method not available error response") {
        EnvGuard disableExt("YAMS_DISABLE_EXTENSIONS", "1");
        auto server = std::make_shared<MCPServer>(std::make_unique<NullTransport>());

        json req = {{"jsonrpc", "2.0"},
                    {"id", 8},
                    {"method", "logging/setLevel"},
                    {"params", {{"level", "warn"}}}};
        auto mr = server->handleRequestPublic(req);
        REQUIRE(mr.has_value());
        auto out = mr.value();
        REQUIRE(out.is_object());
        CHECK(out["jsonrpc"] == "2.0");
        CHECK(out["id"] == 8);
        REQUIRE(out.contains("error"));
        CHECK(out["error"]["code"] == -32601);
        REQUIRE(out["error"]["message"].is_string());
        CHECK(out["error"]["message"].get<std::string>().find("extensions disabled") !=
              std::string::npos);
    }
}
