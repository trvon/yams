// Unit tests for yams::mcp::ModeRouter. Exercises the shared dispatch loop with
// synthetic op tables and fake dispatch callbacks — no MCPServer instance required.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <future>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <yams/daemon/client/global_io_context.h>
#include <yams/mcp/mode_router.h>

using namespace std::chrono_literals;
using json = nlohmann::json;
using Catch::Matchers::ContainsSubstring;
using yams::mcp::ModeRouter;
using yams::mcp::ModeRouterConfig;
using yams::mcp::ModeRouterNormalizeResult;
using yams::mcp::ModeRouterStepResult;

namespace {

json runRouter(const ModeRouter& router, const json& args, std::string_view proto = "2024-11-05",
               bool limitDup = false, std::chrono::milliseconds timeout = 3s) {
    auto& io = yams::daemon::GlobalIOContext::instance().get_io_context();
    auto fut = boost::asio::co_spawn(
        io,
        [&]() -> boost::asio::awaitable<json> {
            co_return co_await router.handle(args, proto, limitDup);
        },
        boost::asio::use_future);
    if (fut.wait_for(timeout) != std::future_status::ready) {
        return json{{"error", "timeout"}};
    }
    return fut.get();
}

ModeRouterConfig makeExecuteCfg(ModeRouterConfig::DispatchFn dispatch,
                                ModeRouterConfig::NormalizeFn normalize = {}) {
    ModeRouterConfig cfg;
    cfg.kind = "write";
    cfg.stepsKey = "operations";
    cfg.summaryPrefix = "Execute";
    cfg.invalidOpHint = "Use 'query' for reads.";
    cfg.opTable = {{"add", "add"}, {"update", "update"}};
    cfg.normalize = std::move(normalize);
    cfg.dispatch = std::move(dispatch);
    cfg.stepProjector = yams::mcp::projections::executeStepProjection;
    cfg.finalResultBuilder = yams::mcp::projections::executeFinalResult;
    return cfg;
}

ModeRouterStepResult makeOk(std::string op, std::size_t i, json data = json::object()) {
    ModeRouterStepResult r;
    r.stepIndex = i;
    r.op = std::move(op);
    r.isError = false;
    r.data = std::move(data);
    return r;
}

ModeRouterStepResult makeErr(std::string op, std::size_t i, std::string msg) {
    ModeRouterStepResult r;
    r.stepIndex = i;
    r.op = std::move(op);
    r.isError = true;
    r.primaryError = msg;
    r.data = json{{"error", std::move(msg)}};
    return r;
}

} // namespace

TEST_CASE("ModeRouter rejects missing stepsKey", "[mcp][mode_router]") {
    ModeRouter router(
        makeExecuteCfg([](std::string, std::string, json,
                          std::size_t) -> boost::asio::awaitable<ModeRouterStepResult> {
            co_return makeOk("add", 0);
        }));

    auto result = runRouter(router, json::object());
    REQUIRE(result.value("isError", false));
    const auto& text = result["content"][0]["text"].get_ref<const std::string&>();
    REQUIRE_THAT(text, ContainsSubstring("operations"));
}

TEST_CASE("ModeRouter rejects empty steps array", "[mcp][mode_router]") {
    ModeRouter router(
        makeExecuteCfg([](std::string, std::string, json,
                          std::size_t) -> boost::asio::awaitable<ModeRouterStepResult> {
            co_return makeOk("add", 0);
        }));

    auto result = runRouter(router, json{{"operations", json::array()}});
    REQUIRE(result.value("isError", false));
}

TEST_CASE("ModeRouter dispatches valid batch and aggregates", "[mcp][mode_router]") {
    std::vector<std::string> calls;
    auto dispatch = [&calls](std::string op, std::string /*target*/, json /*params*/,
                             std::size_t i) -> boost::asio::awaitable<ModeRouterStepResult> {
        calls.push_back(op);
        co_return makeOk(op, i, json{{"ok", true}});
    };

    ModeRouter router(makeExecuteCfg(dispatch));

    json args = {{"operations", json::array({
                                    json{{"op", "add"}, {"params", json{{"path", "a"}}}},
                                    json{{"op", "update"}, {"params", json{{"path", "b"}}}},
                                })}};

    auto result = runRouter(router, args);
    REQUIRE_FALSE(result.value("isError", false));
    REQUIRE(calls.size() == 2);

    const auto& data = result["structuredContent"]["data"];
    REQUIRE(data["succeeded"].get<int>() == 2);
    REQUIRE(data["failed"].get<int>() == 0);
    REQUIRE(data["totalOps"].get<int>() == 2);
    REQUIRE(data["results"].size() == 2);
    REQUIRE(data["results"][0]["success"].get<bool>());
    REQUIRE(data["results"][0]["op"].get<std::string>() == "add");
}

TEST_CASE("ModeRouter flags unknown op with kind-specific hint", "[mcp][mode_router]") {
    ModeRouter router(
        makeExecuteCfg([](std::string, std::string, json,
                          std::size_t) -> boost::asio::awaitable<ModeRouterStepResult> {
            co_return makeOk("add", 0);
        }));

    json args = {{"operations", json::array({json{{"op", "search"}, {"params", json::object()}}})}};
    auto result = runRouter(router, args);
    REQUIRE(result.value("isError", false));

    const auto& data = result["structuredContent"]["data"];
    REQUIRE(data["failed"].get<int>() == 1);
    const auto& err = data["results"][0]["error"].get_ref<const std::string&>();
    REQUIRE_THAT(err, ContainsSubstring("'search' is not a write operation"));
    REQUIRE_THAT(err, ContainsSubstring("Use 'query' for reads."));
}

TEST_CASE("ModeRouter short-circuits on normalize error", "[mcp][mode_router]") {
    bool dispatched = false;
    auto normalize = [](const std::string&, json p) {
        return ModeRouterNormalizeResult{std::move(p), std::string("bad input")};
    };
    auto dispatch = [&dispatched](std::string op, std::string, json,
                                  std::size_t i) -> boost::asio::awaitable<ModeRouterStepResult> {
        dispatched = true;
        co_return makeOk(op, i);
    };

    ModeRouter router(makeExecuteCfg(dispatch, normalize));

    json args = {{"operations", json::array({json{{"op", "add"}, {"params", json::object()}}})}};
    auto result = runRouter(router, args);
    REQUIRE(result.value("isError", false));
    REQUIRE_FALSE(dispatched);
    REQUIRE(result["structuredContent"]["data"]["results"][0]["error"].get<std::string>() ==
            "bad input");
}

TEST_CASE("ModeRouter halts on first failure by default", "[mcp][mode_router]") {
    int calls = 0;
    auto dispatch = [&calls](std::string op, std::string, json,
                             std::size_t i) -> boost::asio::awaitable<ModeRouterStepResult> {
        ++calls;
        if (op == "add") {
            co_return makeErr("add", i, "boom");
        }
        co_return makeOk(op, i);
    };

    ModeRouter router(makeExecuteCfg(dispatch));

    json args = {{"operations", json::array({
                                    json{{"op", "add"}, {"params", json::object()}},
                                    json{{"op", "update"}, {"params", json::object()}},
                                })}};
    auto result = runRouter(router, args);
    REQUIRE(result.value("isError", false));
    REQUIRE(calls == 1);
    REQUIRE(result["structuredContent"]["data"]["results"].size() == 1);
}

TEST_CASE("ModeRouter continues on error when asked", "[mcp][mode_router]") {
    int calls = 0;
    auto dispatch = [&calls](std::string op, std::string, json,
                             std::size_t i) -> boost::asio::awaitable<ModeRouterStepResult> {
        ++calls;
        if (op == "add") {
            co_return makeErr("add", i, "boom");
        }
        co_return makeOk(op, i);
    };

    ModeRouter router(makeExecuteCfg(dispatch));

    json args = {{"operations", json::array({
                                    json{{"op", "add"}, {"params", json::object()}},
                                    json{{"op", "update"}, {"params", json::object()}},
                                })},
                 {"continueOnError", true}};
    auto result = runRouter(router, args);
    REQUIRE(calls == 2);
    const auto& data = result["structuredContent"]["data"];
    REQUIRE(data["succeeded"].get<int>() == 1);
    REQUIRE(data["failed"].get<int>() == 1);
}

TEST_CASE("ModeRouter query projection returns steps/totalSteps/completedSteps",
          "[mcp][mode_router]") {
    ModeRouterConfig cfg;
    cfg.kind = "read";
    cfg.stepsKey = "steps";
    cfg.summaryPrefix = "Pipeline";
    cfg.opTable = {{"search", "search"}};
    cfg.dispatch = [](std::string op, std::string, json,
                      std::size_t i) -> boost::asio::awaitable<ModeRouterStepResult> {
        co_return makeOk(op, i, json{{"hits", 0}});
    };
    cfg.stepProjector = yams::mcp::projections::queryStepProjection;
    cfg.finalResultBuilder = yams::mcp::projections::queryFinalResult;

    ModeRouter router(std::move(cfg));

    json args = {{"steps", json::array({json{{"op", "search"}, {"params", json::object()}}})}};
    auto result = runRouter(router, args);
    REQUIRE_FALSE(result.value("isError", false));
    const auto& data = result["structuredContent"]["data"];
    REQUIRE(data.contains("steps"));
    REQUIRE(data["totalSteps"].get<int>() == 1);
    REQUIRE(data["completedSteps"].get<int>() == 1);
    REQUIRE_FALSE(data["steps"][0]["isError"].get<bool>());
}
