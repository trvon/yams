#pragma once

#include <boost/asio/awaitable.hpp>
#include <nlohmann/json.hpp>

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace yams::mcp {

using json = nlohmann::json;

struct ModeRouterNormalizeResult {
    json params = json::object();
    std::optional<std::string> error;
};

struct ModeRouterStepResult {
    std::size_t stepIndex = 0;
    std::string op;
    bool isError = false;
    std::optional<std::string> primaryError;
    json data = json::object();
};

struct ModeRouterConfig {
    using NormalizeFn =
        std::function<ModeRouterNormalizeResult(const std::string& op, json params)>;
    using DispatchFn = std::function<boost::asio::awaitable<ModeRouterStepResult>(
        std::string op, std::string target, json params, std::size_t stepIndex)>;
    using StepProjectorFn = std::function<json(const ModeRouterStepResult&)>;
    using FinalResultBuilderFn = std::function<json(json projectedSteps, std::size_t totalOps,
                                                    std::size_t succeeded, std::size_t failed)>;

    std::string kind;
    std::unordered_map<std::string, std::string> opTable;
    std::string stepsKey;
    std::string invalidOpHint;
    std::string summaryPrefix = "Batch";

    NormalizeFn normalize;
    DispatchFn dispatch;
    StepProjectorFn stepProjector;
    FinalResultBuilderFn finalResultBuilder;

    bool defaultContinueOnError = false;
    bool singleStepUnwrap = false;
};

class ModeRouter {
public:
    explicit ModeRouter(ModeRouterConfig config);

    [[nodiscard]] boost::asio::awaitable<json>
    handle(const json& args, std::string_view protocolVersion, bool limitToolResultDup) const;

    [[nodiscard]] const ModeRouterConfig& config() const noexcept { return config_; }

private:
    ModeRouterConfig config_;
};

namespace projections {

json executeStepProjection(const ModeRouterStepResult& step);
json executeFinalResult(json projectedSteps, std::size_t totalOps, std::size_t succeeded,
                        std::size_t failed);

json queryStepProjection(const ModeRouterStepResult& step);
json queryFinalResult(json projectedSteps, std::size_t totalOps, std::size_t succeeded,
                      std::size_t failed);

} // namespace projections

} // namespace yams::mcp
