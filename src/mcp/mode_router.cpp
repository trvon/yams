#include <yams/mcp/mode_router.h>

#include <yams/mcp/tool_registry.h>

#include <sstream>
#include <utility>

namespace yams::mcp {

namespace {

json extractStepParams(const json& step) {
    json params = step.value("params", json::object());
    if (!params.is_object()) {
        params = json::object();
    }
    if (!step.is_object()) {
        return params;
    }
    for (const auto& [key, value] : step.items()) {
        if (key == "op" || key == "params") {
            continue;
        }
        if (!params.contains(key)) {
            params[key] = value;
        }
    }
    return params;
}

std::string buildFirstLine(std::string summary, const std::optional<std::string>& primaryError) {
    if (!primaryError || primaryError->empty()) {
        return summary;
    }
    std::string out = std::move(summary);
    out.append(" | ");
    out.append(*primaryError);
    return out;
}

json errorResultWrap(const std::string& message) {
    return wrapToolResultStructured(json::array({content::text(message)}), std::nullopt, true);
}

} // namespace

ModeRouter::ModeRouter(ModeRouterConfig config) : config_(std::move(config)) {}

boost::asio::awaitable<json> ModeRouter::handle(const json& args, std::string_view protocolVersion,
                                                bool limitToolResultDup) const {
    try {
        if (!config_.dispatch) {
            co_return errorResultWrap("ModeRouter: dispatch function not configured");
        }
        if (config_.stepsKey.empty()) {
            co_return errorResultWrap("ModeRouter: stepsKey not configured");
        }

        if (!args.contains(config_.stepsKey) || !args.at(config_.stepsKey).is_array()) {
            co_return errorResultWrap("Error: '" + config_.stepsKey + "' array is required");
        }

        const auto& steps = args.at(config_.stepsKey);
        if (steps.empty()) {
            co_return errorResultWrap("Error: '" + config_.stepsKey + "' array must not be empty");
        }

        const bool continueOnError = args.value("continueOnError", config_.defaultContinueOnError);

        json projectedSteps = json::array();
        std::size_t succeeded = 0;
        std::size_t failed = 0;

        const auto& stepProjector =
            config_.stepProjector ? config_.stepProjector : projections::executeStepProjection;
        const auto& finalResultBuilder = config_.finalResultBuilder
                                             ? config_.finalResultBuilder
                                             : projections::executeFinalResult;

        auto projectFailure = [&stepProjector, &projectedSteps,
                               &failed](std::size_t stepIndex, std::string op, std::string error) {
            ModeRouterStepResult step;
            step.stepIndex = stepIndex;
            step.op = std::move(op);
            step.isError = true;
            step.primaryError = error;
            step.data = json{{"error", std::move(error)}};
            projectedSteps.push_back(stepProjector(step));
            ++failed;
        };

        for (std::size_t i = 0; i < steps.size(); ++i) {
            const auto& step = steps[i];
            if (!step.is_object() || !step.contains("op")) {
                projectFailure(i, "unknown", "step " + std::to_string(i) + " missing 'op' field");
                if (!continueOnError) {
                    break;
                }
                continue;
            }

            std::string op = step["op"].get<std::string>();
            auto toolIt = config_.opTable.find(op);
            if (toolIt == config_.opTable.end()) {
                std::string msg = "'" + op + "' is not a " + config_.kind + " operation";
                if (!config_.invalidOpHint.empty()) {
                    msg.append(". ");
                    msg.append(config_.invalidOpHint);
                }
                projectFailure(i, std::move(op), std::move(msg));
                if (!continueOnError) {
                    break;
                }
                continue;
            }

            json params = extractStepParams(step);

            if (config_.normalize) {
                auto normalized = config_.normalize(op, std::move(params));
                if (normalized.error) {
                    projectFailure(i, std::move(op), *normalized.error);
                    if (!continueOnError) {
                        break;
                    }
                    continue;
                }
                params = std::move(normalized.params);
            }

            std::string target = toolIt->second;
            auto stepResult =
                co_await config_.dispatch(std::move(op), std::move(target), std::move(params), i);
            stepResult.stepIndex = i;

            if (stepResult.isError) {
                ++failed;
            } else {
                ++succeeded;
            }

            projectedSteps.push_back(stepProjector(stepResult));

            if (stepResult.isError && !continueOnError) {
                break;
            }
        }

        if (config_.singleStepUnwrap && steps.size() == 1 && projectedSteps.size() == 1) {
            const auto& onlyStep = projectedSteps[0];
            json unwrapped;
            if (onlyStep.is_object() && onlyStep.contains("result")) {
                unwrapped = onlyStep["result"];
            } else if (onlyStep.is_object() && onlyStep.contains("data")) {
                unwrapped = onlyStep["data"];
            } else {
                unwrapped = onlyStep;
            }

            std::ostringstream summary;
            summary << config_.summaryPrefix << ": 1/1 steps";

            std::optional<std::string> primaryError;
            if (failed > 0 && onlyStep.is_object()) {
                if (onlyStep.contains("error") && onlyStep["error"].is_string()) {
                    primaryError = onlyStep["error"].get<std::string>();
                } else if (unwrapped.is_object() && unwrapped.contains("error") &&
                           unwrapped["error"].is_string()) {
                    primaryError = unwrapped["error"].get<std::string>();
                }
            }

            json contentItems =
                json::array({content::text(buildFirstLine(summary.str(), primaryError))});
            if (!limitToolResultDup) {
                contentItems.push_back(content::json(unwrapped, 2));
            }

            std::optional<json> structured;
            if (protocolVersion >= "2024-11-05") {
                structured = json{{"type", "tool_result"}, {"data", unwrapped}};
            }

            co_return wrapToolResultStructured(contentItems, structured, failed > 0);
        }

        json finalResult =
            finalResultBuilder(std::move(projectedSteps), steps.size(), succeeded, failed);

        std::ostringstream summary;
        summary << config_.summaryPrefix << ": " << succeeded << " succeeded, " << failed
                << " failed out of " << steps.size() << " operations";

        std::optional<std::string> primaryError;
        if (failed > 0 && finalResult.is_object()) {
            for (const auto& key : {"results", "steps"}) {
                if (!finalResult.contains(key) || !finalResult[key].is_array()) {
                    continue;
                }
                for (const auto& entry : finalResult[key]) {
                    if (!entry.is_object()) {
                        continue;
                    }
                    const bool entryIsError =
                        !entry.value("success", true) || entry.value("isError", false);
                    if (!entryIsError) {
                        continue;
                    }
                    if (entry.contains("error") && entry["error"].is_string()) {
                        primaryError = entry["error"].get<std::string>();
                        break;
                    }
                    if (entry.contains("result") && entry["result"].is_object() &&
                        entry["result"].contains("error") && entry["result"]["error"].is_string()) {
                        primaryError = entry["result"]["error"].get<std::string>();
                        break;
                    }
                }
                if (primaryError) {
                    break;
                }
            }
        }

        json contentItems =
            json::array({content::text(buildFirstLine(summary.str(), primaryError))});
        if (!limitToolResultDup) {
            contentItems.push_back(content::json(finalResult, 2));
        }

        std::optional<json> structured;
        if (protocolVersion >= "2024-11-05") {
            structured = json{{"type", "tool_result"}, {"data", finalResult}};
        }

        co_return wrapToolResultStructured(contentItems, structured, failed > 0);
    } catch (const json::exception& e) {
        co_return errorResultWrap(std::string("JSON error: ") + e.what());
    } catch (const std::exception& e) {
        co_return errorResultWrap(std::string("Error: ") + e.what());
    }
}

namespace projections {

json executeStepProjection(const ModeRouterStepResult& step) {
    json out = json{{"stepIndex", step.stepIndex}, {"op", step.op}, {"success", !step.isError}};
    out["data"] = step.data;
    if (step.isError && step.primaryError) {
        out["error"] = *step.primaryError;
    }
    return out;
}

json executeFinalResult(json projectedSteps, std::size_t totalOps, std::size_t succeeded,
                        std::size_t failed) {
    return json{{"results", std::move(projectedSteps)},
                {"totalOps", totalOps},
                {"succeeded", succeeded},
                {"failed", failed}};
}

json queryStepProjection(const ModeRouterStepResult& step) {
    return json{{"stepIndex", step.stepIndex},
                {"op", step.op},
                {"result", step.data},
                {"isError", step.isError}};
}

json queryFinalResult(json projectedSteps, std::size_t totalOps, std::size_t /*succeeded*/,
                      std::size_t /*failed*/) {
    const auto completed = projectedSteps.size();
    return json{{"steps", std::move(projectedSteps)},
                {"totalSteps", totalOps},
                {"completedSteps", completed}};
}

} // namespace projections

} // namespace yams::mcp
