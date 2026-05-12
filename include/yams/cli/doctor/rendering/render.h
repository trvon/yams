#pragma once

#include <nlohmann/json_fwd.hpp>

#include <iostream>
#include <string>
#include <vector>

namespace yams::cli {
class YamsCLI;
namespace ui {
struct RecommendationBuilder;
} // namespace ui
} // namespace yams::cli

namespace yams::cli::doctor {

struct CachedDaemonState;

/// Pure rendering helpers — no mutations, no daemon interaction.
struct DoctorRender {
    struct StepResult {
        std::string name;
        bool ok{false};
        std::string message;
    };

    static void printHeader(std::ostream& os, const std::string& title);
    static void printStatusLine(std::ostream& os, const std::string& label,
                                const std::string& value);
    static void printSummary(std::ostream& os, const std::string& title,
                             const std::vector<StepResult>& steps);

    /// Build the JSON output for the full doctor report.
    static nlohmann::json buildDoctorJson(YamsCLI* cli, const CachedDaemonState& cachedState,
                                          const ui::RecommendationBuilder* recs = nullptr);
};

} // namespace yams::cli::doctor
