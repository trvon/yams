#include <yams/cli/graph_helpers.h>

#include <spdlog/spdlog.h>
#include <filesystem>
#include <unordered_set>

#include <yams/metadata/path_utils.h>

namespace yams::cli {

std::vector<std::string> build_graph_file_node_candidates(const std::string& name) {
    std::vector<std::string> candidates;
    std::unordered_set<std::string> seen;

    auto push_unique = [&](const std::string& value) {
        if (!value.empty() && seen.insert(value).second) {
            candidates.push_back(value);
        }
    };

    push_unique(name);

    try {
        auto derived = yams::metadata::computePathDerivedValues(name);
        if (!derived.normalizedPath.empty()) {
            push_unique(derived.normalizedPath);
        }
    } catch (const std::exception& e) {
        spdlog::trace("Path derivation failed for '{}': {}", name, e.what());
    }

    try {
        std::filesystem::path input(name);
        if (!input.empty()) {
            auto absolute = std::filesystem::absolute(input).lexically_normal();
            push_unique(absolute.string());
        }
    } catch (const std::exception& e) {
        // Best-effort normalization only - path operations can fail on invalid inputs
        spdlog::trace("Path normalization failed for '{}': {}", name, e.what());
    }

    return candidates;
}

} // namespace yams::cli
