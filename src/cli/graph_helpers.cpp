#include <yams/cli/graph_helpers.h>

#include <filesystem>
#include <unordered_set>

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
        std::filesystem::path input(name);
        if (!input.empty()) {
            auto absolute = std::filesystem::absolute(input).lexically_normal();
            push_unique(absolute.string());
        }
    } catch (...) {
        // Best-effort normalization only.
    }

    return candidates;
}

} // namespace yams::cli
