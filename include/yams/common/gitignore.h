#pragma once

#include <filesystem>
#include <fstream>
#include <istream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <yams/common/pattern_utils.h>

namespace yams::common {

/// Parse the root .gitignore subset used by corpus ingestion.
///
/// Negated patterns are intentionally skipped because ingestion does not yet
/// implement ordered gitignore rule evaluation.
inline std::vector<std::string> parseGitignorePatterns(std::istream& input) {
    std::vector<std::string> patterns;
    std::string line;
    while (std::getline(input, line)) {
        while (!line.empty() &&
               (line.back() == ' ' || line.back() == '\t' || line.back() == '\r')) {
            line.pop_back();
        }
        if (line.empty() || line.front() == '#' || line.front() == '!') {
            continue;
        }

        const bool anchored = line.front() == '/';
        if (anchored) {
            line.erase(line.begin());
        }
        const bool directoryOnly = !line.empty() && line.back() == '/';
        if (directoryOnly) {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }

        const bool relativeToRoot = anchored || line.find_first_of("/\\") != std::string::npos;
        std::string pattern = relativeToRoot ? "^" + line : line;
        if (directoryOnly) {
            patterns.push_back(pattern);
            patterns.push_back(pattern + "/**");
        } else if (!relativeToRoot) {
            patterns.push_back("**/" + line);
        } else {
            patterns.push_back(std::move(pattern));
        }
    }
    return patterns;
}

inline std::vector<std::string> loadGitignorePatterns(const std::filesystem::path& root) {
    std::ifstream input(root / ".gitignore");
    if (!input) {
        return {};
    }
    return parseGitignorePatterns(input);
}

inline bool matchesGitignore(std::string_view relativePath, std::span<const std::string> patterns) {
    return matches_any_path(normalize_path(relativePath), patterns);
}

} // namespace yams::common
