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

/// Prepared form of loadGitignorePatterns() for callers that test many paths, such as a
/// directory walk. Gives the same answers as matchesGitignore() on the pattern list.
inline PathPatternSet loadGitignoreMatcher(const std::filesystem::path& root) {
    return PathPatternSet(loadGitignorePatterns(root));
}

inline bool matchesGitignore(std::string_view relativePath, const PathPatternSet& patterns) {
    return patterns.matches(relativePath);
}

/// One .gitignore, with the rules git applies: patterns are relative to the directory that
/// holds the file, later rules override earlier ones, and `!pattern` re-includes.
struct GitignoreRule {
    PathPatternSet pattern;
    bool negated{false};
    bool directoryOnly{false};
};

inline std::vector<GitignoreRule> parseGitignoreRules(std::istream& input) {
    std::vector<GitignoreRule> rules;
    std::string line;
    while (std::getline(input, line)) {
        while (!line.empty() &&
               (line.back() == ' ' || line.back() == '\t' || line.back() == '\r')) {
            line.pop_back();
        }
        if (line.empty() || line.front() == '#') {
            continue;
        }
        GitignoreRule rule;
        if (line.front() == '!') {
            rule.negated = true;
            line.erase(line.begin());
        } else if (line.starts_with("\\!") || line.starts_with("\\#")) {
            line.erase(line.begin()); // escaped literal '!' or '#'
        }
        const bool anchored = !line.empty() && line.front() == '/';
        if (anchored) {
            line.erase(line.begin());
        }
        if (!line.empty() && line.back() == '/') {
            rule.directoryOnly = true;
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        // A pattern with a slash is relative to the .gitignore's directory; one without
        // matches a name at any depth below it.
        const bool relativeToBase = anchored || line.find('/') != std::string::npos;
        const std::string pattern = relativeToBase ? "^" + line : "**/" + line;
        rule.pattern = PathPatternSet(std::span<const std::string>(&pattern, 1));
        rules.push_back(std::move(rule));
    }
    return rules;
}

/// The .gitignore files in effect at one point of a tree walk, outermost first.
///
/// Enter a directory with push() (its rules apply to paths below it) and leave with pop().
/// ignored() takes the path relative to the walk root, already split on '/'. As in git, a
/// path under an ignored directory cannot be re-included, which a walk gets for free by
/// not descending into ignored directories.
class GitignoreStack {
public:
    void push(std::size_t baseDepth, const std::vector<GitignoreRule>* rules) {
        levels_.push_back({baseDepth, rules});
    }
    void pop() { levels_.pop_back(); }

    [[nodiscard]] bool ignored(std::span<const std::string_view> segments, bool isDirectory) const {
        // The last matching rule decides, so search from the innermost file's last rule.
        for (auto level = levels_.rbegin(); level != levels_.rend(); ++level) {
            if (segments.size() <= level->baseDepth) {
                continue;
            }
            const auto relative = segments.subspan(level->baseDepth);
            for (auto rule = level->rules->rbegin(); rule != level->rules->rend(); ++rule) {
                if (rule->directoryOnly && !isDirectory) {
                    continue;
                }
                if (rule->pattern.matchesSegments(relative)) {
                    return !rule->negated;
                }
            }
        }
        return false;
    }

private:
    struct Level {
        std::size_t baseDepth; // segments in the .gitignore's directory, relative to the root
        const std::vector<GitignoreRule>* rules;
    };
    std::vector<Level> levels_;
};

} // namespace yams::common
