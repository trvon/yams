#include <yams/daemon/components/grep_result_window.h>

#include <algorithm>
#include <cctype>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace yams::daemon::grep_result_window {
namespace {

std::string normalizeLine(std::string_view line) {
    std::string normalized;
    normalized.reserve(line.size());
    bool pendingSpace = false;
    for (const unsigned char character : line) {
        if (std::isspace(character) != 0) {
            pendingSpace = !normalized.empty();
            continue;
        }
        if (pendingSpace) {
            normalized.push_back(' ');
            pendingSpace = false;
        }
        normalized.push_back(static_cast<char>(character));
    }
    return normalized;
}

bool isBinaryLike(std::string_view text) {
    return text.find('\0') != std::string_view::npos;
}

bool isBinaryLike(const app::services::GrepMatch& match) {
    if (isBinaryLike(match.line)) {
        return true;
    }
    const auto containsBinaryText = [](const auto& lines) {
        return std::ranges::any_of(lines, [](const auto& line) { return isBinaryLike(line); });
    };
    return containsBinaryText(match.before) || containsBinaryText(match.after);
}

struct MatchIdentity {
    std::string file;
    std::size_t lineNumber{0};
    std::string line;

    bool operator==(const MatchIdentity&) const = default;
};

struct MatchIdentityHash {
    std::size_t operator()(const MatchIdentity& identity) const noexcept {
        const auto combine = [](std::size_t seed, std::size_t value) {
            return seed ^ (value + 0x9e3779b9U + (seed << 6U) + (seed >> 2U));
        };
        auto hash = std::hash<std::string>{}(identity.file);
        hash = combine(hash, std::hash<std::size_t>{}(identity.lineNumber));
        return combine(hash, std::hash<std::string>{}(identity.line));
    }
};

GrepMatch toDaemonMatch(const app::services::GrepFileResult& fileResult,
                        const app::services::GrepMatch& match) {
    GrepMatch output;
    output.file = fileResult.file;
    output.lineNumber = match.lineNumber;
    output.line = match.line;
    output.contextBefore = match.before;
    output.contextAfter = match.after;
    output.matchType = match.matchType.empty() ? "regex" : match.matchType;
    output.confidence = match.confidence;
    return output;
}

} // namespace

Selection select(const std::vector<app::services::GrepFileResult>& fileResults,
                 std::size_t maxTotalMatches) {
    Selection selection;
    std::unordered_set<MatchIdentity, MatchIdentityHash> seen;

    for (const auto& fileResult : fileResults) {
        for (const auto& match : fileResult.matches) {
            ++selection.stats.inputMatches;
            if (isBinaryLike(match)) {
                continue;
            }
            MatchIdentity identity{
                .file = fileResult.file,
                .lineNumber = match.lineNumber,
                .line = normalizeLine(match.line),
            };
            if (!seen.insert(std::move(identity)).second) {
                continue;
            }
            ++selection.stats.uniqueMatches;
            if (maxTotalMatches == 0 || selection.matches.size() < maxTotalMatches) {
                selection.matches.push_back(toDaemonMatch(fileResult, match));
            }
        }
    }

    selection.stats.emittedMatches = selection.matches.size();
    return selection;
}

} // namespace yams::daemon::grep_result_window
