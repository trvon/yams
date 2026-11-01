#pragma once

#include <cstddef>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace yams::cli {
struct LiveGrepOptions {
    std::string pattern;
    std::filesystem::path root;
    std::vector<std::string> globs;
    std::function<bool(std::string_view)> acceptPath;
    bool literal{false};
    bool ignoreCase{false};
    bool wholeWord{false};
    bool invert{false};
    bool pathsOnly{false};
    bool filesWithoutMatch{false};
    std::size_t beforeContext{0};
    std::size_t afterContext{0};
    std::size_t perFileLimit{20};
    std::size_t resultLimit{0};
};
struct LiveGrepHit {
    std::string path;
    std::size_t lineNumber{0};
    std::string line;
    std::vector<std::string> contextBefore;
    std::vector<std::string> contextAfter;
};
struct LiveGrepResult {
    bool available{false};
    std::vector<LiveGrepHit> hits;
};
LiveGrepResult liveGrep(const LiveGrepOptions& options);
} // namespace yams::cli
