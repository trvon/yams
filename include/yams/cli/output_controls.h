#pragma once

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace yams::cli {

struct RetrievalOutputConfig {
    std::size_t budgetChars{0}; // 0 preserves unlimited, binary-safe output.
    std::vector<std::string> excludePaths{"/tmp/", "~/.pi/", "**/scratchpad/**"};
};

// A streaming, bounded-memory sink. Once a line does not fit, discard that line and
// everything after it, but keep counting for the final explicit truncation marker.
class OutputBudget : public std::streambuf {
public:
    OutputBudget(std::ostream& stream, std::size_t limit)
        : stream_(stream), original_(stream.rdbuf()), limit_(limit) {
        if (limit_ != 0)
            stream_.rdbuf(this);
    }
    ~OutputBudget() override {
        if (limit_ == 0)
            return;
        finishLine(false);
        stream_.rdbuf(original_);
        if (omittedChars_ != 0) {
            if (emitted_ != 0 && !endedWithNewline_)
                stream_ << '\n';
            stream_ << "[truncated: " << omittedChars_ << " more chars, " << omittedResults_
                    << " more results; narrow with --path/--limit/--lines]\n";
        }
    }
    OutputBudget(const OutputBudget&) = delete;
    OutputBudget& operator=(const OutputBudget&) = delete;

    void result() {
        if (omittedChars_ != 0 || droppingLine_)
            ++omittedResults_;
    }

private:
    int_type overflow(int_type value) override {
        if (traits_type::eq_int_type(value, traits_type::eof()))
            return traits_type::not_eof(value);
        const char c = traits_type::to_char_type(value);
        if (truncated_) {
            ++omittedChars_;
        } else if (droppingLine_) {
            ++omittedChars_;
            if (c == '\n') {
                droppingLine_ = false;
                truncated_ = true;
            }
        } else if (emitted_ + pending_.size() == limit_) {
            omittedChars_ += pending_.size() + 1;
            pending_.clear();
            droppingLine_ = c != '\n';
            truncated_ = c == '\n';
        } else {
            pending_ += c;
            if (c == '\n')
                finishLine(true);
        }
        return value;
    }
    std::streamsize xsputn(const char* data, std::streamsize count) override {
        for (std::streamsize i = 0; i < count; ++i)
            overflow(traits_type::to_int_type(data[i]));
        return count;
    }
    int sync() override { return original_->pubsync(); }
    void finishLine(bool newline) {
        if (pending_.empty())
            return;
        original_->sputn(pending_.data(), static_cast<std::streamsize>(pending_.size()));
        emitted_ += pending_.size();
        pending_.clear();
        endedWithNewline_ = newline;
    }

    std::ostream& stream_;
    std::streambuf* original_;
    std::size_t limit_;
    std::string pending_;
    std::size_t emitted_{0};
    std::size_t omittedChars_{0};
    std::size_t omittedResults_{0};
    bool droppingLine_{false};
    bool truncated_{false};
    bool endedWithNewline_{true};

    friend void noteOutputResult(std::ostream&);
};

inline void noteOutputResult(std::ostream& stream) {
    if (auto* budget = dynamic_cast<OutputBudget*>(stream.rdbuf()))
        budget->result();
}

// A path-only result is one unique file, not one match in that file.
template <typename Match>
void capGrepResults(std::vector<Match>& matches, std::size_t limit, bool pathsOnly) {
    std::unordered_set<std::string> seen;
    std::size_t kept = 0;
    for (std::size_t i = 0; i < matches.size(); ++i) {
        if (pathsOnly && !seen.insert(matches[i].file).second)
            continue;
        if (limit != 0 && kept >= limit)
            break;
        if (kept != i)
            matches[kept] = std::move(matches[i]);
        ++kept;
    }
    matches.resize(kept);
}

struct ContentSliceOptions {
    std::string lines;
    std::string around;
    std::size_t context{2};
    bool active() const { return !lines.empty() || !around.empty(); }
};

inline std::size_t parsePositiveLine(std::string_view value) {
    std::size_t result = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), result);
    if (error != std::errc{} || end != value.data() + value.size() || result == 0)
        throw std::invalid_argument("--lines requires positive inclusive line numbers A:B");
    return result;
}

inline std::string sliceContent(std::string_view content, const ContentSliceOptions& opts,
                                std::uint64_t maxBytes = 0) {
    if (!opts.lines.empty() && !opts.around.empty())
        throw std::invalid_argument("--lines and --around are mutually exclusive");
    if (!opts.active()) {
        return std::string(content.substr(
            0, maxBytes == 0 ? content.size() : std::min<std::uint64_t>(maxBytes, content.size())));
    }
    std::size_t first = 1;
    std::size_t last = 0;
    if (!opts.lines.empty()) {
        const auto colon = opts.lines.find(':');
        if (colon == std::string::npos)
            throw std::invalid_argument("--lines requires A:B");
        first = parsePositiveLine(std::string_view(opts.lines).substr(0, colon));
        last = parsePositiveLine(std::string_view(opts.lines).substr(colon + 1));
        if (last < first)
            throw std::invalid_argument("--lines end must not precede start");
    } else {
        const auto match = content.find(opts.around);
        if (match == std::string_view::npos)
            return "(no results)\n";
        const auto line = 1 + static_cast<std::size_t>(
                                  std::count(content.begin(), content.begin() + match, '\n'));
        first = line > opts.context ? line - opts.context : 1;
        last = opts.context > SIZE_MAX - line ? SIZE_MAX : line + opts.context;
    }
    std::string result;
    std::size_t number = 1;
    for (std::size_t start = 0; start < content.size() && number <= last; ++number) {
        auto end = content.find('\n', start);
        if (end == std::string_view::npos)
            end = content.size();
        if (number >= first) {
            result += std::to_string(number) + ": ";
            result.append(content.substr(start, end - start));
            result += '\n';
            if (maxBytes != 0 && result.size() >= maxBytes) {
                result.resize(static_cast<std::size_t>(maxBytes));
                break;
            }
        }
        start = end == content.size() ? end : end + 1;
    }
    return result;
}

inline std::string compactSnippet(std::string_view text, std::size_t limit = 120) {
    std::string result;
    for (unsigned char c : text) {
        if (result.size() >= limit)
            break;
        if (std::isspace(c) || c < 0x20 || c == 0x7f) {
            if (!result.empty() && result.back() != ' ')
                result += ' ';
        } else {
            result += static_cast<char>(c);
        }
    }
    // Never emit a partial UTF-8 codepoint at the byte boundary.
    if (result.size() == limit && limit < text.size()) {
        auto start = result.size();
        while (start > 0 && (static_cast<unsigned char>(result[start - 1]) & 0xc0) == 0x80)
            --start;
        if (start > 0 && static_cast<unsigned char>(result[start - 1]) >= 0xc0)
            result.resize(start - 1);
    }
    while (!result.empty() && result.back() == ' ')
        result.pop_back();
    return result;
}

// At most 179 bytes per hit: five ordinary compact hits always fit below 900 bytes.
// Long paths use the basename; --paths-only retains the full path for disambiguation.
inline std::string compactHit(std::string_view path, std::size_t line, std::string_view snippet,
                              std::optional<double> score = std::nullopt) {
    std::string name(path);
    if (name.size() > 72)
        name = std::filesystem::path(name).filename().string();
    const auto prefix = compactSnippet(name, 72) + ":" + std::to_string(line) + ": ";
    std::string suffix;
    if (score) {
        std::ostringstream value;
        value << " [score=" << std::setprecision(3) << *score << ']';
        suffix = compactSnippet(value.str(), 32);
    }
    const auto available =
        prefix.size() + suffix.size() < 178 ? 178 - prefix.size() - suffix.size() : std::size_t{0};
    return prefix + compactSnippet(snippet, std::min<std::size_t>(120, available)) + suffix + '\n';
}

inline bool retrievalGlobMatches(std::string_view path, std::string_view pattern) {
    std::size_t p = 0, i = 0, star = std::string_view::npos, retry = 0;
    while (i < path.size()) {
        if (p < pattern.size() && (pattern[p] == '?' || pattern[p] == path[i])) {
            ++p;
            ++i;
        } else if (p < pattern.size() && pattern[p] == '*') {
            star = p++;
            retry = i;
        } else if (star != std::string_view::npos) {
            p = star + 1;
            i = ++retry;
        } else {
            return false;
        }
    }
    while (p < pattern.size() && pattern[p] == '*')
        ++p;
    return p == pattern.size();
}

inline bool excludeRetrievalPath(std::string_view path, const RetrievalOutputConfig& config,
                                 std::string_view query = {}, bool explicitNoteSelector = false) {
    // Explicitly named/tagged notes remain discoverable even when stored under scratch roots.
    if (explicitNoteSelector)
        return false;
    (void)query;
    const auto normalized =
        std::filesystem::absolute(std::filesystem::path(path)).lexically_normal().generic_string();
    for (const auto& pattern : config.excludePaths) {
        if (pattern.empty() || pattern.starts_with("~/")) // expanded by the config loader
            continue;
        if (pattern.find_first_of("*?") != std::string::npos) {
            if (retrievalGlobMatches(normalized, pattern) ||
                retrievalGlobMatches("/" + normalized, pattern))
                return true;
        } else {
            auto root = std::filesystem::path(pattern).lexically_normal().generic_string();
            while (root.size() > 1 && root.back() == '/')
                root.pop_back();
            if (normalized == root || normalized.starts_with(root + "/"))
                return true;
        }
    }
    return false;
}

} // namespace yams::cli
