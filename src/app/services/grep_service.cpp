#include <yams/app/services/services.hpp>

#include <algorithm>
#include <cctype>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace yams::app::services {

namespace {

// Simple glob matcher supporting '*' and '?'
static bool wildcardMatch(const std::string& text, const std::string& pattern) {
    // DP approach
    const size_t n = text.size();
    const size_t m = pattern.size();
    std::vector<std::vector<bool>> dp(n + 1, std::vector<bool>(m + 1, false));
    dp[0][0] = true;
    for (size_t j = 1; j <= m; ++j) {
        if (pattern[j - 1] == '*')
            dp[0][j] = dp[0][j - 1];
    }
    for (size_t i = 1; i <= n; ++i) {
        for (size_t j = 1; j <= m; ++j) {
            if (pattern[j - 1] == '*') {
                dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
            } else if (pattern[j - 1] == '?' || pattern[j - 1] == text[i - 1]) {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }
    return dp[n][m];
}

static bool hasWildcard(const std::string& s) {
    return s.find('*') != std::string::npos || s.find('?') != std::string::npos;
}

static std::vector<std::string> splitToLines(const std::string& content) {
    std::vector<std::string> lines;
    std::istringstream iss(content);
    std::string line;
    while (std::getline(iss, line)) {
#ifdef _WIN32
        if (!line.empty() && (line.back() == '\r'))
            line.pop_back();
#else
        if (!line.empty() && (line.back() == '\r'))
            line.pop_back();
#endif
        lines.push_back(std::move(line));
    }
    // Handle trailing newline producing an extra empty line? std::getline handles ok.
    return lines;
}

static bool pathFilterMatch(const std::string& filePath, const std::vector<std::string>& filters) {
    if (filters.empty())
        return true;
    for (const auto& f : filters) {
        if (f.empty())
            continue;
        if (hasWildcard(f)) {
            if (wildcardMatch(filePath, f))
                return true;
        } else {
            // Treat filter as prefix (dir) or exact path
            if (filePath == f)
                return true;
#ifdef _WIN32
            // Normalize slash for crude prefix match
            std::string fp = filePath;
            std::replace(fp.begin(), fp.end(), '\\', '/');
            std::string ff = f;
            std::replace(ff.begin(), ff.end(), '\\', '/');
            if (fp.rfind(ff, 0) == 0)
                return true;
#else
            if (filePath.rfind(f, 0) == 0)
                return true;
#endif
        }
    }
    return false;
}

} // namespace

class GrepServiceImpl final : public IGrepService {
public:
    explicit GrepServiceImpl(const AppContext& ctx) : ctx_(ctx) {}

    Result<GrepResponse> grep(const GrepRequest& req) override {
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }
        if (!ctx_.store) {
            return Error{ErrorCode::NotInitialized, "Content store not available"};
        }
        if (req.pattern.empty()) {
            return Error{ErrorCode::InvalidArgument, "Pattern is required"};
        }

        // Build regex
        std::string pat = req.pattern;
        if (req.word) {
            // Wrap with word boundaries; use a non-capturing group to avoid precedence issues
            pat = "\\b(?:" + pat + ")\\b";
        }

        std::regex_constants::syntax_option_type flags = std::regex::ECMAScript;
        if (req.ignoreCase)
            flags |= std::regex::icase;

        std::regex re;
        try {
            re = std::regex(pat, flags);
        } catch (const std::regex_error& e) {
            return Error{ErrorCode::InvalidArgument, std::string("Invalid regex: ") + e.what()};
        }

        // Resolve context windows
        int beforeContext = req.context > 0 ? req.context : req.beforeContext;
        int afterContext = req.context > 0 ? req.context : req.afterContext;
        if (beforeContext < 0)
            beforeContext = 0;
        if (afterContext < 0)
            afterContext = 0;

        auto docsRes = ctx_.metadataRepo->findDocumentsByPath("%");
        if (!docsRes) {
            return Error{ErrorCode::InternalError,
                         "Failed to enumerate documents: " + docsRes.error().message};
        }

        GrepResponse response;
        response.totalMatches = 0;

        const auto& docs = docsRes.value();
        for (const auto& doc : docs) {
            // Apply path filters (if provided)
            if (!pathFilterMatch(doc.filePath, req.paths)) {
                continue;
            }

            // Retrieve content
            std::ostringstream oss;
            auto rs = ctx_.store->retrieveStream(doc.sha256Hash, oss, nullptr);
            if (!rs) {
                // Skip unreadable file
                continue;
            }
            const std::string content = oss.str();
            auto lines = splitToLines(content);

            GrepFileResult fileResult;
            fileResult.file = doc.filePath;
            fileResult.matchCount = 0;

            // Scan lines
            for (size_t i = 0; i < lines.size(); ++i) {
                const std::string& line = lines[i];
                bool matched = std::regex_search(line, re);
                if (req.invert)
                    matched = !matched;

                if (!matched)
                    continue;

                fileResult.matchCount++;
                response.totalMatches++;

                if (!req.count) {
                    GrepMatch gm;
                    if (req.lineNumbers)
                        gm.lineNumber = i + 1;
                    gm.line = line;
                    // Best-effort column positions for first match (only if not inverted)
                    if (!req.invert) {
                        std::smatch sm;
                        if (std::regex_search(line, sm, re)) {
                            gm.columnStart = static_cast<size_t>(sm.position()) + 1; // 1-based
                            gm.columnEnd = gm.columnStart + static_cast<size_t>(sm.length());
                        }
                    }

                    // Context: before
                    if (beforeContext > 0) {
                        size_t start =
                            (i > static_cast<size_t>(beforeContext)) ? (i - beforeContext) : 0;
                        for (size_t j = start; j < i; ++j) {
                            gm.before.push_back(lines[j]);
                        }
                    }
                    // Context: after
                    if (afterContext > 0) {
                        size_t end =
                            std::min(lines.size(), i + 1 + static_cast<size_t>(afterContext));
                        for (size_t j = i + 1; j < end; ++j) {
                            gm.after.push_back(lines[j]);
                        }
                    }

                    fileResult.matches.push_back(std::move(gm));
                }

                if (req.maxCount > 0 && static_cast<int>(fileResult.matchCount) >= req.maxCount) {
                    break;
                }
            }

            if (fileResult.matchCount > 0) {
                response.results.push_back(std::move(fileResult));
                response.filesWith.push_back(doc.filePath);
            } else {
                response.filesWithout.push_back(doc.filePath);
            }
        }

        // If filesWithMatches mode is intended for presentation, the caller can read filesWith.
        // If filesWithoutMatch intended, caller can read filesWithout.
        // If count mode intended, caller can sum fileResult.matchCount or use totalMatches.

        return Result<GrepResponse>(std::move(response));
    }

private:
    AppContext ctx_;
};

// Optional separate factory for direct construction (callers may wire this in a factory unit).
std::shared_ptr<IGrepService> makeGrepService(const AppContext& ctx) {
    return std::make_shared<GrepServiceImpl>(ctx);
}

} // namespace yams::app::services