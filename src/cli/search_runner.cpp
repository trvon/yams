#include <yams/cli/search_runner.h>

#include <spdlog/spdlog.h>

#include <algorithm>

namespace yams::cli::search_runner {

static bool is_parse_like_error(const yams::Error& err) {
    if (err.code == ErrorCode::InvalidArgument)
        return true;
    auto contains = [&](const char* s) { return err.message.find(s) != std::string::npos; };
    return contains("syntax") || contains("FTS5") || contains("unbalanced") || contains("near") ||
           contains("tokenize");
}

boost::asio::awaitable<Result<DaemonSearchResult>>
daemon_search(yams::daemon::DaemonClient& client, DaemonSearchOptions opts, bool enableStreaming) {
    yams::daemon::SearchRequest req;
    req.query = opts.query;
    req.limit = opts.limit;
    req.fuzzy = opts.fuzzy;
    req.literalText = opts.literalText;
    req.similarity = opts.similarity;
    req.pathsOnly = false;
    req.searchType = opts.searchType;
    req.showLineNumbers = opts.showLineNumbers;
    req.symbolRank = opts.symbolRank;
    req.timeout = opts.timeout;

    req.pathPatterns = opts.pathPatterns;
    req.tags = opts.tags;
    req.matchAllTags = opts.matchAllTags;
    req.extension = opts.extension;
    req.mimeType = opts.mimeType;
    req.fileType = opts.fileType;
    req.textOnly = opts.textOnly;
    req.binaryOnly = opts.binaryOnly;

    auto callOnce = [&](const yams::daemon::SearchRequest& r)
        -> boost::asio::awaitable<Result<yams::daemon::SearchResponse>> {
        if (enableStreaming)
            co_return co_await client.streamingSearch(r);
        co_return co_await client.call(r);
    };

    DaemonSearchResult out;
    out.usedStreaming = enableStreaming;

    out.attempts++;
    auto r = co_await callOnce(req);
    if (r) {
        out.response = std::move(r.value());
        // Fuzzy retry when 0 results and fuzzy not requested
        bool noResults = out.response.results.empty() || out.response.totalCount == 0;
        if (noResults && !opts.fuzzy) {
            auto retry = req;
            retry.fuzzy = true;
            out.attempts++;
            auto fr = co_await callOnce(retry);
            if (fr) {
                out.usedFuzzyRetry = true;
                out.response = std::move(fr.value());
            }
        }
        co_return out;
    }

    // Literal-text retry for parse-like failures
    const auto& err = r.error();
    if (!opts.literalText && is_parse_like_error(err)) {
        auto retry = req;
        retry.literalText = true;
        out.attempts++;
        auto rr = co_await callOnce(retry);
        if (rr) {
            out.usedLiteralTextRetry = true;
            out.response = std::move(rr.value());
            co_return out;
        }
    }

    co_return err;
}

} // namespace yams::cli::search_runner
