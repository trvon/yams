#pragma once

#include <filesystem>
#include <functional>
#include <optional>

#include <yams/core/types.h>
// Ensure boost::asio symbols used by daemon_client.h are declared in TU including this header
#include <boost/asio/awaitable.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::app::services {

struct RetrievalOptions {
    // Optional explicit daemon socket path (overrides default resolver)
    std::optional<std::filesystem::path> socketPath;
    std::optional<std::filesystem::path> explicitDataDir; // only when user overrides
    bool enableStreaming{true};
    bool progressiveOutput{false};
    bool singleUseConnections{false};
    std::size_t maxChunkSize{512 * 1024};
    int headerTimeoutMs{30000};
    int bodyTimeoutMs{120000};
    int requestTimeoutMs{30000};
};

struct GetOptions {
    std::string hash;
    std::string name;
    bool byName = false;
    std::string fileType;
    std::string mimeType;
    std::string extension;
    bool binaryOnly = false;
    bool textOnly = false;
    std::string createdAfter;
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;
    bool latest = false;
    bool oldest = false;
    std::string outputPath;
    bool metadataOnly = false;
    uint64_t maxBytes = 0;
    uint32_t chunkSize = 524288;
    bool raw = false;
    bool extract = false;
    bool showGraph = false;
    int graphDepth = 1;
    bool verbose = false;
};

struct GrepOptions {
    std::string pattern;
    std::vector<std::string> paths;
    bool caseInsensitive = false;
    bool invertMatch = false;
    int contextLines = 0;
    size_t maxMatches = 0;
    std::vector<std::string> includePatterns;
    bool recursive = true;
    bool wholeWord = false;
    bool showLineNumbers = false;
    bool showFilename = false;
    bool noFilename = false;
    bool countOnly = false;
    bool filesOnly = false;
    bool filesWithoutMatch = false;
    bool pathsOnly = false;
    bool literalText = false;
    bool regexOnly = false;
    size_t semanticLimit = 10;
    std::vector<std::string> filterTags;
    bool matchAllTags = false;
    std::string colorMode = "auto";
    int beforeContext = 0;
    int afterContext = 0;
    bool showDiff = false;
};

struct ListOptions {
    size_t limit = 20;
    std::vector<std::string> tags;
    std::string format = "table";
    std::string sortBy = "date";
    std::string fileType;
    std::string mimeType;
    std::string extensions;
    std::string createdAfter;
    std::string createdBefore;
    std::string modifiedAfter;
    std::string modifiedBefore;
    std::string indexedAfter;
    std::string indexedBefore;
    std::string sinceTime;
    std::string changeWindow = "24h";
    std::string filterTags;
    std::string namePattern;
    int offset = 0;
    int recentCount = 0;
    int snippetLength = 50;
    bool recent = true;
    bool reverse = false;
    bool verbose = false;
    bool showSnippets = true;
    bool showMetadata = false;
    bool showTags = true;
    bool groupBySession = false;
    bool noSnippets = false;
    bool pathsOnly = false;
    bool binaryOnly = false;
    bool textOnly = false;
    bool showChanges = false;
    bool showDiffTags = false;
    bool showDeleted = false;
    bool matchAllTags = false;
};

struct GetInitOptions {
    std::string hash;
    std::string name;
    bool byName = false;
    bool metadataOnly = false;
    uint64_t maxBytes = 0;
    uint32_t chunkSize = 512 * 1024;
};

class RetrievalService {
public:
    RetrievalService() = default;

    Result<yams::daemon::GetResponse> get(const GetOptions& req,
                                          const RetrievalOptions& opts) const;

    Result<yams::daemon::GrepResponse> grep(const GrepOptions& req,
                                            const RetrievalOptions& opts) const;

    Result<yams::daemon::ListResponse> list(const ListOptions& req,
                                            const RetrievalOptions& opts) const;

    Result<void> getToStdout(const GetInitOptions& req, const RetrievalOptions& opts) const;

    Result<void> getToFile(const GetInitOptions& req, const std::filesystem::path& outputPath,
                           const RetrievalOptions& opts) const;

    struct ChunkedGetResult {
        yams::daemon::GetInitResponse init;
        std::string content; // potentially truncated to cap
    };

    Result<ChunkedGetResult> getChunkedBuffer(const GetInitOptions& req, std::size_t capBytes,
                                              const RetrievalOptions& opts) const;

    // Smart name resolution and retrieval: tries resolver->session list->hybrid search, then get
    Result<yams::daemon::GetResponse>
    getByNameSmart(const std::string& name, bool oldest, bool includeContent, bool useSession,
                   const std::string& sessionName, const RetrievalOptions& opts,
                   std::function<Result<std::string>(const std::string&)> resolver = {}) const;

    Result<std::string> resolveHashPrefix(const std::string& hashPrefix, bool preferOldest,
                                          bool preferLatest, const RetrievalOptions& opts,
                                          std::size_t limit = 32) const;

private:
    // Check if FTS5 index is ready for queries (PBI-040, task 040-1)
    bool isFTS5Ready(const RetrievalOptions& opts) const;
};

} // namespace yams::app::services
