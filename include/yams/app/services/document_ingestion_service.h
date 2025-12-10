#pragma once

#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <yams/core/types.h>
// Ensure boost::asio types used by daemon_client.h are declared
#include <boost/asio/awaitable.hpp>
#include <yams/daemon/client/daemon_client.h>

namespace yams::app::services {

struct AddOptions {
    // Optional explicit daemon socket path (overrides default resolver)
    std::optional<std::filesystem::path> socketPath;
    std::string path;     // file/dir path; empty for content-only
    std::string content;  // inline content when provided
    std::string name;     // optional display name
    std::string mimeType; // optional MIME type
    bool disableAutoMime{false};
    bool noEmbeddings{false};
    std::string collection;    // optional collection name
    std::string snapshotId;    // optional snapshot id
    std::string snapshotLabel; // optional snapshot label
    std::string sessionId;     // session-isolated memory (PBI-082)
    bool recursive{false};     // directory recursion
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;

    // Post-add verification
    bool verify{false};        // verify hash/size for single-file adds
    bool verifyIndexes{false}; // optional: verify index presence (future)

    // Daemon behavior
    std::optional<std::filesystem::path> explicitDataDir; // set only when caller overrides
    int timeoutMs{30000};
    int retries{3};
    int backoffMs{250};
};

class DocumentIngestionService {
public:
    DocumentIngestionService() = default;

    // Perform daemon-first add. Builds AddDocumentRequest, configures DaemonClient,
    // and calls streamingAddDocument. Caller can implement fallback to local services on error.
    Result<yams::daemon::AddDocumentResponse> addViaDaemon(const AddOptions& opts) const;

    // Utility: normalize a provided path to absolute/canonical when possible
    static std::string normalizePath(const std::string& inPath);
};

} // namespace yams::app::services
