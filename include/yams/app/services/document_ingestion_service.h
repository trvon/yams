#pragma once

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <yams/core/types.h>
// Ensure boost::asio types used by daemon_client.h are declared
#include <boost/asio/awaitable.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

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

    // Gitignore handling
    bool noGitignore{false}; // ignore .gitignore patterns when adding files

    // Daemon behavior
    std::optional<std::filesystem::path> explicitDataDir; // set only when caller overrides
    int timeoutMs{30000};
    int retries{3};
    int backoffMs{250};

    // Sync extraction wait options
    bool waitForProcessing{false}; // Wait for text extraction to complete before returning
    int waitTimeoutSeconds{30};    // Max seconds to wait for extraction (0 = no timeout)
};

struct DeleteOptions {
    std::optional<std::filesystem::path> socketPath;
    std::optional<std::filesystem::path> explicitDataDir;
    std::vector<std::string> hashes;
    std::vector<std::string> names;
    std::string sessionId;
    bool dryRun = false;
    int timeoutMs = 30000;
};

struct UpdateOptions {
    std::optional<std::filesystem::path> socketPath;
    std::optional<std::filesystem::path> explicitDataDir;
    std::string hash;
    std::string name;
    std::vector<std::string> addTags;
    std::vector<std::string> removeTags;
    std::map<std::string, std::string> setMetadata;
    std::vector<std::string> removeMetadata;
    std::string sessionId;
    int timeoutMs = 30000;
};

class DocumentIngestionService {
public:
    DocumentIngestionService() = default;
    explicit DocumentIngestionService(std::shared_ptr<yams::daemon::DaemonClient> client);

    // Perform daemon-first add. Builds AddDocumentRequest, configures DaemonClient,
    // and calls streamingAddDocument. Caller can implement fallback to local services on error.
    Result<yams::daemon::AddDocumentResponse> addViaDaemon(const AddOptions& opts) const;

    // New operations
    Result<yams::daemon::DeleteResponse> deleteDocument(const DeleteOptions& opts) const;
    Result<yams::daemon::UpdateDocumentResponse> updateDocument(const UpdateOptions& opts) const;

    // Utility: normalize a provided path to absolute/canonical when possible
    static std::string normalizePath(const std::string& inPath);

private:
    std::shared_ptr<yams::daemon::DaemonClient> client_;
    std::shared_ptr<yams::daemon::DaemonClient> getOrCreateClient(const AddOptions& opts) const;
    std::shared_ptr<yams::daemon::DaemonClient> getOrCreateClient(const DeleteOptions& opts) const;
    std::shared_ptr<yams::daemon::DaemonClient> getOrCreateClient(const UpdateOptions& opts) const;
};

} // namespace yams::app::services
