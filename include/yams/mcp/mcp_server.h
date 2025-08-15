#pragma once

#include <nlohmann/json.hpp>
#include <atomic>
#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <thread>
#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_executor.h>
#include <yams/version.hpp>

namespace yams::mcp {

using json = nlohmann::json;

/**
 * Transport interface for MCP communication
 */
class ITransport {
public:
    virtual ~ITransport() = default;
    virtual void send(const json& message) = 0;
    virtual json receive() = 0;
    virtual bool isConnected() const = 0;
    virtual void close() = 0;
};

/**
 * Standard I/O transport (default for MCP)
 */
class StdioTransport : public ITransport {
public:
    StdioTransport();
    void send(const json& message) override;
    json receive() override;
    bool isConnected() const override { return !closed_; }
    void close() override { closed_ = true; }

    // Set external shutdown flag for non-blocking checks
    void setShutdownFlag(std::atomic<bool>* shutdown) { externalShutdown_ = shutdown; }

private:
    std::atomic<bool> closed_{false};
    std::atomic<bool>* externalShutdown_{nullptr};

    // Helper for non-blocking stdin check
    bool isInputAvailable(int timeoutMs = 100) const;
};

/**
 * WebSocket transport for MCP over WebSocket
 */
class WebSocketTransport : public ITransport {
public:
    struct Config {
        std::string host = "localhost";
        uint16_t port = 8080;
        std::string path = "/mcp";
        bool useSSL = false;
        std::chrono::milliseconds connectTimeout{5000};
        std::chrono::milliseconds receiveTimeout{30000};
        size_t maxMessageSize = 1024 * 1024; // 1MB
        bool enablePingPong = true;
        std::chrono::seconds pingInterval{30};
    };

    explicit WebSocketTransport(const Config& config);
    ~WebSocketTransport();

    void send(const json& message) override;
    json receive() override;
    bool isConnected() const override;
    void close() override;

    // WebSocket-specific methods
    bool connect();
    void reconnect();
    bool waitForConnection(std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * MCP Server implementation
 */
class MCPServer {
public:
    MCPServer(std::shared_ptr<api::IContentStore> store,
              std::shared_ptr<search::SearchExecutor> searchExecutor,
              std::shared_ptr<metadata::MetadataRepository> metadataRepo,
              std::shared_ptr<search::HybridSearchEngine> hybridEngine = nullptr,
              std::unique_ptr<ITransport> transport = std::make_unique<StdioTransport>(),
              std::atomic<bool>* externalShutdown = nullptr);
    ~MCPServer();

    /**
     * Start the server and begin processing requests
     */
    void start();

    /**
     * Stop the server
     */
    void stop();

    /**
     * Check if server is running
     */
    bool isRunning() const { return running_; }

#ifdef YAMS_TESTING
    // Public testing interface - only available when building tests
    json testListTools() { return listTools(); }
    json testSearchDocuments(const json& args) { return searchDocuments(args); }
    json testGrepDocuments(const json& args) { return grepDocuments(args); }
    json testRetrieveDocument(const json& args) { return retrieveDocument(args); }
    json testUpdateDocumentMetadata(const json& args) { return updateDocumentMetadata(args); }
    json testListDocuments(const json& args) { return listDocuments(args); }
    json testGetStats(const json& args) { return getStats(args); }
#endif

private:
    // MCP protocol methods
    json handleRequest(const json& request);
    json initialize(const json& params);
    json listResources();
    json listTools();
    json listPrompts();
    json callTool(const std::string& name, const json& arguments);
    json readResource(const std::string& uri);

    // Tool implementations
    json searchDocuments(const json& args);
    json grepDocuments(const json& args);
    json storeDocument(const json& args);
    json retrieveDocument(const json& args);
    json deleteDocument(const json& args);
    json updateMetadata(const json& args);
    json updateDocumentMetadata(const json& args);
    json getStats(const json& args);

    // New v0.0.2 CLI integration tools
    json deleteByName(const json& args);
    json getByName(const json& args);
    json catDocument(const json& args);
    json listDocuments(const json& args);

    // New v0.0.4 directory and collection tools
    json addDirectory(const json& args);
    json restoreCollection(const json& args);
    json restoreSnapshot(const json& args);
    json listCollections(const json& args);
    json listSnapshots(const json& args);

    // Helper methods
    json createResponse(const json& id, const json& result);
    json createError(const json& id, int code, const std::string& message);

    // Name resolution helpers (similar to CLI commands)
    Result<std::string> resolveNameToHash(const std::string& name);
    Result<std::vector<std::pair<std::string, std::string>>>
    resolveNameToHashes(const std::string& name);
    Result<std::vector<std::pair<std::string, std::string>>>
    resolveNamesToHashes(const std::vector<std::string>& names);
    Result<std::vector<std::pair<std::string, std::string>>>
    resolvePatternToHashes(const std::string& pattern);

    // Collection and snapshot helpers
    json performRestore(const std::vector<metadata::DocumentInfo>& documents,
                        const std::string& outputDir, const std::string& layoutTemplate,
                        bool overwrite, bool createDirs, bool dryRun, const std::string& scope);
    std::string
    expandLayoutTemplate(const std::string& layoutTemplate, const metadata::DocumentInfo& doc,
                         const std::unordered_map<std::string, metadata::MetadataValue>& metadata);

    // Helper methods for hash search
    bool isValidHash(const std::string& str);
    json searchByHash(const std::string& hash, size_t limit);

    // Helper method for formatting search results with context
    std::string formatSnippetWithContext(const std::string& content, const std::string& query,
                                         int beforeContext, int afterContext, bool showLineNumbers,
                                         const std::string& colorMode);

    // Helper structures and methods for grep functionality
    struct GrepMatch {
        size_t lineNumber;
        size_t columnStart;
        size_t columnEnd;
        std::string line;
    };

    std::vector<GrepMatch> processGrepFile(const std::string& filename, const std::string& content,
                                           const std::regex& pattern, bool invertMatch,
                                           int maxCount);
    std::string formatGrepContext(const std::string& content, size_t lineNumber, int beforeContext,
                                  int afterContext);

    // Helper structures and methods for knowledge graph functionality
    struct RelatedDocument {
        std::string hash;
        std::string path;
        std::string relationship;
        int distance;
        json metadata;
    };

    std::vector<RelatedDocument> findRelatedDocuments(const metadata::DocumentInfo& baseDoc,
                                                      int depth, int maxResults = 20);
    json buildKnowledgeGraphResponse(const metadata::DocumentInfo& baseDoc,
                                     const std::vector<RelatedDocument>& related,
                                     bool includeContent, const std::string& outputPath = "");

    // Helper methods for file type classification
    std::string getFileTypeFromMime(const std::string& mimeType);
    bool isBinaryMimeType(const std::string& mimeType);

private:
    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<search::HybridSearchEngine> hybridEngine_;
    std::unique_ptr<ITransport> transport_;

    std::atomic<bool> running_{false};
    std::atomic<bool> initialized_{false};
    std::atomic<bool> readyPending_{false};
    std::atomic<bool>* externalShutdown_{nullptr};

    // Server info
    struct {
        std::string name = "kronos-mcp";
        std::string version = YAMS_VERSION_STRING;
    } serverInfo_;

    // Client info (set during initialize)
    struct {
        std::string name;
        std::string version;
    } clientInfo_;

    // Negotiated protocol version (set during initialize)
    std::string negotiatedProtocolVersion_{"2024-11-05"};

public:
    // Process a single JSON-RPC message. For requests (with id), returns a JSON-RPC response.
    // For notifications (no id), returns a null json.
    json processMessage(const json& message) { return handleRequest(message); }
};

} // namespace yams::mcp