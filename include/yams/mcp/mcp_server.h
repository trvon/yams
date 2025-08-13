#pragma once

#include <yams/core/types.h>
#include <yams/api/content_store.h>
#include <yams/search/search_executor.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/version.hpp>
#include <nlohmann/json.hpp>
#include <memory>
#include <string>
#include <functional>
#include <atomic>
#include <thread>

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
    
private:
    std::atomic<bool> closed_{false};
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
              std::unique_ptr<ITransport> transport = std::make_unique<StdioTransport>());
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
    json storeDocument(const json& args);
    json retrieveDocument(const json& args);
    json deleteDocument(const json& args);
    json updateMetadata(const json& args);
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
    Result<std::vector<std::pair<std::string, std::string>>> resolveNameToHashes(const std::string& name);
    Result<std::vector<std::pair<std::string, std::string>>> resolveNamesToHashes(const std::vector<std::string>& names);
    Result<std::vector<std::pair<std::string, std::string>>> resolvePatternToHashes(const std::string& pattern);
    
    // Collection and snapshot helpers
    json performRestore(const std::vector<metadata::DocumentInfo>& documents,
                       const std::string& outputDir,
                       const std::string& layoutTemplate,
                       bool overwrite,
                       bool createDirs,
                       bool dryRun,
                       const std::string& scope);
    std::string expandLayoutTemplate(const std::string& layoutTemplate,
                                   const metadata::DocumentInfo& doc,
                                   const std::unordered_map<std::string, metadata::MetadataValue>& metadata);
    
    // Helper methods for hash search
    bool isValidHash(const std::string& str);
    json searchByHash(const std::string& hash, size_t limit);
    
private:
    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<search::HybridSearchEngine> hybridEngine_;
    std::unique_ptr<ITransport> transport_;
    
    std::atomic<bool> running_{false};
    std::atomic<bool> initialized_{false};
    
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
};

} // namespace yams::mcp