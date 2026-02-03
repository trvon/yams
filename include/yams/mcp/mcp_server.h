#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/strand.hpp>
#if !defined(YAMS_WASI)
#include <yams/api/content_store.h>
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/list_input_resolver.hpp>
#include <yams/app/services/retrieval_service.h>
#include <yams/app/services/services.hpp>
#endif
#if !defined(YAMS_WASI)
#include <yams/cli/daemon_helpers.h>
#endif
#include <yams/core/types.h>
#if !defined(YAMS_WASI)
#include <yams/daemon/client/daemon_client.h>
#endif
#include <yams/mcp/error_handling.h>
#include <yams/mcp/tool_registry.h>
#if !defined(YAMS_WASI)
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#endif
#include <yams/version.hpp>

#include <nlohmann/json.hpp>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <functional>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::mcp {

using json = nlohmann::json;

// Custom protocol / server-specific error codes
// Reserve a code for unsupported protocol version negotiation failures
static constexpr int kErrUnsupportedProtocolVersion = -32901;

/**
 * Transport interface for MCP communication with modern error handling
 */
class ITransport {
public:
    virtual ~ITransport() = default;
    virtual void send(const json& message) = 0;
    virtual MessageResult receive() = 0;
    virtual bool isConnected() const = 0;
    virtual void close() = 0;
    virtual TransportState getState() const = 0;
};

/**
 * Standard I/O transport (default for MCP) with atomic state management.
 * Implements MCP stdio specification: outputs newline-delimited JSON (NDJSON).
 * Per MCP spec: "Messages are delimited by newlines and MUST NOT contain embedded newlines."
 *
 * For backwards compatibility, can read LSP-style Content-Length framing on input,
 * but always outputs spec-compliant NDJSON.
 */
class StdioTransport : public ITransport {
public:
    StdioTransport();
    ~StdioTransport(); // Ensures writer thread is joined/flushed for clean shutdown
    void send(const json& message) override;
    // NDJSON send - same as send() for stdio (spec-compliant)
    void sendNdjson(const json& message);
    // Enqueue a message for the writer thread (non-blocking for request handlers)
    void sendAsync(json message);
    // Framed send helper for pre-serialized JSON payloads (legacy compatibility)
    void sendFramedSerialized(const std::string& payload);
    MessageResult receive() override;
    bool isConnected() const override { return state_.load() == TransportState::Connected; }
    void close() override { state_.store(TransportState::Closing); }
    TransportState getState() const override { return state_.load(); }

    // Set external shutdown flag for non-blocking checks
    void setShutdownFlag(std::atomic<bool>* shutdown) { externalShutdown_ = shutdown; }

private:
    // Unified non-blocking sender for all transports. Uses async send when available
    // and falls back to a best-effort synchronous send otherwise.

    std::atomic<TransportState> state_{TransportState::Connected};
    std::atomic<bool>* externalShutdown_{nullptr};
    std::atomic<size_t> errorCount_{0};

    // Receive poll timeout (ms). Default 500ms; configurable via env YAMS_MCP_RECV_TIMEOUT_MS.
    int recvTimeoutMs_{500};

    // Mutex for thread-safe output operations (sending only) - instance member for proper RAII
    mutable std::mutex outMutex_;
    // Outbound writer queue + thread to avoid blocking on stdout writes
    std::mutex queueMutex_;
    std::condition_variable queueCv_;
    std::deque<std::string> outQueue_;
    std::thread writerThread_;
    std::atomic<bool> writerRunning_{false};
    void writerLoop();

    // Helper for non-blocking stdin check
    bool isInputAvailable(int timeoutMs = 100) const;

    void sendSerialized(const std::string& payload);

    // Error recovery and circuit breaker
    bool shouldRetryAfterError() const noexcept;
    void recordError() noexcept;
    void resetErrorCount() noexcept;
};

// WebSocket transport removed - not needed for current implementation
// TODO: Add back if WebSocket support is required in the future

/**
 * MCP Server implementation
 */
class MCPServer {
public:
    MCPServer(std::unique_ptr<ITransport> transport, std::atomic<bool>* externalShutdown = nullptr,
              std::filesystem::path overrideSocket = {},
              std::optional<boost::asio::any_io_executor> executor = std::nullopt);
    ~MCPServer();

    void start();
    // Async variant (preferred) when transport supports async stdio
    boost::asio::awaitable<void> startAsync();
    void stop();
    bool isRunning() const { return running_.load(); }

#if defined(YAMS_TESTING) && !defined(YAMS_WASI)
    // Testing hooks: allow unit tests to intercept daemon client creation and
    // validate the resolved socket path and dataDir without making a real connection.
    void
    setEnsureDaemonClientHook(std::function<Result<void>(const yams::daemon::ClientConfig&)> fn) {
        testEnsureDaemonClientHook_ = std::move(fn);
    }
    void setDaemonClientSocketPathForTest(const std::filesystem::path& p) {
        daemon_client_config_.socketPath = p;
        daemon_client_ = nullptr;
        daemon_client_lease_.reset();
        // Service facades hold a non-owning shared_ptr<DaemonClient>. When the lease is reset
        // (and/or the CLI pool is reset), those pointers can become dangling; force rebind.
        retrieval_svc_.reset();
        ingestion_svc_.reset();
        yams::cli::cli_pool_reset_for_test();
    }
#endif

    // Public wrappers for HTTP mode (bridge to internal handlers)
    MessageResult handleRequestPublic(const nlohmann::json& request) {
        return handleRequest(request);
    }
    nlohmann::json callToolPublic(const std::string& name, const nlohmann::json& arguments) {
        return callTool(name, arguments);
    }

    // HTTP mode: per-request session context to route notifications via SSE
    class SessionContext {
    public:
        SessionContext(MCPServer& server, std::string sessionId,
                       std::function<void(const std::string&, const nlohmann::json&)> publisher)
            : server_(server) {
            server_.beginSessionContext(std::move(sessionId), std::move(publisher));
        }
        ~SessionContext() { server_.endSessionContext(); }

    private:
        MCPServer& server_;
    };

private:
    // Refactored members: No direct backend components
    std::atomic<bool> running_{false};
    std::unique_ptr<ITransport> transport_;
    StdioTransport* cachedStdioTransport_{nullptr}; // Cache to avoid repeated dynamic_cast
    std::unique_ptr<ToolRegistry> toolRegistry_;
    std::atomic<bool>* externalShutdown_;
    std::atomic<bool> exitRequested_{false};     // Set when handling explicit 'exit' request
    std::atomic<bool> shutdownRequested_{false}; // Set when 'shutdown' request received

    // Exposed capability flags (snapshotted during initialize)
    bool cancellationSupported_{true};
    bool progressSupported_{true}; // progress notifications scaffold enabled
    // --- Handshake / protocol behavior flags (env-configurable) ---
    bool eagerReadyEnabled_{
        false}; // YAMS_MCP_EAGER_READY=1 -> send ready immediately after initialize response
    bool autoReadyEnabled_{true}; // YAMS_MCP_AUTO_READY=0 to disable auto-ready fallback
    int autoReadyDelayMs_{150};   // YAMS_MCP_READY_DELAY_MS (minimum 20ms)
    bool strictProtocol_{
        false}; // YAMS_MCP_STRICT_PROTOCOL=1 to require explicit supported protocolVersion or fail
    bool limitToolResultDup_{
        true}; // YAMS_MCP_LIMIT_DUP_CONTENT=0 to allow full data duplication for large tool results
    bool handshakeTrace_{
        false}; // YAMS_MCP_HANDSHAKE_TRACE=1 to emit notifications/log lifecycle traces
    bool enableYamsExtensions_{true}; // YAMS_DISABLE_EXTENSIONS=1 to disable YAMS-specific features

    // Throttle concurrent MCP add/store operations (stability under high add rates)
    std::atomic<int> addInFlight_{0};
    // Handshake state / diagnostics
    std::atomic<int> readyEagerCount_{0};
    std::atomic<int> readyAutoCount_{0};
    std::atomic<int> readyClientCount_{0};
    std::atomic<bool> initializedNotificationSeen_{false};
    std::atomic<bool> earlyFeatureUse_{
        false}; // Set when client invokes feature (tools/list, tools/call) pre-initialized

    // --- MCP Apps Extension Support ---
    std::atomic<bool> mcpAppsSupported_{
        false};                   // Set when client supports io.modelcontextprotocol/ui
    std::string mcpAppsMimeType_; // The negotiated mime type (e.g., "text/html;profile=mcp-app")

    // --- Cancellation scaffolding ---
    // Each in-flight request id can be marked cancelable; a cancellation sets the token to true.
    mutable std::mutex cancelMutex_;
    std::unordered_map<std::string, std::shared_ptr<std::atomic<bool>>> cancelTokens_;

    // Single multiplexed daemon client lease (shared transport context)
#if !defined(YAMS_WASI)
    std::shared_ptr<yams::cli::DaemonClientPool::Lease> daemon_client_lease_;
    yams::daemon::DaemonClient* daemon_client_{nullptr};
    yams::daemon::ClientConfig daemon_client_config_{};
    std::filesystem::path daemonSocketOverride_;
    std::function<Result<void>(const yams::daemon::ClientConfig&)> testEnsureDaemonClientHook_{};

    // Service facades sharing daemon_client_ via non-owning shared_ptr
    std::unique_ptr<app::services::RetrievalService> retrieval_svc_;
    std::unique_ptr<app::services::DocumentIngestionService> ingestion_svc_;
#else
    std::filesystem::path daemonSocketOverride_;
#endif
    struct ClientInfo {
        std::string name;
        std::string version;
    };

    // Methods
    MessageResult handleRequest(const nlohmann::json& request);
    boost::asio::awaitable<MessageResult> handleRequestAsync(const nlohmann::json& request);
    nlohmann::json initialize(const nlohmann::json& params);
    nlohmann::json listTools();
    nlohmann::json callTool(const std::string& name, const nlohmann::json& arguments);
    boost::asio::awaitable<nlohmann::json> callToolAsync(const std::string& name,
                                                         const nlohmann::json& arguments);
    nlohmann::json listResources();
    nlohmann::json readResource(const std::string& uri);
    nlohmann::json listPrompts();
    void initializeToolRegistry();
    nlohmann::json createResponse(const nlohmann::json& id, const nlohmann::json& result);
    nlohmann::json createError(const nlohmann::json& id, int code, const std::string& message);
    void sendResponse(const nlohmann::json& message);

    // --- Initialization / lifecycle helpers (added for spec-aligned handshake flexibility) ---
    bool isMethodAllowedBeforeInitialization(const std::string& method) const;
    void markClientInitialized(); // Accept canonical + legacy initialized notifications
    void handleExitRequest();     // Graceful handling of 'exit' to set exitRequested_
    // Cancellation helpers
    void registerCancelable(const nlohmann::json& id);
    void cancelRequest(const nlohmann::json& id);
    bool isCanceled(const nlohmann::json& id) const;
    // Capability builder (augments base capabilities with cancellation / legacy flags)
    nlohmann::json buildServerCapabilities() const;
    // Cancel request handler (JSON-RPC "cancel" -> params { "id": <original request id> })
    void handleCancelRequest(const nlohmann::json& params, const nlohmann::json& id);
    // Progress notification helper (emits "notifications/progress")
    void sendProgress(const std::string& phase, double percent, const std::string& message = "",
                      std::optional<nlohmann::json> progressToken = std::nullopt);
    // Auto-ready scheduling (fallback when client omits 'initialized')
    void scheduleAutoReady();
    bool shouldAutoInitialize() const;
    // Record that a feature was used prior to client 'initialized'
    void recordEarlyFeatureUse();

    Result<void> ensureDaemonClient();

    // YAMS extensions toggle (independent of strict mode which has been removed)
    bool areYamsExtensionsEnabled() const { return enableYamsExtensions_; }

    // HTTP session context controls
    void
    beginSessionContext(std::string sessionId,
                        std::function<void(const std::string&, const nlohmann::json&)> publisher);
    void endSessionContext();

    // --- MCP listChanged notification (MCP 2025-06-18 spec) ---
    void notifyToolsListChanged();

    // --- Unified outbound mechanism (strand-like ordering on IO context) ---
    void enqueueOutbound(std::string payload);
    boost::asio::awaitable<void> outboundDrainAsync();

    std::mutex outboundMutex_;
    std::deque<std::string> outboundQueue_;
    std::atomic<bool> outboundDraining_{false};
    std::unique_ptr<boost::asio::strand<boost::asio::any_io_executor>> outboundStrand_;

    // Notification routing for HTTP mode
    static thread_local std::string tlsSessionId_;
    // Spec-compliant progress token associated with the current in-flight request (if any)
    static thread_local nlohmann::json tlsProgressToken_;
    std::function<void(const std::string&, const nlohmann::json&)> httpPublisher_;

    // Telemetry counters (FSM-integrated)
    std::atomic<uint64_t> telemetrySentBytes_{0};
    std::atomic<uint64_t> telemetryIntegrityFailures_{0};

    // File-backed prompts directory (configurable)
    std::filesystem::path promptsDir_;
#ifdef YAMS_TESTING
public:
    void testSetPromptsDir(const std::filesystem::path& p) { promptsDir_ = p; }
#endif

#if defined(YAMS_TESTING) && !defined(YAMS_WASI)
public:
    // Public testing interface - only available when building tests
    json testListTools() { return listTools(); }

    boost::asio::awaitable<json> testCallToolAsync(const std::string& name, const json& arguments) {
        return callToolAsync(name, arguments);
    }

    void testShutdown() { stop(); }

    void testConfigureDaemonClient(const yams::daemon::ClientConfig& cfg) {
        daemon_client_config_ = cfg;
        daemon_client_ = nullptr;
        daemon_client_lease_.reset();
    }

    // Expose modern handle* methods for testing
    // Request types are already in the yams::mcp namespace

    // Test methods for unit testing
    boost::asio::awaitable<Result<MCPSearchResponse>>
    testHandleSearchDocuments(const MCPSearchRequest& req) {
        return handleSearchDocuments(req);
    }
    boost::asio::awaitable<Result<MCPGrepResponse>>
    testHandleGrepDocuments(const MCPGrepRequest& req) {
        return handleGrepDocuments(req);
    }
    boost::asio::awaitable<Result<MCPRetrieveDocumentResponse>>
    testHandleRetrieveDocument(const MCPRetrieveDocumentRequest& req) {
        return handleRetrieveDocument(req);
    }
    boost::asio::awaitable<Result<MCPListDocumentsResponse>>
    testHandleListDocuments(const MCPListDocumentsRequest& req) {
        return handleListDocuments(req);
    }
    boost::asio::awaitable<Result<MCPStatsResponse>>
    testHandleGetStats(const MCPStatsRequest& req) {
        return handleGetStats(req);
    }

    void testSetEnsureDaemonClientHook(
        std::function<Result<void>(const yams::daemon::ClientConfig&)> hook) {
        testEnsureDaemonClientHook_ = std::move(hook);
    }
#endif

private:
#if defined(YAMS_WASI)
    // WASI profile: only the minimal protocol surface is supported.
    // Keep these as lightweight stubs so we don't pull in the full YAMS stack.
    // NOTE: The generic handlers (listTools/listResources/...) are already declared above.
    // The WASI build provides simplified implementations in the .cpp guarded by YAMS_WASI.
    // (No additional declarations needed here.)
#endif

    // Modern C++20 tool handlers (type-safe, clean)
    boost::asio::awaitable<Result<MCPSearchResponse>>
    handleSearchDocuments(const MCPSearchRequest& req);
    boost::asio::awaitable<Result<MCPGrepResponse>> handleGrepDocuments(const MCPGrepRequest& req);
    boost::asio::awaitable<Result<MCPDownloadResponse>>
    handleDownload(const MCPDownloadRequest& req);
    boost::asio::awaitable<Result<MCPStoreDocumentResponse>>
    handleStoreDocument(const MCPStoreDocumentRequest& req);
    boost::asio::awaitable<Result<MCPRetrieveDocumentResponse>>
    handleRetrieveDocument(const MCPRetrieveDocumentRequest& req);
    boost::asio::awaitable<Result<MCPListDocumentsResponse>>
    handleListDocuments(const MCPListDocumentsRequest& req);
    boost::asio::awaitable<Result<MCPStatsResponse>> handleGetStats(const MCPStatsRequest& req);
    boost::asio::awaitable<Result<MCPStatusResponse>> handleGetStatus(const MCPStatusRequest& req);
    boost::asio::awaitable<Result<MCPDoctorResponse>> handleDoctor(const MCPDoctorRequest& req);
    boost::asio::awaitable<Result<MCPAddDirectoryResponse>>
    handleAddDirectory(const MCPAddDirectoryRequest& req);
    boost::asio::awaitable<Result<MCPGetByNameResponse>>
    handleGetByName(const MCPGetByNameRequest& req);
    boost::asio::awaitable<Result<MCPDeleteByNameResponse>>
    handleDeleteByName(const MCPDeleteByNameRequest& req);
    boost::asio::awaitable<Result<MCPCatDocumentResponse>>
    handleCatDocument(const MCPCatDocumentRequest& req);
    boost::asio::awaitable<Result<MCPUpdateMetadataResponse>>
    handleUpdateMetadata(const MCPUpdateMetadataRequest& req);
    boost::asio::awaitable<Result<MCPRestoreCollectionResponse>>
    handleRestoreCollection(const MCPRestoreCollectionRequest& req);
    boost::asio::awaitable<Result<MCPRestoreSnapshotResponse>>
    handleRestoreSnapshot(const MCPRestoreSnapshotRequest& req);
    boost::asio::awaitable<Result<MCPRestoreResponse>> handleRestore(const MCPRestoreRequest& req);
    boost::asio::awaitable<Result<MCPListCollectionsResponse>>
    handleListCollections(const MCPListCollectionsRequest& req);
    boost::asio::awaitable<Result<MCPListSnapshotsResponse>>
    handleListSnapshots(const MCPListSnapshotsRequest& req);
    boost::asio::awaitable<Result<MCPGraphResponse>> handleGraphQuery(const MCPGraphRequest& req);

    // Session start/stop (simplified surface)
    boost::asio::awaitable<Result<MCPSessionStartResponse>>
    handleSessionStart(const MCPSessionStartRequest& req);
    boost::asio::awaitable<Result<MCPSessionStopResponse>>
    handleSessionStop(const MCPSessionStopRequest& req);
    boost::asio::awaitable<Result<MCPSessionPinResponse>>
    handleSessionPin(const MCPSessionPinRequest& req);
    boost::asio::awaitable<Result<MCPSessionUnpinResponse>>
    handleSessionUnpin(const MCPSessionUnpinRequest& req);
    boost::asio::awaitable<Result<MCPSessionWatchResponse>>
    handleSessionWatch(const MCPSessionWatchRequest& req);

    // (Removed legacy JSON helper declarations – use typed async tool handlers via ToolRegistry)

    // Helper methods

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

#if !defined(YAMS_WASI)
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

    // Core services
    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<search::SearchEngine> searchEngine_;

    // App context and services for business logic
    app::services::AppContext appContext_;
    std::shared_ptr<app::services::ISearchService> searchService_;
    std::shared_ptr<app::services::IGrepService> grepService_;
    std::shared_ptr<app::services::IDocumentService> documentService_;
    std::shared_ptr<app::services::IDownloadService> downloadService_;
    std::shared_ptr<app::services::IIndexingService> indexingService_;
    std::shared_ptr<app::services::IStatsService> statsService_;
    std::unique_ptr<yams::metadata::ConnectionPool> localMetaPool_;
#endif

    std::atomic<bool> initialized_{false};
    // readyPending_ removed (deprecated after canonical tools/call refactor)

    // Server info
    struct {
        std::string name = "yams-mcp";
        std::string version = YAMS_VERSION_STRING;
    } serverInfo_;

    // Instance ID: unique per MCP connection / server lifetime
    std::string instanceId_;

    // Client info (set during initialize)
    ClientInfo clientInfo_;

    // Negotiated protocol version (set during initialize)
    std::string negotiatedProtocolVersion_{"2025-06-18"};

public:
    // Process a single JSON-RPC message. For requests (with id), returns a JSON-RPC response.
    // For notifications (no id), returns an error for no response expected.
    MessageResult processMessage(const json& message) {
        auto result = json_utils::validate_jsonrpc_message(message);
        if (!result) {
            return result.error();
        }
        return handleRequest(result.value());
    }
};

} // namespace yams::mcp
