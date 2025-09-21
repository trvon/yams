#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <ostream>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/response_of.hpp>

#include <chrono>
#include <memory>
#include <optional>
#include <unordered_map>

namespace yams::daemon {

struct ClientConfig {
    std::filesystem::path socketPath; // Empty = auto-detect based on runtime environment
    std::filesystem::path dataDir;    // Optional: pass data dir to daemon on auto-start
    std::chrono::milliseconds connectTimeout{1000};
    std::chrono::milliseconds headerTimeout{30000}; // 30s timeout for receiving headers
    std::chrono::milliseconds bodyTimeout{60000};   // 60s timeout for receiving response body
    std::chrono::milliseconds requestTimeout{5000};
    size_t maxRetries = 3;
    bool autoStart = true;
    bool enableCircuitBreaker = true;
    bool enableChunkedResponses = true;
    size_t maxChunkSize = 512 * 1024; // increased default max chunk size
    size_t maxInflight = 128; // multiplexing: cap concurrent in-flight requests per connection
    bool progressiveOutput = true;
    // Use pooled, multiplexed connections by default. Set to true only for
    // specialized scenarios (debugging, isolation) where pooling is undesirable.
    bool singleUseConnections = false;
    bool disableStreamingForLargeQueries = false;
};

class DaemonClient {
public:
    explicit DaemonClient(const ClientConfig& config = {});
    ~DaemonClient();

    // Non-copyable but movable
    DaemonClient(const DaemonClient&) = delete;
    DaemonClient& operator=(const DaemonClient&) = delete;
    DaemonClient(DaemonClient&&) noexcept;
    DaemonClient& operator=(DaemonClient&&) noexcept;

    // Connection management
    boost::asio::awaitable<Result<void>> connect();
    // Compatibility alias for codepaths expecting connectAsync()
    boost::asio::awaitable<Result<void>> connectAsync() { co_return co_await connect(); }
    void disconnect();
    bool isConnected() const;

    // Set custom timeouts
    void setHeaderTimeout(std::chrono::milliseconds timeout);

    void setBodyTimeout(std::chrono::milliseconds timeout);

    // High-level request methods
    boost::asio::awaitable<Result<SearchResponse>> search(const SearchRequest& req);
    boost::asio::awaitable<Result<AddResponse>> add(const AddRequest& req);
    boost::asio::awaitable<Result<GetResponse>> get(const GetRequest& req);
    boost::asio::awaitable<Result<GetInitResponse>> getInit(const GetInitRequest& req) {
        return call<GetInitRequest>(req);
    }
    boost::asio::awaitable<Result<GetChunkResponse>> getChunk(const GetChunkRequest& req) {
        return call<GetChunkRequest>(req);
    }
    boost::asio::awaitable<Result<SuccessResponse>> getEnd(const GetEndRequest& req) {
        return call<GetEndRequest>(req);
    }
    boost::asio::awaitable<Result<ListResponse>> list(const ListRequest& req);
    boost::asio::awaitable<Result<GrepResponse>> grep(const GrepRequest& req);
    // Streaming path for AddDocument (header-first, final-only chunk)
    boost::asio::awaitable<Result<AddDocumentResponse>>
    streamingAddDocument(const AddDocumentRequest& req);
    boost::asio::awaitable<Result<GetStatsResponse>> getStats(const GetStatsRequest& req);
    boost::asio::awaitable<Result<UpdateDocumentResponse>>
    updateDocument(const UpdateDocumentRequest& req);
    boost::asio::awaitable<Result<PrepareSessionResponse>>
    prepareSession(const PrepareSessionRequest& req) {
        return call<PrepareSessionRequest>(req);
    }

    // High-level streaming helpers
    boost::asio::awaitable<Result<void>> getToStdout(const GetInitRequest& req) {
        if (auto c = co_await connectAsync(); !c)
            co_return c.error();
        auto init = co_await call<GetInitRequest>(req);
        if (!init)
            co_return init.error();
        const auto& ir = init.value();
        if (req.metadataOnly) {
            co_return Result<void>();
        }
        uint64_t remaining = ir.totalSize;
        if (req.maxBytes > 0 && req.maxBytes < remaining)
            remaining = req.maxBytes;
        uint64_t offset = 0;
        const uint32_t step = ir.chunkSize > 0 ? ir.chunkSize : req.chunkSize;
        while (remaining > 0) {
            uint32_t ask = static_cast<uint32_t>(std::min<uint64_t>(remaining, step));
            GetChunkRequest creq{ir.transferId, offset, ask};
            auto cres = co_await call<GetChunkRequest>(creq);
            if (!cres)
                co_return cres.error();
            auto& chunk = cres.value();
            if (!chunk.data.empty()) {
                std::cout.write(chunk.data.data(), static_cast<std::streamsize>(chunk.data.size()));
            }
            uint64_t wrote = chunk.data.size();
            if (wrote == 0 && chunk.bytesRemaining == remaining)
                break;
            offset += wrote;
            if (remaining >= wrote)
                remaining -= wrote;
            else
                remaining = 0;
            if (chunk.bytesRemaining < remaining)
                remaining = chunk.bytesRemaining;
        }
        if (ir.transferId != 0) {
            GetEndRequest ereq{ir.transferId};
            (void)co_await call<GetEndRequest>(ereq);
        }
        if (config_.singleUseConnections) {
            disconnect();
        }
        co_return Result<void>();
    }

    // Streaming grep helper method
    boost::asio::awaitable<Result<GrepResponse>> streamingGrep(const GrepRequest& req);

    // Streaming get helpers (init/header-only + chunk loop)
    boost::asio::awaitable<Result<void>> streamingGetToStdout(const GetInitRequest& req) {
        return getToStdout(req);
    }
    boost::asio::awaitable<Result<void>>
    streamingGetToFile(const GetInitRequest& req, const std::filesystem::path& outputPath) {
        return getToFile(req, outputPath);
    }

    boost::asio::awaitable<Result<void>> getToFile(const GetInitRequest& req,
                                                   const std::filesystem::path& outputPath) {
        if (auto c = co_await connectAsync(); !c)
            co_return c.error();
        auto init = co_await call<GetInitRequest>(req);
        if (!init)
            co_return init.error();
        const auto& ir = init.value();
        if (req.metadataOnly) {
            co_return Result<void>();
        }
        std::ofstream out(outputPath, std::ios::binary);
        if (!out)
            co_return Error{ErrorCode::WriteError, "Failed to open output path"};
        uint64_t remaining = ir.totalSize;
        if (req.maxBytes > 0 && req.maxBytes < remaining)
            remaining = req.maxBytes;
        uint64_t offset = 0;
        const uint32_t step = ir.chunkSize > 0 ? ir.chunkSize : req.chunkSize;
        while (remaining > 0) {
            uint32_t ask = static_cast<uint32_t>(std::min<uint64_t>(remaining, step));
            GetChunkRequest creq{ir.transferId, offset, ask};
            auto cres = co_await call<GetChunkRequest>(creq);
            if (!cres)
                co_return cres.error();
            auto& chunk = cres.value();
            if (!chunk.data.empty()) {
                out.write(chunk.data.data(), static_cast<std::streamsize>(chunk.data.size()));
            }
            uint64_t wrote = chunk.data.size();
            if (wrote == 0 && chunk.bytesRemaining == remaining)
                break;
            offset += wrote;
            if (remaining >= wrote)
                remaining -= wrote;
            else
                remaining = 0;
            if (chunk.bytesRemaining < remaining)
                remaining = chunk.bytesRemaining;
        }
        out.close();
        if (ir.transferId != 0) {
            GetEndRequest ereq{ir.transferId};
            (void)co_await call<GetEndRequest>(ereq);
        }
        if (config_.singleUseConnections) {
            disconnect();
        }
        co_return Result<void>();
    }
    boost::asio::awaitable<Result<SuccessResponse>> remove(const DeleteRequest& req);
    // Returns a lightweight snapshot; does not request detailed (no heavy scans)
    boost::asio::awaitable<Result<StatusResponse>> status();
    boost::asio::awaitable<Result<void>> shutdown(bool graceful = true);
    boost::asio::awaitable<Result<void>> ping();

    // Embedding request methods
    boost::asio::awaitable<Result<EmbeddingResponse>>
    generateEmbedding(const GenerateEmbeddingRequest& req);
    boost::asio::awaitable<Result<BatchEmbeddingResponse>>
    generateBatchEmbeddings(const BatchEmbeddingRequest& req);
    // Streaming progress variants (emit EmbeddingEvent progress, return final response)
    boost::asio::awaitable<Result<BatchEmbeddingResponse>>
    streamingBatchEmbeddings(const BatchEmbeddingRequest& req);
    boost::asio::awaitable<Result<EmbedDocumentsResponse>>
    streamingEmbedDocuments(const EmbedDocumentsRequest& req);
    // Unified call overload for embeddings: uses streaming under the hood
    boost::asio::awaitable<Result<EmbedDocumentsResponse>> call(const EmbedDocumentsRequest& req) {
        co_return co_await streamingEmbedDocuments(req);
    }

    // Collect progress events for embeddings (no stdout printing)
    boost::asio::awaitable<Result<std::vector<EmbeddingEvent>>>
    callEvents(const EmbedDocumentsRequest& req);
    boost::asio::awaitable<Result<ModelLoadResponse>> loadModel(const LoadModelRequest& req);
    boost::asio::awaitable<Result<SuccessResponse>> unloadModel(const UnloadModelRequest& req);
    boost::asio::awaitable<Result<ModelStatusResponse>>
    getModelStatus(const ModelStatusRequest& req);

    // Chunked response handling
    struct ChunkedResponseHandler {
        // Called when header is received
        virtual void onHeaderReceived(const Response&) {}

        // Called for each chunk
        virtual bool onChunkReceived(const Response&, bool) { return true; }

        // Called on error
        virtual void onError(const Error&) {}

        // Called when completed
        virtual void onComplete() {}

        virtual ~ChunkedResponseHandler() = default;
    };

    // Streaming response handlers
    // Streaming search with progressive output
    class StreamingSearchHandler : public ChunkedResponseHandler {
    public:
        explicit StreamingSearchHandler(bool pathsOnly = false, size_t limit = 0)
            : pathsOnly_(pathsOnly), limit_(limit), count_(0) {}

        void onHeaderReceived(const Response& headerResponse) override;
        bool onChunkReceived(const Response& chunkResponse, bool isLastChunk) override;
        void onError(const Error& error) override;
        void onComplete() override;

        // Get accumulated results
        Result<SearchResponse> getResults() const;

    private:
        bool pathsOnly_ = false;
        size_t limit_ = 0;
        size_t count_ = 0;
        std::vector<SearchResult> results_;
        std::optional<Error> error_;
        std::chrono::milliseconds elapsed_{0};
        size_t totalCount_ = 0;
    };

    // Streaming response handlers for list command
    class StreamingListHandler : public ChunkedResponseHandler {
    public:
        explicit StreamingListHandler(bool pathsOnly = false, size_t limit = 0)
            : pathsOnly_(pathsOnly), limit_(limit), count_(0) {}
        void onHeaderReceived(const Response& headerResponse) override;
        bool onChunkReceived(const Response& chunkResponse, bool isLastChunk) override;
        void onError(const Error& error) override;
        void onComplete() override;

        // Get accumulated results
        Result<ListResponse> getResults() const;

    private:
        bool pathsOnly_ = false;
        size_t limit_ = 0;
        size_t count_ = 0;
        std::vector<ListEntry> items_;
        std::optional<Error> error_;
        uint64_t totalCount_ = 0;
    };

    // Streaming response handler for grep
    class StreamingGrepHandler : public ChunkedResponseHandler {
    public:
        explicit StreamingGrepHandler(bool pathsOnly = false, size_t perFileMax = 0)
            : pathsOnly_(pathsOnly), perFileMax_(perFileMax) {}

        void onHeaderReceived(const Response& headerResponse) override;
        bool onChunkReceived(const Response& chunkResponse, bool isLastChunk) override;
        void onError(const Error& error) override;
        void onComplete() override;

        Result<GrepResponse> getResults() const;

    private:
        bool pathsOnly_ = false;
        size_t perFileMax_ = 0; // If non-zero, stop after N matches per file
        std::vector<GrepMatch> matches_;
        std::optional<Error> error_;
        size_t totalMatches_ = 0;
        size_t filesSearched_ = 0;
        // Track counts per file to enforce perFileMax_
        std::unordered_map<std::string, size_t> perFileCount_;
    };

    // Streaming search helper method
    boost::asio::awaitable<Result<SearchResponse>> streamingSearch(const SearchRequest& req);

    // Streaming list helper method
    boost::asio::awaitable<Result<ListResponse>> streamingList(const ListRequest& req);

    // Public method to allow generic request sending by helpers
    boost::asio::awaitable<Result<Response>> executeRequest(const Request& req);

    // Generic typed call (templated) – returns ResponseOfT<Req>
    template <class Req> boost::asio::awaitable<Result<ResponseOfT<Req>>> call(const Req& req);

    // Check if daemon is running (without connecting)
    static bool isDaemonRunning(const std::filesystem::path& socketPath = {});

    // Start daemon if not running
    static Result<void> startDaemon(const ClientConfig& config = {});

    // Helper method to set environment variables for timeouts
    static void setTimeoutEnvVars(std::chrono::milliseconds headerTimeout,
                                  std::chrono::milliseconds bodyTimeout);

    // Path resolution helper (matches daemon's path resolution)
    static std::filesystem::path resolveSocketPath();
    // Config-first path resolution: prefer env/config, then fall back to defaults
    static std::filesystem::path resolveSocketPathConfigFirst();

    // Set whether to use streaming for all operations
    void setStreamingEnabled(bool enabled);

private:
    // Internal implementation
    class Impl;
    std::unique_ptr<Impl> pImpl;
    ClientConfig config_;

    // Generic request sending
    boost::asio::awaitable<Result<Response>> sendRequest(const Request& req);

    // Send request with chunked response handling
    boost::asio::awaitable<Result<void>>
    sendRequestStreaming(const Request& req, std::shared_ptr<ChunkedResponseHandler> handler);

    // Auto-start daemon if configured and not running
    Result<void> autoStartDaemonIfNeeded();

    // Legacy POSIX helpers removed; AsioTransportAdapter handles framing.
};

// Generic typed call helper using ResponseOf trait
template <class Req>
boost::asio::awaitable<Result<ResponseOfT<Req>>> DaemonClient::call(const Req& req) {
    static_assert(
        std::disjunction_v<
            std::is_same<Req, SearchRequest>, std::is_same<Req, AddRequest>,
            std::is_same<Req, GetRequest>, std::is_same<Req, GetInitRequest>,
            std::is_same<Req, GetChunkRequest>, std::is_same<Req, GetEndRequest>,
            std::is_same<Req, DeleteRequest>, std::is_same<Req, ListRequest>,
            std::is_same<Req, ShutdownRequest>, std::is_same<Req, StatusRequest>,
            std::is_same<Req, PingRequest>, std::is_same<Req, GenerateEmbeddingRequest>,
            std::is_same<Req, BatchEmbeddingRequest>, std::is_same<Req, LoadModelRequest>,
            std::is_same<Req, UnloadModelRequest>, std::is_same<Req, ModelStatusRequest>,
            std::is_same<Req, AddDocumentRequest>, std::is_same<Req, GrepRequest>,
            std::is_same<Req, UpdateDocumentRequest>, std::is_same<Req, DownloadRequest>,
            std::is_same<Req, GetStatsRequest>, std::is_same<Req, PrepareSessionRequest>,
            std::is_same<Req, EmbedDocumentsRequest>,
            // Plugin management requests
            std::is_same<Req, PluginScanRequest>, std::is_same<Req, PluginLoadRequest>,
            std::is_same<Req, PluginUnloadRequest>, std::is_same<Req, PluginTrustListRequest>,
            std::is_same<Req, PluginTrustAddRequest>, std::is_same<Req, PluginTrustRemoveRequest>>,
        "Req must be a valid daemon Request alternative");

    // Force streaming for streaming-capable requests
    if constexpr (std::is_same_v<Req, SearchRequest>) {
        co_return co_await streamingSearch(req);
    } else if constexpr (std::is_same_v<Req, ListRequest>) {
        co_return co_await streamingList(req);
    } else if constexpr (std::is_same_v<Req, GrepRequest>) {
        co_return co_await streamingGrep(req);
    }

    auto r = co_await sendRequest(Request{req});
    if (!r)
        co_return r.error();
    auto& payload = r.value();
    if (auto* ok = std::get_if<ResponseOfT<Req>>(&payload))
        co_return *ok;
    if (auto* er = std::get_if<ErrorResponse>(&payload))
        co_return Error{er->code, er->message};
    co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
}

// ============================================================================
// Circuit Breaker for Resilience
// ============================================================================

class CircuitBreaker {
public:
    enum class State {
        Closed,  // Normal operation
        Open,    // Failing, reject requests
        HalfOpen // Testing recovery
    };

    struct Config {
        size_t failureThreshold;
        size_t successThreshold;
        std::chrono::seconds openTimeout;
        std::chrono::seconds halfOpenTimeout;

        Config() : failureThreshold(5), successThreshold(2), openTimeout(30), halfOpenTimeout(10) {}
    };

    explicit CircuitBreaker(const Config& config = Config{});

    // Check if request should be allowed
    bool shouldAllow();

    // Record result
    void recordSuccess();
    void recordFailure();

    State getState() const { return state_; }

private:
    void transitionTo(State newState);
    bool shouldTransitionToHalfOpen() const;

    State state_ = State::Closed;
    Config config_;

    size_t consecutiveFailures_ = 0;
    size_t consecutiveSuccesses_ = 0;

    std::chrono::steady_clock::time_point lastStateChange_;
    std::chrono::steady_clock::time_point openedAt_;
};

} // namespace yams::daemon
