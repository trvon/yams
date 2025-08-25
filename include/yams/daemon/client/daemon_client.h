#pragma once

#include <filesystem>
#include <fstream>
#include <iostream>
#include <ostream>
#include <yams/core/types.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/response_of.hpp>

#include <chrono>
#include <memory>
#include <optional>

namespace yams::daemon {

struct ClientConfig {
    std::filesystem::path socketPath; // Empty = auto-detect based on runtime environment
    std::filesystem::path dataDir;    // Optional: pass data dir to daemon on auto-start
    std::chrono::milliseconds connectTimeout{1000};
    std::chrono::milliseconds requestTimeout{5000};
    size_t maxRetries = 3;
    bool autoStart = true;
    bool enableCircuitBreaker = true;
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
    Result<void> connect();
    void disconnect();
    bool isConnected() const;

    // High-level request methods
    Result<SearchResponse> search(const SearchRequest& req);
    Result<AddResponse> add(const AddRequest& req);
    Result<GetResponse> get(const GetRequest& req);
    Result<GetInitResponse> getInit(const GetInitRequest& req) { return call<GetInitRequest>(req); }
    Result<GetChunkResponse> getChunk(const GetChunkRequest& req) {
        return call<GetChunkRequest>(req);
    }
    Result<SuccessResponse> getEnd(const GetEndRequest& req) { return call<GetEndRequest>(req); }

    // High-level streaming helpers
    Result<void> getToStdout(const GetInitRequest& req) {
        if (auto c = connect(); !c)
            return c.error();
        auto init = call<GetInitRequest>(req);
        if (!init)
            return init.error();
        const auto& ir = init.value();
        if (req.metadataOnly) {
            return Result<void>();
        }
        uint64_t remaining = ir.totalSize;
        if (req.maxBytes > 0 && req.maxBytes < remaining)
            remaining = req.maxBytes;
        uint64_t offset = 0;
        const uint32_t step = ir.chunkSize > 0 ? ir.chunkSize : req.chunkSize;
        while (remaining > 0) {
            uint32_t ask = static_cast<uint32_t>(std::min<uint64_t>(remaining, step));
            GetChunkRequest creq{ir.transferId, offset, ask};
            auto cres = call<GetChunkRequest>(creq);
            if (!cres)
                return cres.error();
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
            (void)call<GetEndRequest>(ereq);
        }
        return Result<void>();
    }

    Result<void> getToFile(const GetInitRequest& req, const std::filesystem::path& outputPath) {
        if (auto c = connect(); !c)
            return c.error();
        auto init = call<GetInitRequest>(req);
        if (!init)
            return init.error();
        const auto& ir = init.value();
        if (req.metadataOnly) {
            return Result<void>();
        }
        std::ofstream out(outputPath, std::ios::binary);
        if (!out)
            return Error{ErrorCode::WriteError, "Failed to open output path"};
        uint64_t remaining = ir.totalSize;
        if (req.maxBytes > 0 && req.maxBytes < remaining)
            remaining = req.maxBytes;
        uint64_t offset = 0;
        const uint32_t step = ir.chunkSize > 0 ? ir.chunkSize : req.chunkSize;
        while (remaining > 0) {
            uint32_t ask = static_cast<uint32_t>(std::min<uint64_t>(remaining, step));
            GetChunkRequest creq{ir.transferId, offset, ask};
            auto cres = call<GetChunkRequest>(creq);
            if (!cres)
                return cres.error();
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
            (void)call<GetEndRequest>(ereq);
        }
        return Result<void>();
    }
    Result<SuccessResponse> remove(const DeleteRequest& req);
    Result<StatusResponse> status();
    Result<void> shutdown(bool graceful = true);
    Result<void> ping();

    // Embedding request methods
    Result<EmbeddingResponse> generateEmbedding(const GenerateEmbeddingRequest& req);
    Result<BatchEmbeddingResponse> generateBatchEmbeddings(const BatchEmbeddingRequest& req);
    Result<ModelLoadResponse> loadModel(const LoadModelRequest& req);
    Result<SuccessResponse> unloadModel(const UnloadModelRequest& req);
    Result<ModelStatusResponse> getModelStatus(const ModelStatusRequest& req);

    // Public method to allow generic request sending by helpers
    Result<Response> executeRequest(const Request& req);

    // Generic typed call (templated) – returns ResponseOfT<Req>
    template <class Req> Result<ResponseOfT<Req>> call(const Req& req);

    // Check if daemon is running (without connecting)
    static bool isDaemonRunning(const std::filesystem::path& socketPath = {});

    // Start daemon if not running
    static Result<void> startDaemon(const ClientConfig& config = {});

    // Path resolution helper (matches daemon's path resolution)
    static std::filesystem::path resolveSocketPath();

private:
    // Internal implementation
    class Impl;
    std::unique_ptr<Impl> pImpl;

    // Generic request sending
    Result<Response> sendRequest(const Request& req);

    // Auto-start daemon if configured and not running
    Result<void> autoStartDaemonIfNeeded();
};

// Generic typed call helper using ResponseOf trait
template <class Req> Result<ResponseOfT<Req>> DaemonClient::call(const Req& req) {
    static_assert(std::disjunction_v<
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
                      std::is_same<Req, GetStatsRequest>>,
                  "Req must be a valid daemon Request alternative");

    auto r = sendRequest(Request{req});
    if (!r)
        return r.error();
    auto& payload = r.value();
    if (auto* ok = std::get_if<ResponseOfT<Req>>(&payload))
        return *ok;
    if (auto* er = std::get_if<ErrorResponse>(&payload))
        return Error{er->code, er->message};
    return Error{ErrorCode::InvalidData, "Unexpected response type"};
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
