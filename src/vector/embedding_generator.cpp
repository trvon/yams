#define _CRT_SECURE_NO_WARNINGS
#include <spdlog/spdlog.h>
#include <filesystem>
#include <future>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/common/utf8_utils.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/profiling.h>
#include <yams/vector/embedding_generator.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <iomanip>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <regex>
#include <shared_mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <yams/daemon/components/TuneAdvisor.h>

// Include daemon client for DaemonBackend
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/ml/provider.h>
#include <yams/vector/simeon_embedding_backend.h>

namespace yams::vector {

// Forward declarations
class DaemonBackend;
class HybridBackend;

/**
 * DaemonBackend - Daemon IPC backend
 * Communicates with the daemon service for embedding generation
 */
// Local awaitable bridge (build-only) to await daemon calls without legacy async_bridge
template <typename T, typename MakeAwaitable>
static yams::Result<T> await_with_timeout(MakeAwaitable&& make, std::chrono::milliseconds timeout) {
    auto shared_promise = std::make_shared<std::promise<yams::Result<T>>>();
    auto fut = shared_promise->get_future();
    auto completed = std::make_shared<std::atomic<bool>>(false);

    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [state = shared_promise, completed,
         maker = std::forward<MakeAwaitable>(make)]() mutable -> boost::asio::awaitable<void> {
            try {
                auto result = co_await maker();
                if (!completed->exchange(true)) {
                    state->set_value(std::move(result));
                }
            } catch (const std::exception& ex) {
                if (!completed->exchange(true)) {
                    state->set_value(yams::Error{yams::ErrorCode::Unknown,
                                                 std::string("await exception: ") + ex.what()});
                }
            } catch (...) {
                if (!completed->exchange(true)) {
                    state->set_value(yams::Error{yams::ErrorCode::Unknown, "await exception"});
                }
            }
            co_return;
        },
        boost::asio::detached);

    if (timeout.count() > 0) {
        const auto status = fut.wait_for(timeout);
        if (status != std::future_status::ready) {
            if (!completed->exchange(true)) {
                try {
                    shared_promise->set_value(
                        yams::Error{yams::ErrorCode::Timeout, "await timeout"});
                } catch (...) {
                    // ignore double set_value races
                }
            }
            return yams::Error{yams::ErrorCode::Timeout, "await timeout"};
        }
    } else {
        fut.wait();
    }

    return fut.get();
}

class DaemonBackend : public IEmbeddingBackend {
public:
    explicit DaemonBackend(const EmbeddingConfig& config) : config_(config), initialized_(false) {}

    bool initialize() override {
        YAMS_ZONE_SCOPED_N("DaemonBackend::initialize");

        if (initialized_) {
            return true;
        }

        try {
            // If running inside the daemon process, prefer an in-process embedding provider to
            // avoid recursive IPC calls back into the daemon.
            if (const char* inproc = std::getenv("YAMS_IN_DAEMON")) {
                std::string v(inproc);
                for (auto& c : v)
                    c = static_cast<char>(std::tolower(c));
                if (!v.empty() && v != "0" && v != "false" && v != "off" && v != "no") {
                    auto isTruthy = [](const char* value) {
                        if (!value || !*value) {
                            return false;
                        }
                        std::string normalized(value);
                        for (auto& c : normalized) {
                            c = static_cast<char>(std::tolower(c));
                        }
                        return normalized != "0" && normalized != "false" && normalized != "off" &&
                               normalized != "no";
                    };

                    // In synthetic/unit tests we intentionally force a deterministic mock provider
                    // while running in-daemon mode.
                    if (isTruthy(std::getenv("YAMS_USE_MOCK_PROVIDER"))) {
                        fallback_provider_ = yams::ml::createEmbeddingProvider("Mock");
                    } else {
                        fallback_provider_ = yams::ml::createEmbeddingProvider();
                    }
                    if (fallback_provider_ && fallback_provider_->isAvailable()) {
                        auto initRes = fallback_provider_->initialize();
                        if (!initRes) {
                            spdlog::warn("DaemonBackend fallback provider init failed: {}",
                                         initRes.error().message);
                            fallback_provider_.reset();
                        } else {
                            fallback_initialized_ = true;
                            cached_dim_ = fallback_provider_->getEmbeddingDimension();
                            cached_seq_len_ = fallback_provider_->getMaxSequenceLength();
                            initialized_ = true;
                            spdlog::info("DaemonBackend using in-process embedding provider '{}'",
                                         fallback_provider_->getProviderName());
                            return true;
                        }
                    } else {
                        fallback_provider_.reset();
                        spdlog::warn("DaemonBackend could not obtain in-process embedding "
                                     "provider; falling back to IPC backend");
                    }
                }
            }

            // Configure daemon client with socket/auto-start from config
            daemon::ClientConfig dcfg;
            // If socket path is empty, let DaemonClient auto-resolve it
            if (!config_.daemon_socket.empty()) {
                dcfg.socketPath = config_.daemon_socket;
            }
            // Otherwise socketPath remains empty and DaemonClient will resolve it
            dcfg.requestTimeout = config_.daemon_timeout;
            dcfg.maxRetries = config_.daemon_max_retries;
            dcfg.autoStart = config_.daemon_auto_start;
            daemon_client_ = std::make_shared<daemon::DaemonClient>(dcfg);

            // No explicit connect here; transport connects lazily per request.

            // Verify daemon is responsive via DaemonClient, then request model preload (non-fatal
            // on error)
            auto st = await_with_timeout<yams::daemon::StatusResponse>(
                [&]() { return daemon_client_->status(); }, std::chrono::seconds(5));
            if (!st) {
                // Downgrade to debug to avoid noisy warnings during CLI init paths.
                // Search will gracefully fall back when daemon is unavailable/slow.
                spdlog::debug("Daemon status probe failed: {}", st.error().message);
            } else if (!config_.model_name.empty()) {
                const auto& s = st.value();
                bool provider_ready = false;
                // Prefer readiness flag when available
                auto it = s.readinessStates.find("model_provider");
                if (it != s.readinessStates.end()) {
                    provider_ready = it->second;
                }
                // Or presence of a provider entry in models
                if (!provider_ready) {
                    for (const auto& m : s.models) {
                        if (m.name == "(provider)") {
                            provider_ready = true;
                            break;
                        }
                    }
                }
                if (provider_ready) {
                    bool model_already_loaded = false;
                    try {
                        for (const auto& m : s.models) {
                            if (m.name == config_.model_name) {
                                model_already_loaded = true;
                                break;
                            }
                        }
                    } catch (...) {
                    }

                    // Allow extended preload timeout via env (default 30s)
                    std::chrono::milliseconds preload_timeout = std::chrono::seconds(30);
                    if (const char* t = std::getenv("YAMS_MODEL_PRELOAD_TIMEOUT_MS")) {
                        try {
                            long v = std::stol(std::string(t));
                            if (v > 0)
                                preload_timeout = std::chrono::milliseconds(v);
                        } catch (...) {
                        }
                    }

                    if (model_already_loaded) {
                        spdlog::debug(
                            "Daemon already has model loaded; skipping preload (model={})",
                            config_.model_name);
                    } else {
                        daemon::LoadModelRequest req;
                        req.modelName = config_.model_name;
                        req.preload = true;
                        auto lm = await_with_timeout<yams::daemon::ModelLoadResponse>(
                            [&]() { return daemon_client_->loadModel(req); }, preload_timeout);
                        if (!lm) {
                            spdlog::debug("Preload model in daemon did not complete: {}",
                                          lm.error().message);
                        }
                    }
                } else {
                    spdlog::debug("Daemon model provider not ready; skipping preload");
                }
            }

            initialized_ = true;
            spdlog::info("DaemonBackend connected to daemon service");
            return true;
        } catch (const std::exception& e) {
            spdlog::error("DaemonBackend initialization failed: {}", e.what());
            return false;
        }
    }

    void shutdown() override {
        if (daemon_client_) {
            daemon_client_->disconnect();
            daemon_client_.reset();
        }
        if (fallback_provider_) {
            try {
                fallback_provider_->shutdown();
            } catch (...) {
            }
            fallback_provider_.reset();
        }
        fallback_initialized_ = false;
        initialized_ = false;
        stats_ = GenerationStats{};
    }

    bool isInitialized() const override {
        if (!initialized_)
            return false;
        if (fallback_initialized_ && fallback_provider_)
            return true;
        return daemon_client_ && daemon_client_->isConnected();
    }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        YAMS_ZONE_SCOPED_N("DaemonBackend::generateEmbedding");

        if (fallback_initialized_ && fallback_provider_) {
            auto start = std::chrono::high_resolution_clock::now();
            auto res = fallback_provider_->generateEmbedding(text);
            if (!res) {
                return Error{ErrorCode::InternalError, res.error().message};
            }
            auto end = std::chrono::high_resolution_clock::now();
            cached_dim_ = fallback_provider_->getEmbeddingDimension();
            cached_seq_len_ = fallback_provider_->getMaxSequenceLength();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            updateStats(1, text.length() / 4, duration);
            return res.value();
        }

        if (!isInitialized()) {
            return Error{ErrorCode::NotInitialized, "Daemon backend not connected"};
        }

        try {
            auto start = std::chrono::high_resolution_clock::now();

            daemon::GenerateEmbeddingRequest req;
            req.text = text;
            req.modelName = config_.model_name;
            req.normalize = config_.normalize_embeddings;

            auto result = await_with_timeout<yams::daemon::EmbeddingResponse>(
                [&]() { return daemon_client_->generateEmbedding(req); }, config_.daemon_timeout);
            if (!result) {
                return Error{ErrorCode::NetworkError, result.error().message};
            }

            const auto& response = result.value();

            // Update cached dimensions
            if (response.dimensions > 0) {
                cached_dim_ = response.dimensions;
            }

            // Update stats
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            updateStats(1, text.length() / 4, duration);

            return response.embedding;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Daemon embedding failed: ") + e.what()};
        }
    }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        YAMS_EMBEDDING_ZONE_BATCH(texts.size());

        if (fallback_initialized_ && fallback_provider_) {
            auto start = std::chrono::high_resolution_clock::now();
            std::vector<std::string> batch(texts.begin(), texts.end());
            auto res = fallback_provider_->generateBatchEmbeddings(batch);
            if (!res) {
                return Error{ErrorCode::InternalError, res.error().message};
            }
            auto end = std::chrono::high_resolution_clock::now();
            cached_dim_ = fallback_provider_->getEmbeddingDimension();
            cached_seq_len_ = fallback_provider_->getMaxSequenceLength();
            size_t total_chars = 0;
            for (const auto& text : texts) {
                total_chars += text.length();
            }
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            updateStats(texts.size(), total_chars / 4, duration);
            stats_.total_batches++;
            stats_.batch_count++;
            return res.value();
        }

        if (!isInitialized()) {
            return Error{ErrorCode::NotInitialized, "Daemon backend not connected"};
        }

        try {
            auto start = std::chrono::high_resolution_clock::now();

            daemon::BatchEmbeddingRequest req;
            req.texts.reserve(texts.size());
            for (const auto& t : texts) {
                req.texts.emplace_back(common::sanitizeUtf8(t));
            }
            req.modelName = config_.model_name;
            req.normalize = config_.normalize_embeddings;
            req.batchSize = config_.batch_size;

            // Heuristic: use streaming for larger batches to avoid idle socket timeouts
            bool useStreaming = texts.size() > 1; // stream whenever batch > 1
            // When streaming, reduce sub-batch size to emit progress more frequently
            if (useStreaming) {
                req.batchSize = std::max<size_t>(4, std::min<size_t>(req.batchSize, 8));
            }

            std::optional<yams::daemon::BatchEmbeddingResponse> maybeResponse;
            Error lastError{ErrorCode::InternalError, "uninitialized"};
            const int maxAttempts = 4;
            size_t attempt = 0;
            size_t currentBatchSize = req.batchSize;
            while (attempt < maxAttempts) {
                ++attempt;
                spdlog::debug("[Embedding][Daemon] Batch request attempt {}: size={} streaming={} "
                              "sub_batch={}",
                              attempt, texts.size(), useStreaming ? "true" : "false",
                              currentBatchSize);
                req.batchSize = currentBatchSize;
                yams::Result<yams::daemon::BatchEmbeddingResponse> result =
                    useStreaming
                        ? await_with_timeout<yams::daemon::BatchEmbeddingResponse>(
                              [&]() { return daemon_client_->streamingBatchEmbeddings(req); },
                              config_.daemon_timeout)
                        : await_with_timeout<yams::daemon::BatchEmbeddingResponse>(
                              [&]() { return daemon_client_->generateBatchEmbeddings(req); },
                              config_.daemon_timeout);
                if (result) {
                    maybeResponse = result.value();
                    break;
                }
                lastError = result.error();
                spdlog::warn(
                    "[Embedding][Daemon] Batch embeddings failed (code={}, msg='{}') on attempt {}",
                    static_cast<int>(lastError.code), lastError.message, attempt);
                // Retry on transient/network/timeout-like failures AND plugin/resource errors
                // InternalError (code=17): transient plugin failures (e.g. ONNX model busy)
                // ResourceExhausted: ONNX slot contention under concurrent load
                const bool canRetry = lastError.code == ErrorCode::Timeout ||
                                      lastError.code == ErrorCode::NetworkError ||
                                      lastError.code == ErrorCode::InvalidState ||
                                      lastError.code == ErrorCode::InternalError ||
                                      lastError.code == ErrorCode::ResourceExhausted;
                if (!canRetry)
                    break;
                // Exponential backoff: 100ms, 200ms, 400ms, 800ms
                using namespace std::chrono_literals;
                std::this_thread::sleep_for(std::chrono::milliseconds(100u << (attempt - 1)));
                // Reduce sub-batch size to mitigate long single-run stalls
                if (currentBatchSize > 4)
                    currentBatchSize = std::max<size_t>(4, currentBatchSize / 2);
                // Force streaming on retry to ensure progress events
                useStreaming = true;
            }

            if (!maybeResponse) {
                return Error{ErrorCode::NetworkError, std::move(lastError.message)};
            }

            const auto& response = *maybeResponse;

            // Update cached dimensions
            if (response.dimensions > 0) {
                cached_dim_ = response.dimensions;
            }

            // Update stats
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            size_t total_chars = 0;
            for (const auto& text : texts) {
                total_chars += text.length();
            }
            updateStats(texts.size(), total_chars / 4, duration);
            stats_.total_batches++;
            stats_.batch_count++; // Track number of batch operations

            return response.embeddings;
        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Daemon batch embedding failed: ") + e.what()};
        }
    }

    size_t getEmbeddingDimension() const override {
        if (cached_dim_ > 0)
            return cached_dim_;
        if (fallback_initialized_ && fallback_provider_) {
            return fallback_provider_->getEmbeddingDimension();
        }
        return config_.embedding_dim;
    }

    size_t getMaxSequenceLength() const override {
        if (cached_seq_len_ > 0)
            return cached_seq_len_;
        if (fallback_initialized_ && fallback_provider_) {
            return fallback_provider_->getMaxSequenceLength();
        }
        return config_.max_sequence_length;
    }

    std::string getBackendName() const override { return "Daemon"; }

    bool isAvailable() const override {
        if (fallback_initialized_ && fallback_provider_)
            return true;
        return daemon_client_ && daemon_client_->isConnected();
    }

    GenerationStats getStats() const override { return stats_; }
    void resetStats() override {
        stats_.total_texts_processed.store(0);
        stats_.total_tokens_processed.store(0);
        stats_.total_inference_time.store(0);
        stats_.avg_inference_time.store(0);
        stats_.batch_count.store(0);
        stats_.total_batches.store(0);
        stats_.throughput_texts_per_sec.store(0.0);
        stats_.throughput_tokens_per_sec.store(0.0);
    }

private:
    void updateStats(size_t texts, size_t tokens, std::chrono::milliseconds duration) {
        stats_.total_texts_processed.fetch_add(texts);
        stats_.total_tokens_processed.fetch_add(tokens);
        stats_.total_inference_time.fetch_add(duration.count());

        auto total_texts = stats_.total_texts_processed.load();
        if (total_texts > 0) {
            auto total_time = stats_.total_inference_time.load();
            stats_.avg_inference_time.store(total_time / total_texts);
        }
        stats_.updateThroughput();
    }

    EmbeddingConfig config_;
    std::shared_ptr<daemon::DaemonClient> daemon_client_;
    std::unique_ptr<yams::ml::IEmbeddingProvider> fallback_provider_;
    bool fallback_initialized_{false};
    bool initialized_;
    GenerationStats stats_;
    mutable size_t cached_dim_ = 0;
    mutable size_t cached_seq_len_ = 0;
};

class HybridBackend : public IEmbeddingBackend {
public:
    explicit HybridBackend(const EmbeddingConfig& config)
        : config_(config), daemon_backend_(std::make_unique<DaemonBackend>(config)),
          initialized_(false) {}

    bool initialize() override {
        YAMS_ZONE_SCOPED_N("HybridBackend::initialize");

        if (initialized_) {
            return true;
        }

        if (daemon_backend_->initialize()) {
            initialized_ = true;
            return true;
        }

        spdlog::error("HybridBackend: daemon unavailable (legacy alias of daemon backend)");
        return false;
    }

    void shutdown() override {
        daemon_backend_->shutdown();
        initialized_ = false;
        stats_ = GenerationStats{};
    }

    bool isInitialized() const override { return initialized_; }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        YAMS_ZONE_SCOPED_N("HybridBackend::generateEmbedding");

        if (!daemon_backend_->isAvailable()) {
            return Error{ErrorCode::NotSupported,
                         "Daemon not available - start with 'yams daemon start'"};
        }

        auto result = daemon_backend_->generateEmbedding(text);
        if (result) {
            daemon_uses_++;
            mergeStats(daemon_backend_->getStats());
        }
        return result;
    }

    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        YAMS_EMBEDDING_ZONE_BATCH(texts.size());

        if (!daemon_backend_->isAvailable()) {
            return Error{ErrorCode::NotSupported,
                         "Daemon not available - start with 'yams daemon start'"};
        }

        auto result = daemon_backend_->generateEmbeddings(texts);
        if (result) {
            daemon_uses_++;
            mergeStats(daemon_backend_->getStats());
        }
        return result;
    }

    size_t getEmbeddingDimension() const override {
        return daemon_backend_->getEmbeddingDimension();
    }

    size_t getMaxSequenceLength() const override { return daemon_backend_->getMaxSequenceLength(); }

    std::string getBackendName() const override {
        return "Daemon (uses: " + std::to_string(daemon_uses_) + ")";
    }

    bool isAvailable() const override { return daemon_backend_->isAvailable(); }

    GenerationStats getStats() const override { return stats_; }
    void resetStats() override {
        stats_.total_texts_processed.store(0);
        stats_.total_tokens_processed.store(0);
        stats_.total_inference_time.store(0);
        stats_.avg_inference_time.store(0);
        stats_.batch_count.store(0);
        stats_.total_batches.store(0);
        stats_.throughput_texts_per_sec.store(0.0);
        stats_.throughput_tokens_per_sec.store(0.0);
        daemon_uses_ = 0;
    }

private:
    void mergeStats(const GenerationStats& backend_stats) {
        stats_.total_texts_processed.store(backend_stats.total_texts_processed.load());
        stats_.total_tokens_processed.store(backend_stats.total_tokens_processed.load());
        stats_.total_inference_time.store(backend_stats.total_inference_time.load());
        stats_.avg_inference_time.store(backend_stats.avg_inference_time.load());
        stats_.batch_count.store(backend_stats.batch_count.load());
        stats_.total_batches.store(backend_stats.total_batches.load());
        stats_.updateThroughput();
    }

    EmbeddingConfig config_;
    std::unique_ptr<DaemonBackend> daemon_backend_;
    bool initialized_;
    GenerationStats stats_;
    mutable size_t daemon_uses_ = 0;
};

// =============================================================================
// EmbeddingGenerator Implementation with Variant Backend
// =============================================================================

class EmbeddingGenerator::Impl {
public:
    explicit Impl(const EmbeddingConfig& config) : config_(config) {
        auto effective = config.backend;
        if (const char* env = std::getenv("YAMS_EMBED_BACKEND"); env && *env) {
            std::string s(env);
            for (auto& c : s)
                c = static_cast<char>(std::tolower(c));
            if (s == "simeon") {
                effective = EmbeddingConfig::Backend::Simeon;
                config_.backend = effective;
            } else if (s == "daemon") {
                effective = EmbeddingConfig::Backend::Daemon;
                config_.backend = effective;
            }
        }
        switch (effective) {
            case EmbeddingConfig::Backend::Simeon:
                backend_ = makeSimeonBackend(config_);
                break;
            case EmbeddingConfig::Backend::Daemon:
            case EmbeddingConfig::Backend::Hybrid:
            default:
                backend_ = std::make_unique<DaemonBackend>(config_);
                break;
        }
    }

    /**
     * Constructor with pre-configured backend injection
     * Allows using a custom IEmbeddingBackend instead of auto-selecting based on config.
     */
    Impl(std::unique_ptr<IEmbeddingBackend> backend, const EmbeddingConfig& config)
        : config_(config), backend_(std::move(backend)) {}

    bool initialize() {
        YAMS_ZONE_SCOPED_N("EmbeddingGenerator::initialize");

        // Check if already initialized (lock-free fast path)
        if (initialized_.load()) {
            return true;
        }

        try {
            // Initialize concurrency cap once per process (TuneAdvisor-aware)
            ConcurrencyGuard::init_from_env_once();
            if (!backend_) {
                setError("No backend configured");
                return false;
            }

            bool init_result = backend_->initialize();
            if (!init_result) {
                setError("Backend initialization failed: " + backend_->getBackendName());
                return false;
            }

            // Atomically update initialization state
            initialized_.store(true);
            has_error_.store(false);
            spdlog::info("EmbeddingGenerator initialized with backend: {}",
                         backend_->getBackendName());
            return true;

        } catch (const std::exception& e) {
            setError("Initialization failed: " + std::string(e.what()));
            return false;
        }
    }

    bool isInitialized() const {
        return initialized_.load() && backend_ && backend_->isInitialized();
    }

    void shutdown() {
        if (backend_) {
            backend_->shutdown();
        }
        initialized_.store(false);
        has_error_.store(false);
        {
            std::unique_lock<std::shared_mutex> lock(error_mutex_);
            last_error_.clear();
        }
    }

    std::vector<float> generateEmbedding(const std::string& text) {
        YAMS_ZONE_SCOPED_N("EmbeddingGenerator::generateEmbedding");
        YAMS_PLOT("embedding::input_chars", static_cast<int64_t>(text.size()));
        ConcurrencyGuard _gate; // limit concurrent embedding jobs

        // Lock-free check for initialization
        if (!initialized_.load() || !backend_) {
            setError("Generator not initialized");
            return {};
        }

        try {
            auto result = backend_->generateEmbedding(text);
            if (!result) {
                setError(result.error().message);
                return {};
            }

            has_error_.store(false);
            YAMS_PLOT("embedding::output_dim", static_cast<int64_t>(result.value().size()));
            return result.value();

        } catch (const std::exception& e) {
            setError("Embedding generation failed: " + std::string(e.what()));
            return {};
        }
    }

    std::vector<std::vector<float>> generateEmbeddings(const std::vector<std::string>& texts) {
        YAMS_EMBEDDING_ZONE_BATCH(texts.size());
        YAMS_PLOT("embedding::batch_size", static_cast<int64_t>(texts.size()));
        ConcurrencyGuard _gate; // limit concurrent embedding jobs

        if (texts.empty()) {
            return {};
        }

        // Lock-free check for initialization
        if (!initialized_.load() || !backend_) {
            setError("Generator not initialized");
            return {};
        }

        try {
            // Convert to span
            std::span<const std::string> texts_span(texts);
            auto result = backend_->generateEmbeddings(texts_span);

            if (!result) {
                setError(result.error().message);
                return {};
            }

            has_error_.store(false);
            YAMS_PLOT("embedding::batch_outputs", static_cast<int64_t>(result.value().size()));
            if (!result.value().empty()) {
                YAMS_PLOT("embedding::batch_output_dim",
                          static_cast<int64_t>(result.value().front().size()));
            }
            return result.value();

        } catch (const std::exception& e) {
            setError("Batch embedding generation failed: " + std::string(e.what()));
            return {};
        }
    }

    size_t estimateTokenCount(const std::string& text) const {
        // Simple estimation: ~4 characters per token on average
        return std::max(static_cast<std::size_t>(1), text.length() / 4);
    }

    // Getters and configuration
    size_t getEmbeddingDimension() const {
        return backend_ ? backend_->getEmbeddingDimension() : config_.embedding_dim;
    }

    size_t getMaxSequenceLength() const {
        return backend_ ? backend_->getMaxSequenceLength() : config_.max_sequence_length;
    }

    const EmbeddingConfig& getConfig() const { return config_; }

    std::string getBackendName() const {
        try {
            return backend_ ? backend_->getBackendName() : std::string{"unknown"};
        } catch (...) {
            return "unknown";
        }
    }

    GenerationStats getStats() const {
        if (!backend_) {
            return GenerationStats{};
        }

        try {
            return backend_->getStats();
        } catch (const std::exception& e) {
            spdlog::warn("getStats: Backend getStats failed: {}", e.what());
            return GenerationStats{};
        }
    }

    void resetStats() {
        if (backend_) {
            try {
                backend_->resetStats();
            } catch (const std::exception& e) {
                spdlog::warn("resetStats: Backend reset failed: {}", e.what());
            }
        }
    }

    std::string getLastError() const {
        std::shared_lock<std::shared_mutex> lock(error_mutex_);
        return last_error_;
    }

    bool hasError() const { return has_error_.load(); }

private:
    // Global simple concurrency gate to prevent unbounded CPU oversubscription when plugins
    // implement embedding backends. Limits concurrent generate* calls across the process.
    struct ConcurrencyGuard final {
        ConcurrencyGuard() { lock(); }
        ~ConcurrencyGuard() { unlock(); }
        static int resolve_cap() {
            int cap = 0;
            try {
                cap = static_cast<int>(yams::daemon::TuneAdvisor::getEmbedMaxConcurrency());
            } catch (...) {
            }
            if (cap <= 0) {
                cap = std::max(1u, std::thread::hardware_concurrency());
            }
            return std::max(1, cap);
        }
        static void init_from_env_once() {
            static std::once_flag once;
            std::call_once(once, []() {
                const int cap = resolve_cap();
                g_max_concurrency_.store(cap, std::memory_order_relaxed);
                spdlog::info("EmbeddingGenerator: max concurrency set to {}", cap);
            });
        }
        static int refresh_cap_locked() {
            const int desired = resolve_cap();
            const int current = g_max_concurrency_.load(std::memory_order_relaxed);
            if (desired != current) {
                g_max_concurrency_.store(desired, std::memory_order_relaxed);
                g_cv_.notify_all();
                return desired;
            }
            return current;
        }
        static void lock() {
            std::unique_lock<std::mutex> lk(g_mtx_);
            int cap = refresh_cap_locked();
            while (g_active_ >= cap) {
                g_cv_.wait(lk);
                cap = refresh_cap_locked();
            }
            ++g_active_;
        }
        static void unlock() {
            std::lock_guard<std::mutex> lk(g_mtx_);
            if (g_active_ > 0) {
                --g_active_;
            }
            g_cv_.notify_one();
        }
        static std::mutex g_mtx_;
        static std::condition_variable g_cv_;
        static std::atomic<int> g_max_concurrency_;
        static int g_active_;
    };

    void setError(const std::string& error) const {
        {
            std::unique_lock<std::shared_mutex> lock(error_mutex_);
            last_error_ = error;
        }
        has_error_.store(true);
    }

    EmbeddingConfig config_;
    std::unique_ptr<IEmbeddingBackend> backend_;
    std::atomic<bool> initialized_{false};
    mutable std::shared_mutex error_mutex_; // Reader-writer lock for error strings only
    mutable std::string last_error_;
    mutable std::atomic<bool> has_error_{false};
};

// Static members for ConcurrencyGuard
std::mutex EmbeddingGenerator::Impl::ConcurrencyGuard::g_mtx_;
std::condition_variable EmbeddingGenerator::Impl::ConcurrencyGuard::g_cv_;
std::atomic<int> EmbeddingGenerator::Impl::ConcurrencyGuard::g_max_concurrency_{2};
int EmbeddingGenerator::Impl::ConcurrencyGuard::g_active_{0};

EmbeddingGenerator::EmbeddingGenerator(const EmbeddingConfig& config)
    : pImpl(std::make_unique<Impl>(config)) {}

EmbeddingGenerator::EmbeddingGenerator(std::unique_ptr<IEmbeddingBackend> backend,
                                       const EmbeddingConfig& config)
    : pImpl(std::make_unique<Impl>(std::move(backend), config)) {}

EmbeddingGenerator::~EmbeddingGenerator() = default;
EmbeddingGenerator::EmbeddingGenerator(EmbeddingGenerator&&) noexcept = default;
EmbeddingGenerator& EmbeddingGenerator::operator=(EmbeddingGenerator&&) noexcept = default;

bool EmbeddingGenerator::initialize() {
    return pImpl ? pImpl->initialize() : false;
}

bool EmbeddingGenerator::isInitialized() const {
    return pImpl && pImpl->isInitialized();
}

void EmbeddingGenerator::shutdown() {
    if (pImpl) {
        pImpl->shutdown();
    }
}

std::vector<float> EmbeddingGenerator::generateEmbedding(const std::string& text) {
    YAMS_ZONE_SCOPED_N("EmbeddingGenerator::generateEmbedding");
    return pImpl ? pImpl->generateEmbedding(text) : std::vector<float>();
}

std::vector<std::vector<float>>
EmbeddingGenerator::generateEmbeddings(const std::vector<std::string>& texts) {
    YAMS_ZONE_SCOPED_N("EmbeddingGenerator::generateEmbeddings");
    return pImpl ? pImpl->generateEmbeddings(texts) : std::vector<std::vector<float>>();
}

std::future<std::vector<float>>
EmbeddingGenerator::generateEmbeddingAsync(const std::string& text) {
    return std::async(std::launch::async, [this, text]() { return generateEmbedding(text); });
}

std::future<std::vector<std::vector<float>>>
EmbeddingGenerator::generateEmbeddingsAsync(const std::vector<std::string>& texts) {
    auto promise = std::make_shared<std::promise<std::vector<std::vector<float>>>>();
    std::future<std::vector<std::vector<float>>> future = promise->get_future();

    std::thread([this, texts, promise]() {
        try {
            promise->set_value(generateEmbeddings(texts));
        } catch (...) {
            try {
                promise->set_exception(std::current_exception());
            } catch (...) {
                // Ignore errors setting exception
            }
        }
    }).detach();

    return future;
}

bool EmbeddingGenerator::loadModel([[maybe_unused]] const std::string& model_path) {
    // Update config and reinitialize
    // This is a simplified implementation
    return initialize();
}

bool EmbeddingGenerator::switchModel([[maybe_unused]] const std::string& model_name,
                                     const EmbeddingConfig& new_config) {
    shutdown();
    pImpl = std::make_unique<Impl>(new_config);
    return initialize();
}

bool EmbeddingGenerator::isModelLoaded() const {
    return isInitialized();
}

void EmbeddingGenerator::unloadModel() {
    shutdown();
}

size_t EmbeddingGenerator::getEmbeddingDimension() const {
    return pImpl ? pImpl->getEmbeddingDimension() : 0;
}

size_t EmbeddingGenerator::getMaxSequenceLength() const {
    return pImpl ? pImpl->getMaxSequenceLength() : 0;
}

const EmbeddingConfig& EmbeddingGenerator::getConfig() const {
    static const EmbeddingConfig empty_config{};
    return pImpl ? pImpl->getConfig() : empty_config;
}

std::string EmbeddingGenerator::getBackendName() const {
    try {
        return pImpl ? pImpl->getBackendName() : std::string{"unknown"};
    } catch (...) {
        return "unknown";
    }
}

void EmbeddingGenerator::updateConfig(const EmbeddingConfig& new_config) {
    shutdown();
    pImpl = std::make_unique<Impl>(new_config);
}

GenerationStats EmbeddingGenerator::getStats() const {
    return pImpl ? pImpl->getStats() : GenerationStats{};
}

void EmbeddingGenerator::resetStats() {
    if (pImpl) {
        pImpl->resetStats();
    }
}

bool EmbeddingGenerator::validateText(const std::string& text) const {
    return !text.empty() && text.length() < 1000000; // 1MB limit
}

size_t EmbeddingGenerator::estimateTokenCount(const std::string& text) const {
    return pImpl->estimateTokenCount(text);
}

std::string EmbeddingGenerator::getModelInfo() const {
    std::ostringstream info;
    info << "Model: " << getConfig().model_name << "\n";
    info << "Embedding Dimension: " << getEmbeddingDimension() << "\n";
    info << "Max Sequence Length: " << getMaxSequenceLength() << "\n";
    info << "Initialized: " << (isInitialized() ? "Yes" : "No") << "\n";
    return info.str();
}

std::string EmbeddingGenerator::getLastError() const {
    return pImpl->getLastError();
}

bool EmbeddingGenerator::hasError() const {
    return pImpl->hasError();
}

// Factory function
std::unique_ptr<EmbeddingGenerator> createEmbeddingGenerator(const EmbeddingConfig& config) {
    auto generator = std::make_unique<EmbeddingGenerator>(config);
    if (!generator->initialize()) {
        return nullptr;
    }
    return generator;
}

// =============================================================================
// Utility Functions
// =============================================================================

namespace embedding_utils {

std::vector<float> normalizeEmbedding(const std::vector<float>& embedding) {
    double norm = 0.0;
    for (float val : embedding) {
        norm += static_cast<double>(val) * static_cast<double>(val);
    }
    norm = std::sqrt(norm);

    if (norm == 0.0) {
        return embedding;
    }

    std::vector<float> normalized;
    normalized.reserve(embedding.size());
    for (float val : embedding) {
        normalized.push_back(val / static_cast<float>(norm));
    }

    return normalized;
}

std::vector<std::vector<float>>
normalizeEmbeddings(const std::vector<std::vector<float>>& embeddings) {
    std::vector<std::vector<float>> normalized;
    normalized.reserve(embeddings.size());

    for (const auto& embedding : embeddings) {
        normalized.push_back(normalizeEmbedding(embedding));
    }

    return normalized;
}

double computeMagnitude(const std::vector<float>& embedding) {
    double magnitude = 0.0;
    for (float val : embedding) {
        magnitude += static_cast<double>(val) * static_cast<double>(val);
    }
    return std::sqrt(magnitude);
}

bool validateEmbedding(const std::vector<float>& embedding, size_t expected_dim) {
    if (embedding.size() != expected_dim) {
        return false;
    }

    for (float val : embedding) {
        if (!std::isfinite(val)) {
            return false;
        }
    }

    return true;
}

std::string embeddingToString(const std::vector<float>& embedding, size_t max_values) {
    std::ostringstream oss;
    oss << "[";

    size_t count = std::min(max_values, embedding.size());
    for (size_t i = 0; i < count; ++i) {
        if (i > 0)
            oss << ", ";
        oss << std::fixed << std::setprecision(4) << embedding[i];
    }

    if (embedding.size() > max_values) {
        oss << ", ... (" << embedding.size() << " total)";
    }

    oss << "]";
    return oss.str();
}

EmbeddingConfig loadConfigFromFile([[maybe_unused]] const std::string& config_path) {
    // TODO: Implement JSON loading
    // For now, return default config
    return EmbeddingConfig{};
}

bool saveConfigToFile([[maybe_unused]] const EmbeddingConfig& config,
                      [[maybe_unused]] const std::string& config_path) {
    // TODO: Implement JSON saving
    return false;
}

std::vector<std::string> getAvailableModels(const std::string& models_dir) {
    std::vector<std::string> models;

    if (std::filesystem::exists(models_dir) && std::filesystem::is_directory(models_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(models_dir)) {
            if (entry.is_directory()) {
                models.push_back(entry.path().filename().string());
            }
        }
    }

    return models;
}

bool downloadModel([[maybe_unused]] const std::string& model_name,
                   [[maybe_unused]] const std::string& target_dir) {
    // TODO: Implement model downloading
    return false;
}

} // namespace embedding_utils

} // namespace yams::vector
