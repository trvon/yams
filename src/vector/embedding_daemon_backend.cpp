// Private implementation TU: DaemonBackend is the daemon IPC embedding transport.
// Split out of embedding_generator.cpp (Brick audit lead 1) so the generator TU
// no longer depends on daemon client/IPC headers; EmbeddingGenerator consumes
// this backend only through the IEmbeddingBackend seam and the factory below.
#include <yams/vector/embedding_generator.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <spdlog/spdlog.h>

#include <yams/common/utf8_utils.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/ml/provider.h>
#include <yams/profiling.h>

namespace yams::vector {

class DaemonBackend;

/**
 * DaemonBackend - Daemon IPC backend
 * Communicates with the daemon service for embedding generation
 */
// Local awaitable bridge (build-only) to await daemon calls without legacy async_bridge.
// Callers must ensure the supplied awaitable factory owns any state it touches because the
// spawned coroutine may outlive the timeout return path.
template <typename T, typename MakeAwaitable>
static yams::Result<T> await_with_timeout(
    MakeAwaitable&& make, std::chrono::milliseconds timeout,
    const std::optional<boost::asio::any_io_executor>& executorOverride = std::nullopt) {
    auto shared_promise = std::make_shared<std::promise<yams::Result<T>>>();
    auto fut = shared_promise->get_future();
    auto completed = std::make_shared<std::atomic<bool>>(false);

    boost::asio::any_io_executor executor =
        executorOverride ? *executorOverride : yams::daemon::GlobalIOContext::global_executor();
    boost::asio::co_spawn(
        executor,
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
                    spdlog::debug("await_with_timeout: promise already satisfied during timeout");
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
            if (yams::config::read_env_bool("YAMS_IN_DAEMON").valueOr(false)) {
                // In synthetic/unit tests we intentionally force a deterministic mock provider
                // while running in-daemon mode.
                bool useMockProvider = false;
#if defined(YAMS_TESTING) || defined(YAMS_TEST_PROVIDER_CONTROLS)
                useMockProvider =
                    yams::config::read_env_bool("YAMS_USE_MOCK_PROVIDER").valueOr(false);
#endif
                fallback_provider_ = useMockProvider ? yams::ml::createEmbeddingProvider("Mock")
                                                     : yams::ml::createEmbeddingProvider();
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
            auto daemonClient = daemon_client_;
            auto st = await_with_timeout<yams::daemon::StatusResponse>(
                [daemonClient]() { return daemonClient->status(); }, std::chrono::seconds(5),
                config_.executorOverride);
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
                        spdlog::debug("DaemonBackend: failed to inspect loaded daemon models");
                    }

                    // Allow extended preload timeout via env (default 30s)
                    std::chrono::milliseconds preload_timeout = std::chrono::seconds(30);
                    if (auto configured =
                            yams::config::read_env_milliseconds("YAMS_MODEL_PRELOAD_TIMEOUT_MS")
                                .value;
                        configured && configured->count() > 0) {
                        preload_timeout = *configured;
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
                            [daemonClient, req]() { return daemonClient->loadModel(req); },
                            preload_timeout, config_.executorOverride);
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
                spdlog::debug("DaemonBackend: fallback provider shutdown raised exception");
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

            auto daemonClient = daemon_client_;
            auto result = await_with_timeout<yams::daemon::EmbeddingResponse>(
                [daemonClient, req]() { return daemonClient->generateEmbedding(req); },
                config_.daemon_timeout, config_.executorOverride);
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
                auto daemonClient = daemon_client_;
                yams::Result<yams::daemon::BatchEmbeddingResponse> result =
                    useStreaming ? await_with_timeout<yams::daemon::BatchEmbeddingResponse>(
                                       [daemonClient, req]() {
                                           return daemonClient->streamingBatchEmbeddings(req);
                                       },
                                       config_.daemon_timeout, config_.executorOverride)
                                 : await_with_timeout<yams::daemon::BatchEmbeddingResponse>(
                                       [daemonClient, req]() {
                                           return daemonClient->generateBatchEmbeddings(req);
                                       },
                                       config_.daemon_timeout, config_.executorOverride);
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

std::unique_ptr<IEmbeddingBackend> createDaemonBackend(const EmbeddingConfig& config) {
    return std::make_unique<DaemonBackend>(config);
}

} // namespace yams::vector
