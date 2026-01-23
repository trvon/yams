#include <spdlog/spdlog.h>
#include <future>
#include <map>
#include <numeric>
#include <random>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/ml/provider.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

namespace yams::ml {

template <typename T, typename MakeAwaitable>
static Result<T> awaitDaemonCall(MakeAwaitable&& make, std::chrono::milliseconds timeout) {
    if (yams::daemon::GlobalIOContext::is_destroyed()) {
        return Error{ErrorCode::SystemShutdown, "IO context destroyed during shutdown"};
    }

    auto shared_promise = std::make_shared<std::promise<Result<T>>>();
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
                    state->set_value(
                        Error{ErrorCode::Unknown, std::string("await exception: ") + ex.what()});
                }
            } catch (...) {
                if (!completed->exchange(true)) {
                    state->set_value(Error{ErrorCode::Unknown, "await exception"});
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
                    shared_promise->set_value(Error{ErrorCode::Timeout, "daemon call timeout"});
                } catch (...) {
                }
            }
            return Error{ErrorCode::Timeout, "daemon call timeout"};
        }
    } else {
        fut.wait();
    }

    return fut.get();
}

class MockEmbeddingProvider : public IEmbeddingProvider {
public:
    explicit MockEmbeddingProvider(size_t dimension = 384)
        : dimension_(dimension), initialized_(false) {
        spdlog::debug("MockEmbeddingProvider created with dimension {}", dimension);
    }

    ~MockEmbeddingProvider() override {
        if (initialized_) {
            shutdown();
        }
    }

    Result<void> initialize() override {
        if (initialized_) {
            return Result<void>();
        }

        spdlog::info("Initializing MockEmbeddingProvider");
        initialized_ = true;
        return Result<void>();
    }

    void shutdown() override {
        if (!initialized_) {
            return;
        }

        spdlog::info("Shutting down MockEmbeddingProvider");
        initialized_ = false;
    }

    Result<std::vector<float>>
    generateEmbedding([[maybe_unused]] const std::string& text) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Mock provider not initialized"};
        }

        // Generate deterministic embedding based on text hash
        std::hash<std::string> hasher;
        const std::size_t seed = hasher(text);
        std::mt19937 gen(static_cast<std::mt19937::result_type>(seed));
        std::normal_distribution<float> dist(0.0f, 1.0f);

        std::vector<float> embedding(dimension_);
        for (size_t i = 0; i < dimension_; ++i) {
            embedding[i] = dist(gen);
        }

        // Normalize to unit length
        float norm = 0.0f;
        for (float val : embedding) {
            norm += val * val;
        }
        norm = std::sqrt(norm);

        if (norm > 0) {
            for (float& val : embedding) {
                val /= norm;
            }
        }

        return embedding;
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings([[maybe_unused]] const std::vector<std::string>& texts) override {
        if (!initialized_) {
            return Error{ErrorCode::NotInitialized, "Mock provider not initialized"};
        }

        std::vector<std::vector<float>> embeddings;
        embeddings.reserve(texts.size());

        for (const auto& text : texts) {
            auto result = generateEmbedding(text);
            if (!result) {
                return result.error();
            }
            embeddings.push_back(std::move(result.value()));
        }

        return embeddings;
    }

    bool isAvailable() const override {
        return true; // Mock is always available
    }

    std::string getProviderName() const override { return "Mock"; }

    size_t getEmbeddingDimension() const override { return dimension_; }

    size_t getMaxSequenceLength() const override {
        return 512; // Mock limit
    }

private:
    size_t dimension_;
    bool initialized_;
};

class DaemonClientEmbeddingProvider : public IEmbeddingProvider {
public:
    DaemonClientEmbeddingProvider() : initialized_(false) {
        spdlog::debug("DaemonClientEmbeddingProvider created");
    }

    ~DaemonClientEmbeddingProvider() override {
        if (initialized_) {
            shutdown();
        }
    }

    Result<void> initialize() override {
        if (initialized_) {
            return Result<void>();
        }

        try {
            if (!daemon::DaemonClient::isDaemonRunning()) {
                return Error{ErrorCode::NotSupported,
                             "Daemon not running - start with 'yams daemon start'"};
            }

            daemon::ClientConfig cfg;
            cfg.autoStart = false;
            cfg.requestTimeout = requestTimeout_;
            cfg.maxRetries = 3;

            client_ = std::make_unique<daemon::DaemonClient>(cfg);

            auto statusResult = awaitDaemonCall<daemon::StatusResponse>(
                [this]() { return client_->status(); }, std::chrono::seconds(5));

            if (statusResult) {
                const auto& status = statusResult.value();
                if (status.embeddingDim > 0) {
                    cachedDim_ = status.embeddingDim;
                }
            }

            initialized_ = true;
            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::InternalError,
                         std::string("Failed to initialize daemon client: ") + e.what()};
        }
    }

    void shutdown() override {
        if (!initialized_) {
            return;
        }

        if (client_) {
            client_->disconnect();
            client_.reset();
        }
        initialized_ = false;
        spdlog::debug("DaemonClientEmbeddingProvider shutdown");
    }

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        if (!initialized_ || !client_) {
            return Error{ErrorCode::NotInitialized,
                         "Daemon client not initialized - is daemon running?"};
        }

        daemon::GenerateEmbeddingRequest req;
        req.text = text;
        req.normalize = true;

        auto result = awaitDaemonCall<daemon::EmbeddingResponse>(
            [this, &req]() { return client_->generateEmbedding(req); }, requestTimeout_);

        if (!result) {
            return Error{result.error().code, result.error().message};
        }

        const auto& response = result.value();
        if (response.dimensions > 0) {
            cachedDim_ = response.dimensions;
        }

        return response.embedding;
    }

    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        if (!initialized_ || !client_) {
            return Error{ErrorCode::NotInitialized,
                         "Daemon client not initialized - is daemon running?"};
        }

        if (texts.empty()) {
            return std::vector<std::vector<float>>{};
        }

        daemon::BatchEmbeddingRequest req;
        req.texts = texts;
        req.normalize = true;
        req.batchSize = 8; // Reasonable sub-batch for streaming

        // Use streaming batch embeddings for better progress tracking
        auto result = awaitDaemonCall<daemon::BatchEmbeddingResponse>(
            [this, &req]() { return client_->streamingBatchEmbeddings(req); }, batchTimeout_);

        if (!result) {
            return Error{result.error().code, result.error().message};
        }

        const auto& response = result.value();
        if (response.dimensions > 0) {
            cachedDim_ = response.dimensions;
        }

        return response.embeddings;
    }

    bool isAvailable() const override {
        // Check if daemon is running (static check, no connection needed)
        return daemon::DaemonClient::isDaemonRunning();
    }

    std::string getProviderName() const override { return "ONNX"; }

    size_t getEmbeddingDimension() const override {
        return cachedDim_ > 0 ? cachedDim_ : 384; // Default to 384 (MiniLM)
    }

    size_t getMaxSequenceLength() const override { return cachedSeqLen_ > 0 ? cachedSeqLen_ : 512; }

private:
    std::unique_ptr<daemon::DaemonClient> client_;
    bool initialized_;
    mutable size_t cachedDim_ = 0;
    mutable size_t cachedSeqLen_ = 0;
    std::chrono::milliseconds requestTimeout_{30000};
    std::chrono::milliseconds batchTimeout_{300000};
};

static std::map<std::string, EmbeddingProviderFactory> g_embeddingProviders;

void registerEmbeddingProvider(const std::string& name, EmbeddingProviderFactory factory) {
    g_embeddingProviders[name] = factory;
}

std::vector<std::string> getRegisteredEmbeddingProviders() {
    std::vector<std::string> names;
    for (const auto& [name, _] : g_embeddingProviders) {
        names.push_back(name);
    }
    return names;
}

std::unique_ptr<IEmbeddingProvider> createEmbeddingProvider(const std::string& preferredProvider) {
    static bool initialized = false;
    if (!initialized) {
        registerEmbeddingProvider("Mock", []() -> std::unique_ptr<IEmbeddingProvider> {
            return std::make_unique<MockEmbeddingProvider>();
        });
        registerEmbeddingProvider("ONNX", []() -> std::unique_ptr<IEmbeddingProvider> {
            return std::make_unique<DaemonClientEmbeddingProvider>();
        });
        registerEmbeddingProvider("DaemonClient", []() -> std::unique_ptr<IEmbeddingProvider> {
            return std::make_unique<DaemonClientEmbeddingProvider>();
        });
        initialized = true;
    }

    if (!preferredProvider.empty()) {
        auto it = g_embeddingProviders.find(preferredProvider);
        if (it != g_embeddingProviders.end()) {
            return it->second();
        }
        spdlog::warn("Preferred embedding provider '{}' not found", preferredProvider);
    }

    if (auto it = g_embeddingProviders.find("ONNX"); it != g_embeddingProviders.end()) {
        auto provider = it->second();
        if (provider && provider->isAvailable()) {
            return provider;
        }
    }

    if (auto it = g_embeddingProviders.find("DaemonClient"); it != g_embeddingProviders.end()) {
        auto provider = it->second();
        if (provider && provider->isAvailable()) {
            return provider;
        }
    }

    if (auto it = g_embeddingProviders.find("Mock"); it != g_embeddingProviders.end()) {
        return it->second();
    }

    spdlog::error("No embedding providers available");
    return nullptr;
}

} // namespace yams::ml
