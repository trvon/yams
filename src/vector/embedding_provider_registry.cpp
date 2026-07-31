#include <spdlog/spdlog.h>
#include <map>
#include <mutex>
#include <optional>
#include <yams/daemon/client/await_result_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/ml/provider.h>

namespace yams::ml {

std::unique_ptr<IEmbeddingProvider> createMockEmbeddingProvider();

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

            client_ = std::make_shared<daemon::DaemonClient>(cfg);

            auto client = client_;
            auto statusResult = daemon::detail::awaitResultSync<daemon::StatusResponse>(
                daemon::GlobalIOContext::global_executor(),
                [client = std::move(client)]() { return client->status(); },
                std::chrono::seconds(5), "Embedding provider status");

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

        auto client = client_;
        auto result = daemon::detail::awaitResultSync<daemon::EmbeddingResponse>(
            daemon::GlobalIOContext::global_executor(),
            [client = std::move(client), req = std::move(req)]() {
                return client->generateEmbedding(req);
            },
            requestTimeout_, "Embedding generation");

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
        auto client = client_;
        auto result = daemon::detail::awaitResultSync<daemon::BatchEmbeddingResponse>(
            daemon::GlobalIOContext::global_executor(),
            [client = std::move(client), req = std::move(req)]() {
                return client->streamingBatchEmbeddings(req);
            },
            batchTimeout_, "Batch embedding generation");

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
    std::shared_ptr<daemon::DaemonClient> client_;
    bool initialized_;
    mutable size_t cachedDim_ = 0;
    mutable size_t cachedSeqLen_ = 0;
    std::chrono::milliseconds requestTimeout_{30000};
    std::chrono::milliseconds batchTimeout_{300000};
};

static std::map<std::string, EmbeddingProviderFactory> g_embeddingProviders;
static std::mutex g_embeddingProvidersMutex;
static std::once_flag g_defaultProvidersOnce;

void registerEmbeddingProvider(const std::string& name, EmbeddingProviderFactory factory) {
    std::lock_guard lock(g_embeddingProvidersMutex);
    g_embeddingProviders[name] = std::move(factory);
}

std::vector<std::string> getRegisteredEmbeddingProviders() {
    std::lock_guard lock(g_embeddingProvidersMutex);
    std::vector<std::string> names;
    for (const auto& [name, _] : g_embeddingProviders) {
        names.push_back(name);
    }
    return names;
}

namespace {
void ensureDefaultEmbeddingProvidersRegistered() {
    std::call_once(g_defaultProvidersOnce, [] {
        registerEmbeddingProvider("Mock", &createMockEmbeddingProvider);
        registerEmbeddingProvider("ONNX", []() -> std::unique_ptr<IEmbeddingProvider> {
            return std::make_unique<DaemonClientEmbeddingProvider>();
        });
        registerEmbeddingProvider("DaemonClient", []() -> std::unique_ptr<IEmbeddingProvider> {
            return std::make_unique<DaemonClientEmbeddingProvider>();
        });
    });
}

std::optional<EmbeddingProviderFactory> findEmbeddingProviderFactory(const std::string& name) {
    std::lock_guard lock(g_embeddingProvidersMutex);
    auto it = g_embeddingProviders.find(name);
    if (it == g_embeddingProviders.end()) {
        return std::nullopt;
    }
    return it->second;
}
} // namespace

std::unique_ptr<IEmbeddingProvider> createEmbeddingProvider(const std::string& preferredProvider) {
    ensureDefaultEmbeddingProvidersRegistered();

    if (!preferredProvider.empty()) {
        auto factory = findEmbeddingProviderFactory(preferredProvider);
        if (factory) {
            return (*factory)();
        }
        spdlog::warn("Preferred embedding provider '{}' not found", preferredProvider);
    }

    if (auto factory = findEmbeddingProviderFactory("ONNX")) {
        auto provider = (*factory)();
        if (provider && provider->isAvailable()) {
            return provider;
        }
    }

    spdlog::error("No embedding providers available");
    return nullptr;
}

} // namespace yams::ml
