#define _CRT_SECURE_NO_WARNINGS
#include <spdlog/spdlog.h>
#include <filesystem>
#include <future>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/common/utf8_utils.h>
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
#include <shared_mutex>
#include <sstream>
#include <string_view>
#include <thread>
#include <yams/daemon/components/TuneAdvisor.h>

#include <yams/vector/simeon_embedding_backend.h>

namespace yams::vector {

// Daemon IPC backend defined in embedding_daemon_backend.cpp; the generator
// depends only on this factory and the IEmbeddingBackend seam.
std::unique_ptr<IEmbeddingBackend> createDaemonBackend(const EmbeddingConfig& config);

// Forward declarations

// =============================================================================
// EmbeddingGenerator Implementation with Variant Backend
// =============================================================================

class EmbeddingGenerator::Impl {
public:
    explicit Impl(const EmbeddingConfig& config) : config_(config) {
        // Backend policy belongs to the caller's resolved lifecycle snapshot. Keep this vector
        // layer deterministic and independent of daemon configuration or ambient environment.
        switch (config_.backend) {
            case EmbeddingConfig::Backend::Simeon:
                backend_ = makeSimeonBackend(config_);
                break;
            case EmbeddingConfig::Backend::OnnxRuntime:
            case EmbeddingConfig::Backend::Daemon:
            case EmbeddingConfig::Backend::Hybrid:
            default:
                backend_ = createDaemonBackend(config_);
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
            spdlog::debug("EmbeddingGenerator::Impl::getBackendName failed");
            return "unknown";
        }
    }

    std::string getEmbeddingSpaceIdentity() const {
        try {
            return backend_ ? backend_->getEmbeddingSpaceIdentity() : std::string{};
        } catch (...) {
            spdlog::debug("EmbeddingGenerator::Impl::getEmbeddingSpaceIdentity failed");
            return {};
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
    // TuneAdvisor intentionally remains the process-wide source: an instance-local snapshot
    // cannot coordinate admission across generators and could oversubscribe the host.
    struct ConcurrencyGuard final {
        ConcurrencyGuard() { lock(); }
        ~ConcurrencyGuard() { unlock(); }
        static int resolve_cap() {
            int cap = 0;
            try {
                cap = static_cast<int>(yams::daemon::TuneAdvisor::getEmbedMaxConcurrency());
            } catch (...) {
                spdlog::debug("EmbeddingGenerator: failed to read TuneAdvisor concurrency cap");
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
        spdlog::debug("EmbeddingGenerator::getBackendName failed");
        return "unknown";
    }
}

std::string EmbeddingGenerator::getEmbeddingSpaceIdentity() const {
    try {
        return pImpl ? pImpl->getEmbeddingSpaceIdentity() : std::string{};
    } catch (...) {
        spdlog::debug("EmbeddingGenerator::getEmbeddingSpaceIdentity failed");
        return {};
    }
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
    return pImpl ? pImpl->estimateTokenCount(text) : 0;
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
    return pImpl ? pImpl->getLastError() : std::string{};
}

bool EmbeddingGenerator::hasError() const {
    return pImpl && pImpl->hasError();
}

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

} // namespace embedding_utils

} // namespace yams::vector
