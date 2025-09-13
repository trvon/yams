#include <spdlog/spdlog.h>
#include <yams/daemon/resource/onnx_genai_adapter.h>

namespace yams::daemon {

struct OnnxGenAIAdapter::Impl {
#ifdef YAMS_ENABLE_ONNX_GENAI
    // Placeholder for real GenAI objects, e.g., genai::Embedding, genai::Session, etc.
    size_t dim = 0;
    bool ready = false;
#else
    size_t dim = 0;
    bool ready = false;
#endif
};

OnnxGenAIAdapter::OnnxGenAIAdapter() : impl_(std::make_unique<Impl>()) {}
OnnxGenAIAdapter::~OnnxGenAIAdapter() = default;

bool OnnxGenAIAdapter::available() const {
#ifdef YAMS_ENABLE_ONNX_GENAI
    return impl_ && impl_->ready;
#else
    return false;
#endif
}

bool OnnxGenAIAdapter::init(const std::string& model_id_or_path, const Options& opts) {
    (void)model_id_or_path;
    (void)opts;
#ifdef YAMS_ENABLE_ONNX_GENAI
    spdlog::info("[GenAI] (daemon) initializing GenAI adapter for '{}'", model_id_or_path);
    // TODO: Wire real GenAI initialization when the package is available
    try {
        impl_->dim = 0; // to be discovered from the pipeline
        impl_->ready = true;
        return true;
    } catch (const std::exception& e) {
        spdlog::warn("[GenAI] init failed: {}", e.what());
        impl_->ready = false;
        return false;
    }
#else
    spdlog::debug("[GenAI] Disabled at build time (YAMS_ENABLE_ONNX_GENAI not set)");
    impl_->ready = false;
    return false;
#endif
}

size_t OnnxGenAIAdapter::embedding_dim() const {
    return impl_ ? impl_->dim : 0;
}

std::vector<float> OnnxGenAIAdapter::embed(const std::string& text) const {
    (void)text;
#ifdef YAMS_ENABLE_ONNX_GENAI
    if (!available())
        return {};
    // TODO: call GenAI encode
    return {};
#else
    return {};
#endif
}

std::vector<std::vector<float>>
OnnxGenAIAdapter::embed_batch(const std::vector<std::string>& texts) const {
    (void)texts;
#ifdef YAMS_ENABLE_ONNX_GENAI
    if (!available())
        return {};
    // TODO: call GenAI batch encode
    return {};
#else
    return {};
#endif
}

} // namespace yams::daemon
