#include <spdlog/spdlog.h>
#include <yams/vector/onnx_genai_adapter.h>

namespace yams::vector {

struct OnnxGenAIAdapter::Impl {
#if defined(YAMS_USE_ONNX_GENAI_HEADER_PRESENT)
    // Placeholder for future GenAI runtime objects (e.g., pipeline/session)
#endif
    size_t dim = 0;
    bool ready = false;
};

OnnxGenAIAdapter::OnnxGenAIAdapter() : impl_(std::make_unique<Impl>()) {}
OnnxGenAIAdapter::~OnnxGenAIAdapter() = default;

bool OnnxGenAIAdapter::init(const std::string& model_id_or_path, const Options& opts) {
    (void)model_id_or_path;
    (void)opts;
#if defined(YAMS_USE_ONNX_GENAI_HEADER_PRESENT)
    // GenAI headers are present, but the concrete pipeline is not yet wired.
    // Keep adapter disabled while returning a clear diagnostic.
    impl_->dim = 0;
    impl_->ready = false;
    spdlog::info("[GenAI] ONNX Runtime GenAI headers detected; adapter not yet implemented");
    return false;
#else
    impl_->dim = 0;
    impl_->ready = false;
    spdlog::debug("[GenAI] Adapter stub init; headers not detected");
    return false;
#endif
}

bool OnnxGenAIAdapter::available() const {
    return impl_ && impl_->ready;
}
size_t OnnxGenAIAdapter::embedding_dim() const {
    return impl_ ? impl_->dim : 0;
}

std::vector<float> OnnxGenAIAdapter::embed(const std::string& text) const {
    (void)text;
    return {};
}

std::vector<std::vector<float>>
OnnxGenAIAdapter::embed_batch(const std::vector<std::string>& texts) const {
    (void)texts;
    return {};
}

} // namespace yams::vector
