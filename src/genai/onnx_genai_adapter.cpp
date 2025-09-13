#include <spdlog/spdlog.h>
#include <yams/genai/onnx_genai_adapter.h>

namespace yams::genai {

struct OnnxGenAIAdapter::Impl {
    size_t dim = 0;
    bool ready = false;
};

OnnxGenAIAdapter::OnnxGenAIAdapter() : impl_(std::make_unique<Impl>()) {}
OnnxGenAIAdapter::~OnnxGenAIAdapter() = default;

bool OnnxGenAIAdapter::init(const std::string& model_id_or_path, const Options& opts) {
    (void)model_id_or_path;
    (void)opts;
#if defined(YAMS_GENAI_RUNTIME_PRESENT)
    // Placeholder: runtime headers are available, but implementation is pending.
    spdlog::info(
        "[GenAI] Runtime detected but pipeline not implemented; stub returning unavailable");
    impl_->dim = 0;
    impl_->ready = false;
    return false;
#else
    spdlog::debug("[GenAI] Headers not present; unified adapter stub");
    impl_->dim = 0;
    impl_->ready = false;
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

} // namespace yams::genai
