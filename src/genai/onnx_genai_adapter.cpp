#include <spdlog/spdlog.h>
#include <algorithm>
#include <string>
#include <vector>
#include <yams/genai/onnx_genai_adapter.h>
#include <yams/vector/embedding_generator.h>

#if defined(YAMS_GENAI_RUNTIME_PRESENT)
#if __has_include(<ort_genai.h>)
#include <ort_genai.h>
#define YAMS_GENAI_HEADER_FOUND 1
#elif __has_include(<onnxruntime/genai/embedding.h>)
#include <onnxruntime/genai/embedding.h>
#include <onnxruntime/genai/pipeline.h>
#define YAMS_GENAI_HEADER_FOUND 1
#endif
#endif

namespace yams::genai {

struct OnnxGenAIAdapter::Impl {
    size_t dim = 0;
    bool ready = false;
    yams::vector::EmbeddingConfig cfg;
    std::unique_ptr<yams::vector::TextPreprocessor> preproc;
    yams::vector::ModelManager modelMgr;

    static bool looks_like_path(const std::string& s) {
        if (s.empty())
            return false;
        if (s.find('/') != std::string::npos || s.find("\\") != std::string::npos)
            return true;
        if (s.rfind("hf://", 0) == 0 || s.rfind("http://", 0) == 0 || s.rfind("https://", 0) == 0)
            return true;
        if (s.size() > 5 && s.rfind(".onnx") == s.size() - 5)
            return true;
        return false;
    }
};

OnnxGenAIAdapter::OnnxGenAIAdapter() : impl_(std::make_unique<Impl>()) {}
OnnxGenAIAdapter::~OnnxGenAIAdapter() = default;

bool OnnxGenAIAdapter::init(const std::string& model_id_or_path, const Options& opts) {
    try {
        // Initialize minimal local pipeline using existing ONNX path; no env guards.
        auto& c = impl_->cfg;
        c.backend = yams::vector::EmbeddingConfig::Backend::Local;
        c.model_name = model_id_or_path;
        c.model_path.clear();
        if (Impl::looks_like_path(model_id_or_path)) {
            c.model_path = model_id_or_path;
        }
        c.num_threads = std::max(1, opts.intra_op_threads);
        c.normalize_embeddings = opts.normalize;

        impl_->preproc = std::make_unique<yams::vector::TextPreprocessor>(c);

        // Load model via ModelManager
        const std::string name = c.model_name;
        const std::string path = c.model_path;
        if (!impl_->modelMgr.loadModel(name, path)) {
            spdlog::warn("[GenAI] Adapter init: failed to load model '{}' (path='{}')", name, path);
            impl_->ready = false;
            impl_->dim = 0;
            return false;
        }
        size_t d = impl_->modelMgr.getModelEmbeddingDim(name);
        if (d == 0)
            d = c.embedding_dim;
        impl_->dim = d;
        impl_->ready = true;
        spdlog::info("[GenAI] Adapter ready with model '{}' dim={} (threads={})", name, impl_->dim,
                     c.num_threads);
        return true;
    } catch (const std::exception& e) {
        spdlog::warn("[GenAI] Adapter init exception: {}", e.what());
        impl_->ready = false;
        impl_->dim = 0;
        return false;
    }
}

bool OnnxGenAIAdapter::available() const {
    return impl_ && impl_->ready;
}
size_t OnnxGenAIAdapter::embedding_dim() const {
    return impl_ ? impl_->dim : 0;
}

std::vector<float> OnnxGenAIAdapter::embed(const std::string& text) const {
    if (!impl_ || !impl_->ready)
        return {};
    try {
        const auto& c = impl_->cfg;
        // Determine sequence length
        size_t max_len = impl_->modelMgr.getModelMaxLength(c.model_name);
        if (max_len == 0)
            max_len = c.max_sequence_length;
        auto tokens = impl_->preproc->tokenize(text);
        tokens = impl_->preproc->truncateTokens(tokens, max_len);
        tokens = impl_->preproc->padTokens(tokens, max_len);
        auto mask = impl_->preproc->generateAttentionMask(tokens);
        std::vector<std::vector<int32_t>> btok = {tokens};
        std::vector<std::vector<int32_t>> bmsk = {mask};
        auto mat = impl_->modelMgr.runInference(c.model_name, btok, bmsk);
        if (mat.empty())
            return {};
        auto v = std::move(mat[0]);
        if (impl_->cfg.normalize_embeddings) {
            // simple L2 normalize
            double ss = 0.0;
            for (float f : v)
                ss += static_cast<double>(f) * f;
            if (ss > 0) {
                float inv = static_cast<float>(1.0 / std::sqrt(ss));
                for (auto& f : v)
                    f *= inv;
            }
        }
        return v;
    } catch (...) {
        return {};
    }
}

std::vector<std::vector<float>>
OnnxGenAIAdapter::embed_batch(const std::vector<std::string>& texts) const {
    if (!impl_ || !impl_->ready)
        return {};
    if (texts.empty())
        return {};
    try {
        const auto& c = impl_->cfg;
        size_t max_len = impl_->modelMgr.getModelMaxLength(c.model_name);
        if (max_len == 0)
            max_len = c.max_sequence_length;
        std::vector<std::vector<int32_t>> btok;
        std::vector<std::vector<int32_t>> bmsk;
        btok.reserve(texts.size());
        bmsk.reserve(texts.size());
        for (const auto& t : texts) {
            auto tokens = impl_->preproc->tokenize(t);
            tokens = impl_->preproc->truncateTokens(tokens, max_len);
            tokens = impl_->preproc->padTokens(tokens, max_len);
            btok.push_back(tokens);
            bmsk.push_back(impl_->preproc->generateAttentionMask(tokens));
        }
        auto mat = impl_->modelMgr.runInference(c.model_name, btok, bmsk);
        if (mat.size() != texts.size())
            return {};
        if (impl_->cfg.normalize_embeddings) {
            for (auto& row : mat) {
                double ss = 0.0;
                for (float f : row)
                    ss += static_cast<double>(f) * f;
                if (ss > 0) {
                    float inv = static_cast<float>(1.0 / std::sqrt(ss));
                    for (auto& f : row)
                        f *= inv;
                }
            }
        }
        return mat;
    } catch (...) {
        return {};
    }
}

} // namespace yams::genai
