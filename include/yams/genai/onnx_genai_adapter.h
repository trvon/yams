// Unified ONNX GenAI adapter interface (stub until real pipeline wired)
#pragma once

#include <memory>
#include <string>
#include <vector>

namespace yams::genai {

class OnnxGenAIAdapter {
public:
    struct Options {
        int intra_op_threads = 4;
        int inter_op_threads = 1;
        bool normalize = true;
    };

    OnnxGenAIAdapter();
    ~OnnxGenAIAdapter();

    bool init(const std::string& model_id_or_path, const Options& opts);
    bool available() const;
    size_t embedding_dim() const;

    std::vector<float> embed(const std::string& text) const;
    std::vector<std::vector<float>> embed_batch(const std::vector<std::string>& texts) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace yams::genai
