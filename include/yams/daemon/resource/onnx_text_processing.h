#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::daemon {

struct OnnxTextConfig {
    struct ModelSettings {
        std::string vocab_file;
        bool do_lower_case = true;
        std::string cls_token = "[CLS]";
        std::string sep_token = "[SEP]";
        std::string unk_token = "[UNK]";
        std::string pad_token = "[PAD]";
        std::string mask_token = "[MASK]";
    } model_settings;

    size_t max_sequence_length = 512;
    size_t embedding_dim = 384;
    bool normalize_embeddings = true;
    float padding_token_id = 0.0f;
    float unk_token_id = 1.0f;
    std::string model_name = "all-MiniLM-L6-v2";
    std::string model_path = "models/all-MiniLM-L6-v2.onnx";
    bool enable_gpu = false;
    int num_threads = -1;
    int inter_op_threads = -1;
};

class OnnxTextPreprocessor {
public:
    explicit OnnxTextPreprocessor(const OnnxTextConfig& config);

    std::string normalizeText(const std::string& text);
    std::vector<int32_t> tokenize(const std::string& text);
    std::vector<std::vector<int32_t>> tokenizeBatch(const std::vector<std::string>& texts);
    std::vector<int32_t> truncateTokens(const std::vector<int32_t>& tokens, size_t max_length);
    std::vector<int32_t> padTokens(const std::vector<int32_t>& tokens, size_t target_length);
    std::vector<int32_t> generateAttentionMask(const std::vector<int32_t>& tokens);
    size_t getVocabSize() const;
    bool isValidToken(int32_t token_id) const;
    std::string decodeToken(int32_t token_id) const;

private:
    void initializeBasicVocab();
    int32_t getTokenId(const std::string& token);
    int32_t getSpecialTokenId(const std::string& token);

    OnnxTextConfig config_;
    std::unordered_map<std::string, int32_t> vocab_to_id_;
};

} // namespace yams::daemon
