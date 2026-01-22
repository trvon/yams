#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <yams/core/types.h>

namespace yams::daemon {

struct ColbertConfig {
    std::string model_path;
    std::string model_name;
    size_t max_sequence_length = 512;
    size_t token_dim = 48;
    int num_threads = 4;
    bool enable_gpu = false;
    int32_t query_marker_id = 50368;
    int32_t doc_marker_id = 50369;
    std::vector<int32_t> skiplist;
};

struct ColbertEmbeddings {
    std::vector<std::vector<float>> token_embeddings;
    std::vector<int32_t> token_ids;
};

class OnnxColbertSession {
public:
    OnnxColbertSession(const std::string& modelPath, const std::string& modelName,
                       const ColbertConfig& config);
    ~OnnxColbertSession();

    Result<ColbertEmbeddings> encodeQuery(const std::string& query);
    Result<ColbertEmbeddings> encodeDocument(const std::string& document);
    Result<std::vector<ColbertEmbeddings>>
    encodeDocuments(const std::vector<std::string>& documents);

    Result<std::vector<float>> encodeDocumentEmbedding(const std::string& document);
    Result<std::vector<std::vector<float>>>
    encodeDocumentEmbeddings(const std::vector<std::string>& documents);

    float computeMaxSim(const ColbertEmbeddings& query, const ColbertEmbeddings& document) const;

    std::string getName() const { return modelName_; }
    size_t getMaxSequenceLength() const { return maxSequenceLength_; }
    size_t getTokenDim() const { return tokenDim_; }

    bool isValid() const;

private:
    std::string modelPath_;
    std::string modelName_;
    ColbertConfig config_;
    size_t maxSequenceLength_ = 512;
    size_t tokenDim_ = 48;

    class Impl;
    std::unique_ptr<Impl> pImpl_;

    OnnxColbertSession(const OnnxColbertSession&) = delete;
    OnnxColbertSession& operator=(const OnnxColbertSession&) = delete;
};

} // namespace yams::daemon
