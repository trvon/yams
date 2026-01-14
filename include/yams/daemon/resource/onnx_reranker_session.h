#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <yams/core/types.h>

namespace yams::daemon {

// ============================================================================
// ONNX Reranker Session - Cross-encoder model for (query, document) scoring
// ============================================================================

struct RerankerConfig {
    std::string model_path;
    std::string model_name;
    size_t max_sequence_length = 512;
    int num_threads = 4;
    bool enable_gpu = false;
};

struct RerankerResult {
    float score;           // Relevance score (higher = more relevant)
    size_t document_index; // Index of the document in the input batch
};

class OnnxRerankerSession {
public:
    OnnxRerankerSession(const std::string& modelPath, const std::string& modelName,
                        const RerankerConfig& config);
    ~OnnxRerankerSession();

    // Score a single (query, document) pair
    Result<float> score(const std::string& query, const std::string& document);

    // Score a batch of (query, document) pairs efficiently
    // Returns scores in the same order as input documents
    Result<std::vector<float>> scoreBatch(const std::string& query,
                                          const std::vector<std::string>& documents);

    // Rerank documents by relevance to query
    // Returns document indices sorted by score (highest first), with scores
    Result<std::vector<RerankerResult>>
    rerank(const std::string& query, const std::vector<std::string>& documents, size_t topK = 0);

    // Model information
    std::string getName() const { return modelName_; }
    size_t getMaxSequenceLength() const { return maxSequenceLength_; }

    // Validation
    bool isValid() const;

private:
    std::string modelPath_;
    std::string modelName_;
    RerankerConfig config_;
    size_t maxSequenceLength_ = 512;

    // ONNX Runtime components (PIMPL)
    class Impl;
    std::unique_ptr<Impl> pImpl_;

    // No copy/move
    OnnxRerankerSession(const OnnxRerankerSession&) = delete;
    OnnxRerankerSession& operator=(const OnnxRerankerSession&) = delete;
};

} // namespace yams::daemon
