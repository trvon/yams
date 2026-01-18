#pragma once

#include <yams/search/search_engine.h>
#include <yams/daemon/resource/onnx_reranker_session.h>

#include <memory>

namespace yams::search {

/**
 * @brief Adapter to bridge OnnxRerankerSession to IReranker interface
 *
 * Wraps the daemon's OnnxRerankerSession to implement the search engine's
 * IReranker interface for cross-encoder document reranking.
 */
class OnnxRerankerAdapter : public IReranker {
public:
    explicit OnnxRerankerAdapter(std::shared_ptr<daemon::OnnxRerankerSession> session)
        : session_(std::move(session)) {}

    ~OnnxRerankerAdapter() override = default;

    Result<std::vector<float>> scoreDocuments(const std::string& query,
                                              const std::vector<std::string>& documents) override {
        if (!session_ || !session_->isValid()) {
            return Error{ErrorCode::InvalidState, "Reranker session not available"};
        }
        return session_->scoreBatch(query, documents);
    }

    bool isReady() const override { return session_ && session_->isValid(); }

    // Access underlying session for diagnostics
    daemon::OnnxRerankerSession* getSession() const { return session_.get(); }

private:
    std::shared_ptr<daemon::OnnxRerankerSession> session_;
};

} // namespace yams::search
