#pragma once

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <yams/vector/batch_metrics.h>

namespace yams::vector {

struct DynamicBatcherConfig {
    std::size_t maxSequenceLengthTokens{512};
    double safetyFactor{0.9};
    std::size_t minTokens{1024};
    std::size_t maxTokens{262144};
    std::size_t initialTokens{0};  // if 0, computed from maxSequenceLengthTokens * 8 * safetyFactor
    std::size_t advisoryDocCap{0}; // optional hard cap on documents per batch
};

// Cheap token estimator: chars/4
inline std::size_t estimate_tokens_cheap(const std::string& s) {
    return std::max<std::size_t>(1, s.size() / 4);
}

class DynamicBatcher {
public:
    explicit DynamicBatcher(const DynamicBatcherConfig& cfg) : cfg_(cfg) {
        std::size_t base =
            cfg_.initialTokens
                ? cfg_.initialTokens
                : static_cast<std::size_t>(cfg_.maxSequenceLengthTokens * 8 * cfg_.safetyFactor);
        budgetTokens_ = std::clamp(base, cfg_.minTokens, cfg_.maxTokens);
        yams::vector::batchmetrics::set_effective_tokens(budgetTokens_);
    }

    std::size_t currentBudgetTokens() const { return budgetTokens_; }

    // Pack a batch of documents under the current token budget.
    // Returns how many docs were selected starting from index startIdx.
    template <typename GetTextFn>
    std::size_t packByTokens(std::size_t startIdx, std::size_t total,
                             std::vector<std::string>& outTexts,
                             std::vector<std::string>& outHashes, GetTextFn&& getTextAndHash) {
        outTexts.clear();
        outHashes.clear();
        std::size_t used = 0;
        std::size_t i = startIdx;
        while (i < total) {
            auto [ok, text, hash] = getTextAndHash(i);
            if (!ok) {
                ++i;
                continue;
            } // skip failed/empty extractions transparently
            std::size_t est = estimate_tokens_cheap(text);
            if (!outTexts.empty() && used + est > budgetTokens_) {
                break;
            }
            outTexts.push_back(std::move(text));
            outHashes.push_back(std::move(hash));
            used += est;
            ++i;
            if (cfg_.advisoryDocCap > 0 && outTexts.size() >= cfg_.advisoryDocCap) {
                break;
            }
        }
        if (outTexts.empty() && startIdx < total) {
            // Force at least one doc even if it's large
            auto [ok, text, hash] = getTextAndHash(startIdx);
            if (ok) {
                outTexts.push_back(std::move(text));
                outHashes.push_back(std::move(hash));
            }
        }
        return outTexts.size();
    }

    void onSuccess(std::size_t docsInBatch, std::size_t /*usedTokensApprox*/ = 0) {
        // AIMD: linear increase (10%), cap at maxTokens
        std::size_t inc = std::max<std::size_t>(1, budgetTokens_ / 10);
        budgetTokens_ = std::min(cfg_.maxTokens, budgetTokens_ + inc);
        yams::vector::batchmetrics::set_effective_tokens(budgetTokens_);
        yams::vector::batchmetrics::record_success(docsInBatch);
    }

    void onFailure() {
        // AIMD: multiplicative decrease (halve)
        budgetTokens_ = std::max(cfg_.minTokens, budgetTokens_ / 2);
        yams::vector::batchmetrics::set_effective_tokens(budgetTokens_);
        yams::vector::batchmetrics::record_failure();
        yams::vector::batchmetrics::record_backoff();
    }

private:
    DynamicBatcherConfig cfg_;
    std::size_t budgetTokens_;
};

} // namespace yams::vector
