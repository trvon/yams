#include <yams/daemon/components/MetadataWriteFacade.h>

#include <spdlog/spdlog.h>
#include <yams/core/assert.hpp>

#include <utility>

namespace yams::daemon {

MetadataWriteFacade::MetadataWriteFacade(WriteCoordinator* wc, metadata::MetadataRepository* repo)
    : wc_(wc), repo_(repo), coalescer_("MetadataWriteFacade") {
    YAMS_PRECONDITION(wc_ != nullptr || repo_ != nullptr,
                      "MetadataWriteFacade requires a repository when coordinator is absent");
}

MetadataWriteFacade::~MetadataWriteFacade() {
    flush();
}

void MetadataWriteFacade::setMetadata(int64_t docId, const std::string& key,
                                      metadata::MetadataValue value) {
    YAMS_DCHECK(docId > 0, "metadata facade writes should target a persisted document id");
    if (wc_) {
        coalescer_.addOp(SetMetadataBatchOp{{{docId, key, std::move(value)}}}, wc_);
    } else {
        pendingMetadata_.emplace_back(docId, key, std::move(value));
    }
}

void MetadataWriteFacade::updateExtractionStatus(int64_t docId, bool contentExtracted,
                                                 metadata::ExtractionStatus status,
                                                 const std::string& error) {
    YAMS_DCHECK(docId > 0,
                "metadata facade extraction updates should target a persisted document id");
    if (wc_) {
        coalescer_.addOp(UpdateExtractionStatusOp{docId, contentExtracted, status, error}, wc_);
    } else {
        pendingExtractionUpdates_.push_back(metadata::ExtractionStatusUpdate{
            .documentId = docId,
            .contentExtracted = contentExtracted,
            .status = status,
            .error = error,
        });
    }
}

void MetadataWriteFacade::updateRepairStatus(const std::vector<std::string>& hashes,
                                             metadata::RepairStatus status) {
    for (const auto& hash : hashes) {
        YAMS_DCHECK(!hash.empty(),
                    "metadata facade repair updates should target non-empty document hashes");
    }
    if (wc_) {
        coalescer_.addOp(UpdateRepairStatusOp{hashes, status}, wc_);
        return;
    }
    if (hashes.empty()) {
        return;
    }
    if (!pendingRepairStatusOps_.empty() && pendingRepairStatusOps_.back().status == status) {
        auto& last = pendingRepairStatusOps_.back().hashes;
        last.insert(last.end(), hashes.begin(), hashes.end());
        return;
    }
    pendingRepairStatusOps_.push_back(UpdateRepairStatusOp{hashes, status});
}

void MetadataWriteFacade::flush() {
    if (wc_) {
        coalescer_.flush(wc_);
        return;
    }

    YAMS_ASSERT(repo_ != nullptr, "fallback metadata flush requires a repository");

    if (!pendingMetadata_.empty()) {
        auto result = repo_->setMetadataBatch(pendingMetadata_);
        if (!result) {
            spdlog::warn("[MetadataWriteFacade] fallback metadata batch flush failed: {}",
                         result.error().message);
        } else {
            pendingMetadata_.clear();
        }
    }

    if (!pendingExtractionUpdates_.empty()) {
        auto result = repo_->batchUpdateDocumentExtractionStatuses(pendingExtractionUpdates_);
        if (!result) {
            spdlog::warn("[MetadataWriteFacade] fallback extraction-status flush failed: {}",
                         result.error().message);
        } else {
            pendingExtractionUpdates_.clear();
        }
    }

    std::vector<UpdateRepairStatusOp> failedRepairStatusOps;
    failedRepairStatusOps.reserve(pendingRepairStatusOps_.size());
    for (auto& op : pendingRepairStatusOps_) {
        if (op.hashes.empty()) {
            continue;
        }
        auto result = repo_->batchUpdateDocumentRepairStatuses(op.hashes, op.status);
        if (!result) {
            spdlog::warn("[MetadataWriteFacade] fallback repair-status flush failed: {}",
                         result.error().message);
            failedRepairStatusOps.push_back(std::move(op));
        }
    }
    pendingRepairStatusOps_ = std::move(failedRepairStatusOps);
}

} // namespace yams::daemon
