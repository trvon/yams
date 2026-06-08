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
    if (wc_) {
        coalescer_.addOp(SetMetadataBatchOp{{{docId, key, std::move(value)}}}, wc_);
    } else {
        pendingMetadata_.emplace_back(docId, key, std::move(value));
    }
}

void MetadataWriteFacade::updateExtractionStatus(int64_t docId, bool contentExtracted,
                                                 metadata::ExtractionStatus status,
                                                 const std::string& error) {
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
    if (!repo_) {
        return;
    }

    if (!pendingMetadata_.empty()) {
        auto result = repo_->setMetadataBatch(pendingMetadata_);
        if (!result) {
            spdlog::warn("[MetadataWriteFacade] fallback metadata batch flush failed: {}",
                         result.error().message);
        }
        pendingMetadata_.clear();
    }

    if (!pendingExtractionUpdates_.empty()) {
        auto result = repo_->batchUpdateDocumentExtractionStatuses(pendingExtractionUpdates_);
        if (!result) {
            spdlog::warn("[MetadataWriteFacade] fallback extraction-status flush failed: {}",
                         result.error().message);
        }
        pendingExtractionUpdates_.clear();
    }

    for (auto& op : pendingRepairStatusOps_) {
        if (op.hashes.empty()) {
            continue;
        }
        auto result = repo_->batchUpdateDocumentRepairStatuses(op.hashes, op.status);
        if (!result) {
            spdlog::warn("[MetadataWriteFacade] fallback repair-status flush failed: {}",
                         result.error().message);
        }
    }
    pendingRepairStatusOps_.clear();
}

} // namespace yams::daemon
