#include <yams/daemon/components/MetadataWriteFacade.h>

#include <utility>

namespace yams::daemon {

MetadataWriteFacade::MetadataWriteFacade(WriteCoordinator* wc, metadata::MetadataRepository* repo)
    : wc_(wc), repo_(repo), coalescer_("MetadataWriteFacade") {}

MetadataWriteFacade::~MetadataWriteFacade() {
    flush();
}

void MetadataWriteFacade::setMetadata(int64_t docId, const std::string& key,
                                      metadata::MetadataValue value) {
    if (wc_) {
        coalescer_.addOp(SetMetadataBatchOp{{{docId, key, std::move(value)}}}, wc_);
    } else {
        (void)repo_->setMetadata(docId, key, std::move(value));
    }
}

void MetadataWriteFacade::updateExtractionStatus(int64_t docId, bool contentExtracted,
                                                 metadata::ExtractionStatus status,
                                                 const std::string& error) {
    if (wc_) {
        coalescer_.addOp(UpdateExtractionStatusOp{docId, contentExtracted, status, error}, wc_);
    } else {
        (void)repo_->updateDocumentExtractionStatus(docId, contentExtracted, status, error);
    }
}

void MetadataWriteFacade::updateRepairStatus(const std::vector<std::string>& hashes,
                                             metadata::RepairStatus status) {
    if (wc_) {
        coalescer_.addOp(UpdateRepairStatusOp{hashes, status}, wc_);
    } else {
        (void)repo_->batchUpdateDocumentRepairStatuses(hashes, status);
    }
}

void MetadataWriteFacade::flush() {
    if (wc_) {
        coalescer_.flush(wc_);
    }
}

} // namespace yams::daemon
