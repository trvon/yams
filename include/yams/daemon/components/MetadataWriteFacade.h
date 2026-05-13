#pragma once

#include <yams/daemon/components/WriteBatchCoalescer.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>

#include <memory>
#include <string>
#include <vector>

namespace yams::daemon {

class MetadataWriteFacade {
public:
    MetadataWriteFacade(WriteCoordinator* wc, metadata::MetadataRepository* repo);
    ~MetadataWriteFacade();

    void setMetadata(int64_t docId, const std::string& key, metadata::MetadataValue value);
    void updateExtractionStatus(int64_t docId, bool contentExtracted,
                                metadata::ExtractionStatus status, const std::string& error);
    void updateRepairStatus(const std::vector<std::string>& hashes, metadata::RepairStatus status);

    void flush();

private:
    WriteCoordinator* wc_;
    metadata::MetadataRepository* repo_;
    WriteBatchCoalescer coalescer_;
};

} // namespace yams::daemon
