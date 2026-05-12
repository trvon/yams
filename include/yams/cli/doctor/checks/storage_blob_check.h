#pragma once

#include <cstdint>
#include <ostream>
#include <string>

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::cli::doctor {

class DoctorContext;

/// Checks storage blob integrity: verifies that CAS blobs on disk
/// match the metadata references and reports missing or orphaned blobs.
class StorageBlobCheck {
public:
    struct Result {
        uint64_t metadataDocuments{0};
        uint64_t storageObjects{0};
        uint64_t storageBytes{0};
        bool ok{true};
    };

    Result execute(const DoctorContext& ctx);
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
