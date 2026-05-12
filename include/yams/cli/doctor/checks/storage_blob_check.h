#pragma once

#include <cstdint>
#include <ostream>
#include <string>
#include <vector>

namespace yams::cli::doctor {

class DoctorContext;

/// Verifies CAS blob integrity: metadata references vs on-disk objects,
/// missing blobs, and orphaned storage objects.
class StorageBlobCheck {
public:
    struct MissingBlob {
        std::string sha256Hash; // metadata hash key
        std::string filePath;   // document file path
        std::string fileHash;   // actual file hash (if known)
    };

    struct OrphanedBlob {
        std::string objectPath; // storage object relative path
        uint64_t size{0};       // on-disk size in bytes
    };

    struct Result {
        uint64_t metadataDocuments{0};
        uint64_t storageObjects{0};
        uint64_t storageBytes{0};
        uint64_t missingBlobs{0};
        uint64_t orphanedBlobs{0};
        std::vector<MissingBlob> missingBlobDetails;   // capped at 50
        std::vector<OrphanedBlob> orphanedBlobDetails; // capped at 50
        bool ok{true};
    };

    Result execute(const DoctorContext& ctx);
    static void render(std::ostream& os, const Result& r);
};

} // namespace yams::cli::doctor
