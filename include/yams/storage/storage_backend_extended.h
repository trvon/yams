#pragma once

#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <yams/core/types.h>
#include <yams/storage/storage_backend.h>

namespace yams::storage {

class IStorageBackendExtended : public IStorageBackend {
public:
    virtual ~IStorageBackendExtended() = default;

    virtual Result<std::string>
    initiateMultipartUpload(std::string_view key,
                            const std::unordered_map<std::string, std::string>& metadata) = 0;

    virtual Result<std::string> uploadPart(std::string_view key, const std::string& uploadId,
                                           int partNumber, std::span<const std::byte> data,
                                           const std::string& checksum = "") = 0;

    virtual Result<void>
    completeMultipartUpload(std::string_view key, const std::string& uploadId,
                            const std::vector<std::pair<int, std::string>>& parts) = 0;

    virtual Result<void> abortMultipartUpload(std::string_view key,
                                              const std::string& uploadId) = 0;

    virtual Result<std::string> calculateChecksum(std::span<const std::byte> data,
                                                  const std::string& algorithm) = 0;

    virtual Result<bool> verifyChecksum(std::string_view key, const std::string& expectedChecksum,
                                        const std::string& algorithm) = 0;
};

} // namespace yams::storage
