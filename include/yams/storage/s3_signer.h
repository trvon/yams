#pragma once

#include <span>
#include <string>
#include "storage_backend.h"
#include <curl/curl.h>
#include <yams/core/types.h>

namespace yams::storage {

class S3Signer {
public:
    /**
     * Sign the request using AWS Signature Version 4
     * method: HTTP method ("GET", "PUT", etc.)
     * url: full request URL
     * payload: optional body payload for signing; empty for GET/HEAD
     * Sets required headers (x-amz-date, Authorization, x-amz-content-sha256)
     */
    static Result<void> signRequest(CURL* curl, const BackendConfig& config,
                                    const std::string& method, const std::string& url,
                                    std::span<const std::byte> payload = {});
};

} // namespace yams::storage