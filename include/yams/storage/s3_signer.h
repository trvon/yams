#pragma once

#include <span>
#include <string>
#include "storage_backend.h"
#include <curl/curl.h>
#include <yams/core/types.h>

struct curl_slist; // forward decl

namespace yams::storage {

class S3Signer {
public:
    /**
     * Sign the request using AWS Signature Version 4
     * method: HTTP method ("GET", "PUT", etc.)
     * url: full request URL (https://host/encoded/path[?query])
     * payload: optional body payload for signing; empty for GET/HEAD
     * Returns a curl_slist* with headers to attach via CURLOPT_HTTPHEADER.
     * Caller is responsible for freeing with curl_slist_free_all().
     */
    static Result<curl_slist*>
    signRequest(CURL* curl, const BackendConfig& config, const std::string& method,
                const std::string& url, std::span<const std::byte> payload = {},
                const std::vector<std::pair<std::string, std::string>>& extraHeaders = {});
};

} // namespace yams::storage
