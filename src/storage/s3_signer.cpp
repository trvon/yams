#include <yams/storage/s3_signer.h>

namespace yams::storage {

Result<void> S3Signer::signRequest(CURL* curl, const BackendConfig& config,
                                   const std::string& method, const std::string& url,
                                   std::span<const std::byte> payload) {
    (void)curl;
    (void)config;
    (void)method;
    (void)url;
    (void)payload;
    return Result<void>(Error{ErrorCode::NotImplemented});
}

} // namespace yams::storage