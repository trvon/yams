#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <yams/core/types.h>
#include <yams/storage/storage_engine.h>

namespace yams::storage {

enum class RemoteFallbackPolicy {
    Strict,
    FallbackLocalIfConfigured,
};

struct StorageBootstrapDecision {
    std::string configuredEngine{"local"};
    std::string activeEngine{"local"};
    std::filesystem::path requestedDataDir;
    std::filesystem::path activeDataDir;

    RemoteFallbackPolicy fallbackPolicy{RemoteFallbackPolicy::Strict};
    std::filesystem::path fallbackLocalDataDir;
    bool fallbackTriggered{false};
    std::string fallbackReason;

    // Populated when activeEngine == "s3"
    std::shared_ptr<IStorageEngine> storageEngineOverride;
};

std::optional<RemoteFallbackPolicy> parseRemoteFallbackPolicy(std::string value);
const char* toString(RemoteFallbackPolicy policy);
bool looksLikeCloudflareApiBearerToken(std::string_view value);
bool isCloudflareR2EndpointHost(std::string_view endpointHost);
std::string extractCloudflareR2AccountId(std::string_view endpointHost);
Result<std::string> loadCloudflareApiTokenFromKeychain(std::string_view accountId);
Result<void> storeCloudflareApiTokenInKeychain(std::string_view accountId,
                                               std::string_view apiToken);

Result<StorageBootstrapDecision>
resolveStorageBootstrapDecision(const std::filesystem::path& configPath,
                                const std::filesystem::path& requestedDataDir);

} // namespace yams::storage
