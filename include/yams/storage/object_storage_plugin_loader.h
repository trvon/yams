#pragma once

#include <memory>
#include <string>
#include <yams/storage/storage_backend.h>

namespace yams::storage {

std::unique_ptr<IStorageBackend> tryCreatePluginBackendByName(const std::string& name,
                                                              const BackendConfig& config);
std::unique_ptr<IStorageBackend> tryCreateS3PluginBackend(const BackendConfig& config);

} // namespace yams::storage
