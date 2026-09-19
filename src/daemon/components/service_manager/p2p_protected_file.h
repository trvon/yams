// Private, non-installed implementation header. Declares the P2P protected-file
// accessors whose definitions live in p2p_integration.cpp so that
// memory_sync.cpp can consume them without exposing the platform-specific
// key-protection machinery. Not installed; do not include from public headers.
#pragma once

#include <cstddef>
#include <filesystem>
#include <string>
#include <string_view>

#include <yams/core/types.h>

namespace yams::daemon::service_manager_detail {

yams::Result<std::string> readProtectedP2pPrivateKey(const std::filesystem::path& keyPath);
yams::Result<std::string> readProtectedP2pTrustFile(const std::filesystem::path& path,
                                                    std::size_t maxBytes);
yams::Result<void> writeExclusiveP2pPrivateKey(const std::filesystem::path& path,
                                               std::string_view contents);

} // namespace yams::daemon::service_manager_detail
