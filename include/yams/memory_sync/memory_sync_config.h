// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <string_view>

#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/storage/storage_backend.h>

namespace yams::memory_sync {

/// Daemon-facing memory-sync configuration, parsed from a flat TOML map
/// (`[memory_sync]` section). No YAMS_* env knobs.
struct MemorySyncDaemonConfig {
    bool enabled{false};
    std::string nodeId{"default"};      /// this daemon's writer identity
    std::string backend{"filesystem"};  /// "filesystem" | "s3"
    std::string path;                   /// filesystem base dir, or s3:// URL
    std::uint32_t syncIntervalMs{5000}; /// periodic reconcile cadence

    /// Parse `memory_sync.*` keys from a flat `section.key -> value` map.
    static MemorySyncDaemonConfig fromToml(const std::map<std::string, std::string>& flat) {
        MemorySyncDaemonConfig cfg;
        auto get = [&](std::string_view suffix) -> const std::string* {
            const auto it = flat.find(std::string("memory_sync.") + std::string(suffix));
            return it == flat.end() ? nullptr : &it->second;
        };
        if (const auto* v = get("enabled")) {
            cfg.enabled = parseBool(*v);
        }
        if (const auto* v = get("node_id")) {
            cfg.nodeId = *v;
        }
        if (const auto* v = get("backend")) {
            cfg.backend = *v;
        }
        if (const auto* v = get("path")) {
            cfg.path = *v;
        }
        if (const auto* v = get("sync_interval_ms")) {
            try {
                const auto ms = std::stoul(*v);
                cfg.syncIntervalMs = static_cast<std::uint32_t>(ms);
            } catch (...) {
            }
        }
        return cfg;
    }

private:
    static bool parseBool(std::string_view v) {
        return v == "true" || v == "1" || v == "yes" || v == "on";
    }
};

/// Build a MemorySyncService from the daemon config. Resolves the backend:
/// `filesystem` -> FilesystemBackend at `path`; `s3` -> StorageBackendFactory.
/// Returns Error when the backend cannot be created/initialized.
inline Result<std::unique_ptr<MemorySyncService>>
createMemorySyncService(const MemorySyncDaemonConfig& config) {
    if (!config.enabled) {
        return Error{ErrorCode::InvalidState, "memory_sync is disabled"};
    }

    storage::BackendConfig backendConfig;
    std::unique_ptr<storage::IStorageBackend> backend;

    if (config.backend == "filesystem") {
        if (config.path.empty()) {
            return Error{ErrorCode::InvalidArgument, "memory_sync.path is required"};
        }
        backendConfig.type = "filesystem";
        backendConfig.localPath = std::filesystem::path(config.path);
        auto fs = std::make_unique<storage::FilesystemBackend>();
        if (auto r = fs->initialize(backendConfig); !r) {
            return r.error();
        }
        backend = std::move(fs);
    } else if (config.backend == "s3") {
        if (config.path.empty()) {
            return Error{ErrorCode::InvalidArgument, "memory_sync.path (s3:// URL) is required"};
        }
        // Delegate to the URL parser so scheme detection, credentials in the
        // authority, and query params (region, use_path_style, cache, timeout)
        // all flow through the same path as the daemon's storage resolver.
        backendConfig = storage::StorageBackendFactory::parseURL(config.path);
        if (backendConfig.type != "s3") {
            return Error{ErrorCode::InvalidArgument,
                         "memory_sync.backend is s3 but path is not an s3:// URL: " + config.path};
        }
        backend = storage::StorageBackendFactory::create(backendConfig);
        if (!backend) {
            return Error{ErrorCode::InvalidState, "failed to create s3 storage backend"};
        }
        if (auto r = backend->initialize(backendConfig); !r) {
            return r.error();
        }
    } else {
        return Error{ErrorCode::InvalidArgument, "unknown memory_sync.backend: " + config.backend};
    }

    return std::make_unique<MemorySyncService>(
        std::move(backend), MemorySyncConfig{config.nodeId, config.syncIntervalMs});
}

} // namespace yams::memory_sync
