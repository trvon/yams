// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map> // IWYU pragma: keep
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/storage/storage_backend.h>

namespace yams::memory_sync {

/// Daemon-facing memory-sync configuration, parsed from a flat TOML map
/// (`[memory_sync]` section). No YAMS_* env knobs.
inline bool isCanonicalWriterUuid(std::string_view value) {
    if (value.size() != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' ||
        value[23] != '-') {
        return false;
    }
    for (std::size_t i = 0; i < value.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            continue;
        }
        if (!std::isxdigit(static_cast<unsigned char>(value[i]))) {
            return false;
        }
    }
    return true;
}

inline bool isCanonicalSessionId(std::string_view value) {
    return !value.empty() && value.size() <= 128 &&
           std::all_of(value.begin(), value.end(),
                       [](unsigned char ch) { return std::isalnum(ch) || ch == '-' || ch == '_'; });
}

inline bool isCanonicalCorpusId(std::string_view value) {
    return !value.empty() && value.size() <= 128 &&
           std::all_of(value.begin(), value.end(), [](unsigned char ch) {
               return std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.';
           });
}

struct MemorySyncDaemonConfig {
    bool enabled{false};
    std::string nodeId;                     /// explicit stable daemon writer UUID
    std::string corpusId;                   /// stable replication-domain identifier
    std::uint64_t corpusEpoch{0};           /// incompatible reset/migration generation
    std::string backend{"filesystem"};      /// "filesystem" | "s3"
    std::string path;                       /// filesystem base dir, or s3:// URL
    std::uint32_t syncIntervalMs{5000};     /// periodic reconcile cadence
    MemorySyncLimits limits{};              /// bounded scan, envelope, value, and cache budgets
    std::string mode{"persistent"};         /// persistent | temporary
    std::string sessionId;                  /// required owner label for temporary mode
    std::uint32_t temporarySessionTtlMs{0}; /// 0 disables stale-session collection
    bool allowLegacyUnbound{false};         /// one-time schema-v2 migration gate
    bool writerAuthRequired{false};         /// require schema-v4 Ed25519 envelopes
    std::string writerAuthManifestPath;     /// ACL-protected corpus/epoch key manifest

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
        if (const auto* v = get("corpus_id")) {
            cfg.corpusId = *v;
        }
        if (const auto* v = get("corpus_epoch")) {
            std::uint64_t epoch = 0;
            const auto [end, error] = std::from_chars(v->data(), v->data() + v->size(), epoch);
            if (error == std::errc{} && end == v->data() + v->size()) {
                cfg.corpusEpoch = epoch;
            }
        }
        if (const auto* v = get("backend")) {
            cfg.backend = *v;
        }
        if (const auto* v = get("path")) {
            cfg.path = *v;
        }
        if (const auto* v = get("mode")) {
            cfg.mode = *v;
        }
        if (const auto* v = get("session_id")) {
            cfg.sessionId = *v;
        }
        if (const auto* v = get("allow_legacy_unbound")) {
            cfg.allowLegacyUnbound = parseBool(*v);
        }
        if (const auto* v = get("writer_auth_required")) {
            cfg.writerAuthRequired = parseBool(*v);
        }
        if (const auto* v = get("writer_auth_manifest")) {
            cfg.writerAuthManifestPath = *v;
        }
        if (const auto* v = get("temporary_session_ttl_ms")) {
            std::uint64_t ttl = 0;
            const auto [end, error] = std::from_chars(v->data(), v->data() + v->size(), ttl);
            if (error == std::errc{} && end == v->data() + v->size() &&
                ttl <= std::numeric_limits<std::uint32_t>::max()) {
                cfg.temporarySessionTtlMs = static_cast<std::uint32_t>(ttl);
            }
        }
        if (const auto* v = get("sync_interval_ms")) {
            std::uint64_t ms = 0;
            const auto [end, error] = std::from_chars(v->data(), v->data() + v->size(), ms);
            if (error == std::errc{} && end == v->data() + v->size() &&
                ms <= std::numeric_limits<std::uint32_t>::max()) {
                cfg.syncIntervalMs = static_cast<std::uint32_t>(ms);
            }
        }
        const auto parseLimit = [&](std::string_view key, std::size_t& target) {
            if (const auto* v = get(key)) {
                std::uint64_t parsed = 0;
                const auto [end, error] = std::from_chars(v->data(), v->data() + v->size(), parsed);
                if (error == std::errc{} && end == v->data() + v->size() && parsed != 0 &&
                    parsed <= std::numeric_limits<std::size_t>::max()) {
                    target = static_cast<std::size_t>(parsed);
                }
            }
        };
        parseLimit("max_index_objects_per_sync", cfg.limits.maxIndexObjectsPerSync);
        parseLimit("max_envelope_bytes", cfg.limits.maxEnvelopeBytes);
        parseLimit("max_value_bytes", cfg.limits.maxValueBytes);
        parseLimit("max_merged_keys", cfg.limits.maxMergedKeys);
        parseLimit("max_cache_bytes", cfg.limits.maxCacheBytes);
        parseLimit("max_tracked_identities", cfg.limits.maxTrackedIdentities);
        return cfg;
    }

private:
    static bool parseBool(std::string_view v) {
        return v == "true" || v == "1" || v == "yes" || v == "on";
    }
};

inline Result<storage::BackendConfig>
resolveMemorySyncS3Config(const MemorySyncDaemonConfig& config,
                          std::optional<storage::BackendConfig> credentialSource = std::nullopt) {
    if (config.path.empty()) {
        return Error{ErrorCode::InvalidArgument, "memory_sync.path (s3:// URL) is required"};
    }
    std::string scopedUrl = config.path;
    if (config.mode == "temporary") {
        scopedUrl += (scopedUrl.ends_with('/') ? "" : "/");
        scopedUrl += ".sessions/" + config.sessionId;
    }
    auto backendConfig =
        credentialSource.value_or(storage::StorageBackendFactory::parseURL(scopedUrl));
    // Credentials and endpoint policy may come from the primary storage bootstrap, but
    // addressing always comes from memory_sync.path so distinct buckets cannot alias.
    backendConfig.type = "s3";
    backendConfig.url = scopedUrl;
    const auto parsedTarget = storage::StorageBackendFactory::parseURL(scopedUrl);
    if (parsedTarget.type != "s3") {
        return Error{ErrorCode::InvalidArgument,
                     "memory_sync.backend is s3 but path is not an s3:// URL: " + config.path};
    }
    return backendConfig;
}

inline Result<std::string> readWriterAuthFile(const std::filesystem::path& path,
                                              std::size_t maxBytes = std::size_t{1024} * 1024) {
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (error || size == 0 || size > maxBytes) {
        return Error{ErrorCode::InvalidArgument,
                     "writer authentication file is missing, empty, or oversized"};
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return Error{ErrorCode::IOError, "cannot read writer authentication file"};
    }
    std::string content(static_cast<std::size_t>(size), '\0');
    input.read(content.data(), static_cast<std::streamsize>(content.size()));
    if (!input) {
        return Error{ErrorCode::IOError, "cannot read complete writer authentication file"};
    }
    return content;
}

inline Result<std::shared_ptr<const WriterAuthenticator>>
loadWriterAuthenticator(const MemorySyncDaemonConfig& config) {
    if (!config.writerAuthRequired) {
        return std::shared_ptr<const WriterAuthenticator>{};
    }
    if (config.writerAuthManifestPath.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "writer_auth_manifest is required when writer authentication is enabled"};
    }
    if (config.allowLegacyUnbound) {
        return Error{ErrorCode::InvalidArgument,
                     "legacy unbound migration cannot run in authenticated writer mode"};
    }

    const std::filesystem::path manifestPath(config.writerAuthManifestPath);
    auto manifestBytes = readWriterAuthFile(manifestPath);
    if (!manifestBytes) {
        return manifestBytes.error();
    }
    try {
        const auto manifest = nlohmann::json::parse(manifestBytes.value());
        if (manifest.at("schema_version").get<std::uint32_t>() != 1 ||
            manifest.at("corpus_id").get<std::string>() != config.corpusId ||
            manifest.at("corpus_epoch").get<std::uint64_t>() != config.corpusEpoch) {
            return Error{ErrorCode::InvalidArgument,
                         "writer authentication manifest corpus or epoch mismatch"};
        }
        const auto base = manifestPath.parent_path();
        const auto& local = manifest.at("local_key");
        if (local.at("writer_id").get<std::string>() != config.nodeId) {
            return Error{ErrorCode::InvalidArgument,
                         "writer authentication manifest local writer mismatch"};
        }
        const auto resolve = [&](const std::string& value) {
            const std::filesystem::path path(value);
            return path.is_absolute() ? path : base / path;
        };

        WriterAuthConfig auth;
        auth.required = true;
        auth.localWriterId = config.nodeId;
        auth.localKeyId = local.at("key_id").get<std::string>();
        auto privatePem = readWriterAuthFile(
            resolve(local.at("private_key_path").get<std::string>()), std::size_t{64} * 1024);
        if (!privatePem) {
            return privatePem.error();
        }
        auth.localPrivateKeyPem = std::move(privatePem.value());

        const auto& trusted = manifest.at("trusted_writers");
        if (!trusted.is_array() || trusted.empty() ||
            trusted.size() > config.limits.maxTrackedIdentities) {
            return Error{ErrorCode::InvalidArgument,
                         "trusted writer manifest is empty or exceeds configured bound"};
        }
        auth.trustedKeys.reserve(trusted.size());
        for (const auto& entry : trusted) {
            auto publicPem = readWriterAuthFile(
                resolve(entry.at("public_key_path").get<std::string>()), std::size_t{64} * 1024);
            if (!publicPem) {
                return publicPem.error();
            }
            auth.trustedKeys.push_back(TrustedWriterKey{
                entry.at("writer_id").get<std::string>(), entry.at("key_id").get<std::string>(),
                std::move(publicPem.value()), entry.value("revoked", false)});
        }
        return WriterAuthenticator::create(std::move(auth), config.corpusId, config.corpusEpoch);
    } catch (const std::exception&) {
        return Error{ErrorCode::InvalidArgument, "writer authentication manifest is malformed"};
    }
}

/// Build a MemorySyncService from the daemon config. Resolves the backend:
/// `filesystem` -> FilesystemBackend at `path`; `s3` -> StorageBackendFactory.
/// Returns Error when the backend cannot be created/initialized.
inline Result<std::unique_ptr<MemorySyncService>>
createMemorySyncService(const MemorySyncDaemonConfig& config,
                        std::optional<storage::BackendConfig> resolvedS3Config = std::nullopt,
                        std::function<bool()> canAdmitRemoteWork = {}) {
    if (!config.enabled) {
        return Error{ErrorCode::InvalidState, "memory_sync is disabled"};
    }

    if (!isCanonicalWriterUuid(config.nodeId)) {
        return Error{ErrorCode::InvalidArgument,
                     "memory_sync.node_id must be an explicit canonical UUID"};
    }
    if (!isCanonicalCorpusId(config.corpusId) || config.corpusEpoch == 0) {
        return Error{ErrorCode::InvalidArgument,
                     "memory_sync.corpus_id and positive corpus_epoch are required"};
    }

    auto writerAuth = loadWriterAuthenticator(config);
    if (!writerAuth) {
        return writerAuth.error();
    }

    const bool temporary = config.mode == "temporary";
    if (config.mode != "persistent" && !temporary) {
        return Error{ErrorCode::InvalidArgument,
                     "memory_sync.mode must be persistent or temporary"};
    }
    if (temporary && !isCanonicalSessionId(config.sessionId)) {
        return Error{ErrorCode::InvalidArgument,
                     "temporary memory sync requires a canonical session_id"};
    }

    storage::BackendConfig backendConfig;
    std::unique_ptr<storage::IStorageBackend> backend;

    if (config.backend == "filesystem") {
        if (config.path.empty()) {
            return Error{ErrorCode::InvalidArgument, "memory_sync.path is required"};
        }
        backendConfig.type = "filesystem";
        backendConfig.localPath = std::filesystem::path(config.path);
        if (temporary) {
            backendConfig.localPath /= ".sessions";
            backendConfig.localPath /= config.sessionId;
        }
        auto fs = std::make_unique<storage::FilesystemBackend>();
        if (auto r = fs->initialize(backendConfig); !r) {
            return r.error();
        }
        backend = std::move(fs);
    } else if (config.backend == "s3") {
        // Preserve credentials/endpoint policy from storage bootstrap while resolving the
        // destination bucket and prefix from memory_sync.path.
        auto resolved = resolveMemorySyncS3Config(config, resolvedS3Config);
        if (!resolved) {
            return resolved.error();
        }
        backendConfig = std::move(resolved.value());
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

    std::unique_ptr<storage::IStorageBackend> sessionMaintenanceBackend;
    std::string sessionLeaseKey;
    if (temporary && config.temporarySessionTtlMs != 0) {
        if (config.temporarySessionTtlMs < config.syncIntervalMs * 3ULL) {
            return Error{ErrorCode::InvalidArgument,
                         "temporary_session_ttl_ms must be at least three sync intervals"};
        }

        storage::BackendConfig maintenanceConfig;
        if (config.backend == "filesystem") {
            maintenanceConfig.type = "filesystem";
            maintenanceConfig.localPath = std::filesystem::path(config.path);
            auto fs = std::make_unique<storage::FilesystemBackend>();
            if (auto initialized = fs->initialize(maintenanceConfig); !initialized) {
                return initialized.error();
            }
            sessionMaintenanceBackend = std::move(fs);
        } else {
            MemorySyncDaemonConfig persistentRoot = config;
            persistentRoot.mode = "persistent";
            auto resolved = resolveMemorySyncS3Config(persistentRoot, resolvedS3Config);
            if (!resolved) {
                return resolved.error();
            }
            maintenanceConfig = std::move(resolved.value());
            sessionMaintenanceBackend = storage::StorageBackendFactory::create(maintenanceConfig);
            if (!sessionMaintenanceBackend) {
                return Error{ErrorCode::InvalidState,
                             "failed to create temporary-session maintenance backend"};
            }
            if (auto initialized = sessionMaintenanceBackend->initialize(maintenanceConfig);
                !initialized) {
                return initialized.error();
            }
        }

        const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
        const auto leases = sessionMaintenanceBackend->listPage(
            ".session-leases/", std::nullopt, config.limits.maxIndexObjectsPerSync);
        if (!leases) {
            return leases.error();
        }
        for (const auto& leaseKey : leases.value().keys) {
            constexpr std::string_view kLeasePrefix = ".session-leases/";
            if (!leaseKey.starts_with(kLeasePrefix)) {
                continue;
            }
            const std::string session = leaseKey.substr(kLeasePrefix.size());
            if (session == config.sessionId || !isCanonicalSessionId(session)) {
                continue;
            }
            const auto lease = sessionMaintenanceBackend->retrieve(leaseKey);
            if (!lease || lease.value().empty() || lease.value().size() > 32) {
                continue;
            }
            const std::string timestamp(reinterpret_cast<const char*>(lease.value().data()),
                                        lease.value().size());
            std::int64_t refreshedMs = 0;
            const auto [end, error] =
                std::from_chars(timestamp.data(), timestamp.data() + timestamp.size(), refreshedMs);
            if (error != std::errc{} || end != timestamp.data() + timestamp.size() ||
                refreshedMs < 0 || nowMs - refreshedMs <= config.temporarySessionTtlMs) {
                continue;
            }
            const std::string sessionPrefix = ".sessions/" + session + "/";
            const auto objects = sessionMaintenanceBackend->listPage(
                sessionPrefix, std::nullopt, config.limits.maxIndexObjectsPerSync);
            if (!objects || objects.value().nextCursor) {
                continue; // Never partially delete a session beyond the configured bound.
            }
            bool removedAll = true;
            for (const auto& object : objects.value().keys) {
                if (!sessionMaintenanceBackend->remove(object)) {
                    removedAll = false;
                    break;
                }
            }
            if (removedAll) {
                (void)sessionMaintenanceBackend->remove(leaseKey);
            }
        }
        sessionLeaseKey = ".session-leases/" + config.sessionId;
    }

    return std::make_unique<MemorySyncService>(
        std::move(backend),
        MemorySyncConfig{config.nodeId, config.syncIntervalMs, config.corpusId, config.corpusEpoch,
                         config.limits, temporary, std::move(writerAuth.value())},
        std::move(canAdmitRemoteWork), config.allowLegacyUnbound,
        std::move(sessionMaintenanceBackend), std::move(sessionLeaseKey));
}

} // namespace yams::memory_sync
