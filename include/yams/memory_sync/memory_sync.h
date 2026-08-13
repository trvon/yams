// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/core/types.h>
#include <yams/crypto/hasher.h>
#include <yams/memory_sync/version_vector.h>
#include <yams/storage/storage_backend.h>

namespace yams::memory_sync {

/// Backend-agnostic memory sync loop. Memory records are published as
/// content-addressed blobs plus one version-vector-tagged index record per write;
/// `sync()` lists the index, pulls records, and LWW-merges to a deterministic
/// winner per logical key. The storage backend is injected, so filesystem and
/// S3/R2 are interchangeable.
class MemorySyncLoop {
public:
    MemorySyncLoop(storage::IStorageBackend& backend, NodeId nodeId)
        : backend_(&backend), nodeId_(std::move(nodeId)) {}

    /// Publish `content` under `logicalKey`. Stores the content-addressed blob and
    /// an index record stamped with this node's version vector and hybrid timestamp.
    Result<void> publish(std::string_view logicalKey, std::span<const std::byte> content) {
        const std::string hash = hashContent(content);

        if (auto r = backend_->store(blobKey(hash), content); !r) {
            return r.error();
        }

        version_.increment(nodeId_);
        ++logicalClock_;

        MemoryIndexRecord record;
        record.entryHash = hash;
        record.ts.physicalMs = nowMs();
        record.ts.logical = logicalClock_;
        record.ts.origin = nodeId_;
        record.version = version_;

        if (auto r = backend_->store(indexKey(logicalKey, hash), serialize(record)); !r) {
            return r.error();
        }
        return {};
    }

    /// Reconcile against the backend: list the index, pull every record, LWW-merge
    /// per logical key, and fold remote histories into this node's version vector.
    /// Returns the merged `logicalKey -> winning record` map.
    Result<std::map<std::string, MemoryIndexRecord>> sync() {
        auto listed = backend_->list("index/");
        if (!listed) {
            return listed.error();
        }

        std::map<std::string, MemoryIndexRecord> merged;
        for (const auto& key : listed.value()) {
            const std::string logicalKey = logicalKeyFromIndexKey(key);
            if (logicalKey.empty()) {
                continue;
            }
            auto fetched = backend_->retrieve(key);
            if (!fetched) {
                return fetched.error();
            }
            auto record = deserialize(fetched.value());
            if (!record) {
                return record.error();
            }
            version_.merge(record.value().version);

            const auto it = merged.find(logicalKey);
            if (it == merged.end() || lwwWins(record.value(), it->second)) {
                merged[logicalKey] = record.value();
            }
        }
        merged_ = merged;
        return merged_;
    }

    /// Read the winning content for `logicalKey`. Re-syncs to stay current.
    Result<std::vector<std::byte>> read(std::string_view logicalKey) {
        auto merged = sync();
        if (!merged) {
            return merged.error();
        }
        const auto& map = merged.value();
        const auto it = map.find(std::string(logicalKey));
        if (it == map.end()) {
            return Error{ErrorCode::NotFound, "no memory record for key"};
        }
        return backend_->retrieve(blobKey(it->second.entryHash));
    }

private:
    static std::uint64_t nowMs() {
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::system_clock::now().time_since_epoch())
                                              .count());
    }

    static std::string blobKey(std::string_view hash) {
        return std::string("blob/") + std::string(hash);
    }

    static std::string indexKey(std::string_view logicalKey, std::string_view hash) {
        return std::string("index/") + std::string(logicalKey) + "/" + std::string(hash);
    }

    static std::string logicalKeyFromIndexKey(std::string_view key) {
        constexpr std::string_view prefix = "index/";
        if (!key.starts_with(prefix)) {
            return {};
        }
        const auto rest = key.substr(prefix.size());
        const auto slash = rest.rfind('/');
        return slash == std::string_view::npos ? std::string(rest)
                                               : std::string(rest.substr(0, slash));
    }

    static std::string hashContent(std::span<const std::byte> content) {
        crypto::SHA256Hasher hasher;
        hasher.init();
        hasher.update(content);
        return hasher.finalize();
    }

    static std::vector<std::byte> serialize(const MemoryIndexRecord& record) {
        const std::string dump = nlohmann::json(record).dump();
        std::vector<std::byte> bytes(dump.size());
        std::memcpy(bytes.data(), dump.data(), dump.size());
        return bytes;
    }

    static Result<MemoryIndexRecord> deserialize(std::span<const std::byte> bytes) {
        try {
            const std::string_view text(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            return nlohmann::json::parse(text).get<MemoryIndexRecord>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::InvalidData, e.what()};
        }
    }

    storage::IStorageBackend* backend_;
    NodeId nodeId_;
    VersionVector version_;
    std::uint64_t logicalClock_{0};
    std::map<std::string, MemoryIndexRecord> merged_;
};

} // namespace yams::memory_sync
