// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <span>
#include <string>
#include <string_view>

#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <yams/crypto/hasher.h>
#include <yams/memory_sync/memory_sync_service.h>
#include <yams/memory_sync/version_vector.h>
#include <yams/storage/storage_engine.h>

namespace yams::memory_sync {

/// Adapts content-addressed storage writes to the memory-sync blob namespace.
/// The adapter is deliberately narrow: callers inject it where an ingestion or
/// repair path owns the successful content-store write, avoiding a second
/// storage-engine interface and preserving existing storage configuration.
class ContentBlobSyncAdapter {
public:
    ContentBlobSyncAdapter(storage::IStorageEngine& contentStore, MemorySyncService& syncService)
        : storageEngine_(&contentStore), syncService_(syncService) {}

    ContentBlobSyncAdapter(api::IContentStore& contentStore, MemorySyncService& syncService)
        : apiContentStore_(&contentStore), syncService_(syncService) {}

    /// Persist locally, then publish the same bytes under `content-blob/<hash>`.
    /// A sync failure is returned so callers never mistake a local-only write for
    /// replicated content.
    Result<void> store(std::string_view hash, std::span<const std::byte> data) {
        auto key = blobKey(hash);
        if (!key) {
            return key.error();
        }
        if (!matchesDigest(hash, data)) {
            return Error{ErrorCode::HashMismatch, "content bytes do not match the claimed hash"};
        }
        auto stored = storeContent(hash, data);
        if (!stored) {
            return stored;
        }
        auto published = syncService_.publishIfChanged(key.value(), data);
        if (!published) {
            return published.error();
        }
        return {};
    }

    /// Publish a blob that was written by an existing content-store path.
    Result<bool> publishExisting(std::string_view hash) {
        auto key = blobKey(hash);
        if (!key) {
            return key.error();
        }
        auto content = retrieveContent(hash);
        if (!content) {
            return content.error();
        }
        if (!matchesDigest(hash, content.value())) {
            return Error{ErrorCode::HashMismatch,
                         "stored content bytes do not match the claimed hash"};
        }
        return syncService_.publishIfChanged(key.value(), content.value());
    }

    /// Publish a causal deletion after the owning content-store transaction commits.
    Result<void> publishDelete(std::string_view hash) {
        auto key = blobKey(hash);
        if (!key) {
            return key.error();
        }
        return syncService_.erase(key.value(), std::string(hash));
    }

    /// Remove a local blob selected by a winning tombstone. Returns whether bytes existed.
    Result<bool> applyDelete(std::string_view hash) {
        auto key = blobKey(hash);
        if (!key) {
            return key.error();
        }
        auto exists = contentExists(hash);
        if (!exists) {
            return exists.error();
        }
        if (!exists.value()) {
            return false;
        }
        if (storageEngine_ != nullptr) {
            if (auto removed = storageEngine_->remove(hash); !removed) {
                return removed.error();
            }
        } else {
            auto removed = apiContentStore_->remove(std::string(hash));
            if (!removed) {
                return removed.error();
            }
            if (!removed.value()) {
                return false;
            }
        }
        return true;
    }

    /// Pull and persist the blob only when it is absent from the local store.
    Result<void> apply(std::string_view hash) { return applyImpl(hash, false); }

    /// Apply a winner from the service's already-reconciled cache. Daemon apply
    /// paths use this variant to avoid one full backend listing per content blob.
    Result<void> applyCached(std::string_view hash) { return applyImpl(hash, true); }

private:
    Result<void> applyImpl(std::string_view hash, bool cachedOnly) {
        auto key = blobKey(hash);
        if (!key) {
            return key.error();
        }
        auto exists = contentExists(hash);
        if (!exists) {
            return exists.error();
        }
        if (exists.value()) {
            return {};
        }
        auto content =
            cachedOnly ? syncService_.readCached(key.value()) : syncService_.read(key.value());
        if (!content) {
            return content.error();
        }
        if (!matchesDigest(hash, content.value())) {
            return Error{ErrorCode::HashMismatch,
                         "replicated content bytes do not match the claimed hash"};
        }
        return storeContent(hash, content.value());
    }

    static bool matchesDigest(std::string_view expected, std::span<const std::byte> data) {
        crypto::SHA256Hasher hasher;
        hasher.init();
        hasher.update(data);
        return hasher.finalize() == expected;
    }

    static Result<std::string> blobKey(std::string_view hash) {
        if (!isSha256Digest(hash)) {
            return Error{ErrorCode::InvalidArgument,
                         "content blob sync requires a SHA-256 content hash"};
        }
        return "content-blob/" + std::string(hash);
    }

    Result<bool> contentExists(std::string_view hash) const {
        if (storageEngine_ != nullptr) {
            return storageEngine_->exists(hash);
        }
        return apiContentStore_->exists(std::string(hash));
    }

    Result<std::vector<std::byte>> retrieveContent(std::string_view hash) const {
        if (storageEngine_ != nullptr) {
            return storageEngine_->retrieve(hash);
        }
        return apiContentStore_->retrieveBytes(std::string(hash));
    }

    Result<void> storeContent(std::string_view hash, std::span<const std::byte> data) {
        if (storageEngine_ != nullptr) {
            return storageEngine_->store(hash, data);
        }
        auto stored = apiContentStore_->storeBytes(data);
        if (!stored) {
            return stored.error();
        }
        if (stored.value().contentHash != hash) {
            return Error{ErrorCode::HashMismatch,
                         "content store returned a different content hash"};
        }
        return {};
    }

    storage::IStorageEngine* storageEngine_{nullptr};
    api::IContentStore* apiContentStore_{nullptr};
    MemorySyncService& syncService_;
};

} // namespace yams::memory_sync
