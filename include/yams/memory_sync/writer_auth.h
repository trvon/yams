// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <yams/core/types.h>
#include <yams/memory_sync/version_vector.h>

namespace yams::memory_sync {

struct TrustedWriterKey {
    NodeId writerId;
    std::string keyId;
    std::string publicKeyPem;
    bool revoked{false};
};

/// Authentication material is scoped by the caller to one corpus and epoch.
/// Private key bytes are never serialized into an envelope or surfaced by status APIs.
struct WriterKeyPair {
    std::string privateKeyPem;
    std::string publicKeyPem;
};

Result<WriterKeyPair> generateWriterKeyPair();

struct WriterAuthConfig {
    bool required{false};
    NodeId localWriterId;
    std::string localKeyId;
    std::string localPrivateKeyPem;
    std::vector<TrustedWriterKey> trustedKeys;
};

/// Ed25519 signer/verifier for canonical schema-v4 memory-sync envelopes.
/// The implementation is opaque so OpenSSL types do not leak into public headers.
class WriterAuthenticator {
public:
    static Result<std::shared_ptr<const WriterAuthenticator>>
    create(WriterAuthConfig config, std::string corpusId, std::uint64_t corpusEpoch);

    bool required() const noexcept;
    Result<void> sign(MemoryIndexRecord& record) const;
    bool verify(const MemoryIndexRecord& record) const noexcept;

private:
    struct Impl;
    explicit WriterAuthenticator(std::shared_ptr<Impl> impl);
    std::shared_ptr<Impl> impl_;
};

} // namespace yams::memory_sync
