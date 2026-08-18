// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <yams/memory_sync/writer_auth.h>

#include <algorithm>
#include <array>
#include <map>
#include <span>
#include <string_view>
#include <utility>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

namespace yams::memory_sync {
namespace {

struct PkeyDeleter {
    void operator()(EVP_PKEY* key) const noexcept { EVP_PKEY_free(key); }
};
using Pkey = std::shared_ptr<EVP_PKEY>;

constexpr std::size_t kMaxPemBytes = std::size_t{64} * 1024;
constexpr std::size_t kMaxTrustedKeys = 8192;

Pkey readPublicKey(const std::string& pem) {
    if (pem.empty() || pem.size() > kMaxPemBytes) {
        return {};
    }
    BIO* rawBio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    if (rawBio == nullptr) {
        return {};
    }
    std::unique_ptr<BIO, decltype(&BIO_free)> bio(rawBio, BIO_free);
    return Pkey(PEM_read_bio_PUBKEY(bio.get(), nullptr, nullptr, nullptr), PkeyDeleter{});
}

int rejectEncryptedPem(char* buffer, int size, int readWrite, void* userData) {
    (void)buffer;
    (void)size;
    (void)readWrite;
    (void)userData;
    return 0;
}

Pkey readPrivateKey(const std::string& pem) {
    if (pem.empty() || pem.size() > kMaxPemBytes) {
        return {};
    }
    BIO* rawBio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    if (rawBio == nullptr) {
        return {};
    }
    std::unique_ptr<BIO, decltype(&BIO_free)> bio(rawBio, BIO_free);
    return Pkey(PEM_read_bio_PrivateKey(bio.get(), nullptr, rejectEncryptedPem, nullptr),
                PkeyDeleter{});
}

bool validKeyId(std::string_view value) {
    return !value.empty() && value.size() <= 128 &&
           std::ranges::all_of(value, [](unsigned char ch) {
               return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                      (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.';
           });
}

bool samePublicKey(EVP_PKEY* lhs, EVP_PKEY* rhs) {
    std::array<unsigned char, 32> left{};
    std::array<unsigned char, 32> right{};
    std::size_t leftSize = left.size();
    std::size_t rightSize = right.size();
    return EVP_PKEY_get_raw_public_key(lhs, left.data(), &leftSize) == 1 &&
           EVP_PKEY_get_raw_public_key(rhs, right.data(), &rightSize) == 1 &&
           leftSize == rightSize && std::equal(left.begin(), left.end(), right.begin());
}

std::string hexEncode(std::span<const unsigned char> bytes) {
    const auto hexDigit = [](unsigned char nibble) {
        return static_cast<char>(nibble < 10 ? '0' + nibble : 'a' + (nibble - 10));
    };
    std::string encoded;
    encoded.reserve(bytes.size() * 2);
    for (const auto byte : bytes) {
        encoded.push_back(hexDigit(byte >> 4));
        encoded.push_back(hexDigit(byte & 0x0f));
    }
    return encoded;
}

std::vector<unsigned char> hexDecode(std::string_view encoded) {
    if (encoded.size() % 2 != 0) {
        return {};
    }
    const auto nibble = [](unsigned char ch) -> int {
        if (ch >= '0' && ch <= '9') {
            return ch - '0';
        }
        if (ch >= 'a' && ch <= 'f') {
            return ch - 'a' + 10;
        }
        return -1;
    };
    std::vector<unsigned char> decoded;
    decoded.reserve(encoded.size() / 2);
    for (std::size_t i = 0; i < encoded.size(); i += 2) {
        const int high = nibble(static_cast<unsigned char>(encoded[i]));
        const int low = nibble(static_cast<unsigned char>(encoded[i + 1]));
        if (high < 0 || low < 0) {
            return {};
        }
        decoded.push_back(static_cast<unsigned char>((high << 4) | low));
    }
    return decoded;
}

} // namespace

struct WriterAuthenticator::Impl {
    struct KeyEntry {
        Pkey key;
        bool revoked{false};
    };

    std::string corpusId;
    std::uint64_t corpusEpoch{0};
    bool authRequired{false};
    NodeId localWriterId;
    std::string localKeyId;
    Pkey privateKey;
    std::map<std::pair<NodeId, std::string>, KeyEntry> trusted;
};

Result<WriterKeyPair> generateWriterKeyPair() {
    std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> context(
        EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519, nullptr), EVP_PKEY_CTX_free);
    if (!context || EVP_PKEY_keygen_init(context.get()) != 1) {
        return Error{ErrorCode::InternalError, "failed to initialize Ed25519 key generation"};
    }
    EVP_PKEY* rawKey = nullptr;
    if (EVP_PKEY_keygen(context.get(), &rawKey) != 1 || rawKey == nullptr) {
        return Error{ErrorCode::InternalError, "failed to generate Ed25519 key"};
    }
    Pkey key(rawKey, PkeyDeleter{});
    std::unique_ptr<BIO, decltype(&BIO_free)> privateBio(BIO_new(BIO_s_mem()), BIO_free);
    std::unique_ptr<BIO, decltype(&BIO_free)> publicBio(BIO_new(BIO_s_mem()), BIO_free);
    if (!privateBio || !publicBio ||
        PEM_write_bio_PrivateKey(privateBio.get(), key.get(), nullptr, nullptr, 0, nullptr,
                                 nullptr) != 1 ||
        PEM_write_bio_PUBKEY(publicBio.get(), key.get()) != 1) {
        return Error{ErrorCode::InternalError, "failed to encode Ed25519 key pair"};
    }
    BUF_MEM* privateBuffer = nullptr;
    BUF_MEM* publicBuffer = nullptr;
    (void)BIO_ctrl(privateBio.get(), BIO_C_GET_BUF_MEM_PTR, 0, static_cast<void*>(&privateBuffer));
    (void)BIO_ctrl(publicBio.get(), BIO_C_GET_BUF_MEM_PTR, 0, static_cast<void*>(&publicBuffer));
    if (privateBuffer == nullptr || publicBuffer == nullptr || privateBuffer->length == 0 ||
        publicBuffer->length == 0) {
        return Error{ErrorCode::InternalError, "generated Ed25519 key encoding is empty"};
    }
    return WriterKeyPair{.privateKeyPem = std::string(privateBuffer->data, privateBuffer->length),
                         .publicKeyPem = std::string(publicBuffer->data, publicBuffer->length)};
}

WriterAuthenticator::WriterAuthenticator(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

Result<std::shared_ptr<const WriterAuthenticator>>
WriterAuthenticator::create(WriterAuthConfig config, std::string corpusId,
                            std::uint64_t corpusEpoch) {
    if (corpusId.empty() || corpusEpoch == 0) {
        return Error{ErrorCode::InvalidArgument, "writer authentication requires corpus identity"};
    }
    if (config.trustedKeys.size() > kMaxTrustedKeys) {
        return Error{ErrorCode::InvalidArgument, "trusted writer key set exceeds hard limit"};
    }
    auto impl = std::make_shared<Impl>();
    impl->corpusId = std::move(corpusId);
    impl->corpusEpoch = corpusEpoch;
    impl->authRequired = config.required;

    for (const auto& trusted : config.trustedKeys) {
        if (trusted.writerId.empty() || !validKeyId(trusted.keyId) ||
            trusted.publicKeyPem.empty()) {
            return Error{ErrorCode::InvalidArgument, "trusted writer entry is incomplete"};
        }
        auto key = readPublicKey(trusted.publicKeyPem);
        if (!key || EVP_PKEY_base_id(key.get()) != EVP_PKEY_ED25519) {
            return Error{ErrorCode::InvalidArgument,
                         "trusted writer key is not a valid Ed25519 public key"};
        }
        if (!impl->trusted
                 .emplace(std::pair{trusted.writerId, trusted.keyId},
                          Impl::KeyEntry{.key = std::move(key), .revoked = trusted.revoked})
                 .second) {
            return Error{ErrorCode::InvalidArgument, "duplicate trusted writer and key identifier"};
        }
    }

    if (!config.localPrivateKeyPem.empty()) {
        if (config.localWriterId.empty() || !validKeyId(config.localKeyId)) {
            return Error{ErrorCode::InvalidArgument, "local signing identity is incomplete"};
        }
        auto privateKey = readPrivateKey(config.localPrivateKeyPem);
        if (!privateKey || EVP_PKEY_base_id(privateKey.get()) != EVP_PKEY_ED25519) {
            return Error{ErrorCode::InvalidArgument,
                         "local writer key is not a valid Ed25519 private key"};
        }
        const auto trusted = impl->trusted.find({config.localWriterId, config.localKeyId});
        if (trusted == impl->trusted.end() || trusted->second.revoked ||
            !samePublicKey(privateKey.get(), trusted->second.key.get())) {
            return Error{ErrorCode::InvalidArgument,
                         "local signing key is not an active trusted writer key"};
        }
        impl->localWriterId = std::move(config.localWriterId);
        impl->localKeyId = std::move(config.localKeyId);
        impl->privateKey = std::move(privateKey);
    }
    if (impl->authRequired && (!impl->privateKey || impl->trusted.empty())) {
        return Error{ErrorCode::InvalidArgument,
                     "required writer authentication needs local and trusted keys"};
    }
    return std::shared_ptr<const WriterAuthenticator>{new WriterAuthenticator(std::move(impl))};
}

bool WriterAuthenticator::required() const noexcept {
    return impl_->authRequired;
}

Result<void> WriterAuthenticator::sign(MemoryIndexRecord& record) const {
    if (!impl_->privateKey || record.origin != impl_->localWriterId ||
        record.corpusId != impl_->corpusId || record.corpusEpoch != impl_->corpusEpoch) {
        return Error{ErrorCode::InvalidState,
                     "writer signing identity does not match the configured corpus"};
    }
    record.schemaVersion = kAuthenticatedMemoryIndexSchemaVersion;
    record.signatureAlgorithm = std::string(kMemoryIndexSignatureAlgorithm);
    record.signingKeyId = impl_->localKeyId;
    record.signature.clear();
    if (!record.hasValidIdentity(impl_->corpusId, impl_->corpusEpoch, record.logicalKey)) {
        return Error{ErrorCode::InvalidData, "cannot sign an invalid memory envelope"};
    }

    const std::string canonical = canonicalEnvelopeBytes(record);
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(EVP_MD_CTX_new(),
                                                                    EVP_MD_CTX_free);
    if (!context || EVP_DigestSignInit(context.get(), nullptr, nullptr, nullptr,
                                       impl_->privateKey.get()) != 1) {
        return Error{ErrorCode::InternalError, "failed to initialize envelope signing"};
    }
    std::size_t signatureSize = 0;
    if (EVP_DigestSign(context.get(), nullptr, &signatureSize,
                       reinterpret_cast<const unsigned char*>(canonical.data()),
                       canonical.size()) != 1 ||
        signatureSize != 64) {
        return Error{ErrorCode::InternalError, "failed to size Ed25519 signature"};
    }
    std::vector<unsigned char> signature(signatureSize);
    if (EVP_DigestSign(context.get(), signature.data(), &signatureSize,
                       reinterpret_cast<const unsigned char*>(canonical.data()),
                       canonical.size()) != 1) {
        return Error{ErrorCode::InternalError, "failed to sign memory envelope"};
    }
    signature.resize(signatureSize);
    record.signature = hexEncode(signature);
    return {};
}

bool WriterAuthenticator::verify(const MemoryIndexRecord& record) const noexcept {
    try {
        if (record.schemaVersion != kAuthenticatedMemoryIndexSchemaVersion ||
            record.signatureAlgorithm != kMemoryIndexSignatureAlgorithm ||
            record.corpusId != impl_->corpusId || record.corpusEpoch != impl_->corpusEpoch ||
            !record.hasValidIdentity(impl_->corpusId, impl_->corpusEpoch, record.logicalKey)) {
            return false;
        }
        const auto trusted = impl_->trusted.find({record.origin, record.signingKeyId});
        if (trusted == impl_->trusted.end() || trusted->second.revoked) {
            return false;
        }
        const auto signature = hexDecode(record.signature);
        if (signature.size() != 64) {
            return false;
        }
        const std::string canonical = canonicalEnvelopeBytes(record);
        std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(EVP_MD_CTX_new(),
                                                                        EVP_MD_CTX_free);
        return context &&
               EVP_DigestVerifyInit(context.get(), nullptr, nullptr, nullptr,
                                    trusted->second.key.get()) == 1 &&
               EVP_DigestVerify(context.get(), signature.data(), signature.size(),
                                reinterpret_cast<const unsigned char*>(canonical.data()),
                                canonical.size()) == 1;
    } catch (...) {
        return false;
    }
}

} // namespace yams::memory_sync
