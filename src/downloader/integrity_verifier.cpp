/*
 * yams/src/downloader/integrity_verifier.cpp
 *
 * IntegrityVerifier MVP (SHA-256 via OpenSSL EVP)
 *
 * Implements yams::downloader::IIntegrityVerifier using OpenSSL's EVP interface.
 * - MVP supports SHA-256 only (HashAlgo::Sha256). Other algos are accepted by
 *   reset() but will fall back to SHA-256 for now.
 * - update() accepts byte spans and feeds them to the active digest context.
 * - finalize() returns a Checksum { algo, hex } and resets the internal context.
 *
 * Dependencies:
 * - OpenSSL::Crypto (linked by CMake in yams_downloader target)
 * - C++20 (std::span)
 */

#include <yams/downloader/downloader.hpp>

#include <openssl/evp.h>
#include <openssl/sha.h>

#include <array>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace yams::downloader {

namespace {

// Simple RAII wrapper for EVP_MD_CTX
struct EvpMdCtx {
    EVP_MD_CTX* ctx{nullptr};
    EvpMdCtx() : ctx(EVP_MD_CTX_new()) {}
    ~EvpMdCtx() {
        if (ctx)
            EVP_MD_CTX_free(ctx);
    }
    EvpMdCtx(const EvpMdCtx&) = delete;
    EvpMdCtx& operator=(const EvpMdCtx&) = delete;
    EvpMdCtx(EvpMdCtx&& other) noexcept : ctx(other.ctx) { other.ctx = nullptr; }
    EvpMdCtx& operator=(EvpMdCtx&& other) noexcept {
        if (this != &other) {
            if (ctx)
                EVP_MD_CTX_free(ctx);
            ctx = other.ctx;
            other.ctx = nullptr;
        }
        return *this;
    }
    explicit operator bool() const noexcept { return ctx != nullptr; }
};

inline const EVP_MD* resolve_algo(HashAlgo algo) {
    switch (algo) {
        case HashAlgo::Sha256:
            return EVP_sha256();
        case HashAlgo::Sha512:
            return EVP_sha512(); // not used in MVP; fallback below
        case HashAlgo::Md5:
            return EVP_md5(); // not used in MVP; fallback below
        default:
            return EVP_sha256();
    }
}

inline std::string to_hex_lower(const unsigned char* bytes, std::size_t len) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.resize(len * 2);
    for (std::size_t i = 0; i < len; ++i) {
        unsigned v = bytes[i];
        out[2 * i + 0] = kHex[(v >> 4) & 0xF];
        out[2 * i + 1] = kHex[(v >> 0) & 0xF];
    }
    return out;
}

class OpenSslIntegrityVerifier final : public IIntegrityVerifier {
public:
    OpenSslIntegrityVerifier() {
        // Default to SHA-256
        reset(HashAlgo::Sha256);
    }

    ~OpenSslIntegrityVerifier() override = default;

    void reset([[maybe_unused]] HashAlgo algo) override {
        // MVP: Only SHA-256 guaranteed. If not Sha256, still initialize with Sha256.
        _algo = HashAlgo::Sha256;

        _md = resolve_algo(_algo);
        _ctx = EvpMdCtx{};
        if (!_ctx || !_md) {
            // In a full implementation, throw or convert to a Result-style API.
            // Here we leave ctx null; update/finalize will be no-ops (empty checksum).
            return;
        }
        if (EVP_DigestInit_ex(_ctx.ctx, _md, nullptr) != 1) {
            // Initialization failed; mark ctx invalid
            _ctx = EvpMdCtx{};
        }
        _finalized = false;
    }

    void update(std::span<const std::byte> data) override {
        if (!_ctx || _finalized || data.empty())
            return;
        (void)EVP_DigestUpdate(_ctx.ctx, data.data(), data.size());
    }

    Checksum finalize() override {
        Checksum out;
        out.algo = _algo;

        if (!_ctx || _finalized) {
            // Return an empty hex string if not initialized or already finalized.
            out.hex.clear();
            return out;
        }

        std::array<unsigned char, EVP_MAX_MD_SIZE> md_buf{};
        unsigned md_len = 0;

        if (EVP_DigestFinal_ex(_ctx.ctx, md_buf.data(), &md_len) != 1) {
            // Failure: return empty hex
            out.hex.clear();
            _finalized = true;
            return out;
        }

        // For SHA-256, md_len should be 32
        out.hex = to_hex_lower(md_buf.data(), md_len);
        _finalized = true;

        // Prepare for potential reuse: re-init with same algo
        reset(_algo);
        return out;
    }

private:
    HashAlgo _algo{HashAlgo::Sha256};
    const EVP_MD* _md{nullptr};
    EvpMdCtx _ctx{};
    bool _finalized{false};
};

} // namespace

// Optional factory (consistent with other components' style)
std::unique_ptr<IIntegrityVerifier> makeIntegrityVerifierSha256Only() {
    return std::make_unique<OpenSslIntegrityVerifier>();
}

} // namespace yams::downloader