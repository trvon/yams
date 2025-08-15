#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace yams::cli::crypto {

/**
 * CLI crypto utilities for:
 * - Ed25519 key generation (to PEM strings or files)
 * - API key generation
 * - JWT generation (EdDSA/Ed25519)
 *
 * Notes:
 * - Implementations rely on OpenSSL 3.x (EVP_PKEY with Ed25519).
 * - All functions are synchronous and intended for CLI usage paths.
 * - File-writing utilities should ensure secure permissions for private keys (0600 best-effort).
 */

/**
 * Default number of random bytes used for API keys (hex-encoded -> 2x characters).
 */
inline constexpr std::size_t DEFAULT_API_KEY_BYTES = 32;

/**
 * JWT claim set for signing.
 * Required: issuer (iss), subject (sub), ttl for exp calculation.
 * Optional: audience (aud), jwt id (jti), not-before (nbf), issued-at (iat).
 */
struct JwtClaims {
    // Registered claims
    std::string issuer;                // iss
    std::string subject;               // sub
    std::vector<std::string> audience; // aud (multi-valued supported)

    // Timestamps
    std::chrono::system_clock::time_point issuedAt;           // iat (default: now if not set)
    std::optional<std::chrono::system_clock::time_point> nbf; // nbf
    std::chrono::seconds ttl{0};                              // used to compute exp
    std::optional<std::string> jti;                           // jti

    // Helper: returns computed exp (issuedAt + ttl)
    [[nodiscard]] std::chrono::system_clock::time_point expiresAt() const { return issuedAt + ttl; }
};

/**
 * Generate an Ed25519 keypair and return as PEM strings.
 *
 * @return pair {private_key_pem, public_key_pem} on success
 *         ErrorCode::InternalError on crypto failures
 */
[[nodiscard]] Result<std::pair<std::string, std::string>> generateEd25519KeypairPem();

/**
 * Generate an Ed25519 keypair and write to files.
 *
 * - privateKeyPath: PKCS#8 PEM (unencrypted)
 * - publicKeyPath: SubjectPublicKeyInfo PEM
 * - If overwrite == false and either file exists, returns ErrorCode::InvalidState
 * - On success, tries to set private key file permissions to 0600 (best-effort)
 *
 * @return Success or Error
 *         ErrorCode::InvalidState if files exist and overwrite==false
 *         ErrorCode::WriteError on I/O failures
 *         ErrorCode::InternalError on crypto failures
 */
[[nodiscard]] Result<void>
generateEd25519KeypairToFiles(const std::filesystem::path& privateKeyPath,
                              const std::filesystem::path& publicKeyPath, bool overwrite = false);

/**
 * Generate a cryptographically random API key and encode as lowercase hex.
 *
 * @param numBytes number of random bytes (DEFAULT_API_KEY_BYTES recommended)
 * @return Hex string length = 2*numBytes. If RNG fails, implementation must fall back to a secure
 * alternative.
 */
[[nodiscard]] std::string generateApiKeyHex(std::size_t numBytes = DEFAULT_API_KEY_BYTES);

/**
 * Sign a JWT using EdDSA (Ed25519).
 *
 * Header fields set by implementation:
 * - alg = "EdDSA"
 * - typ = "JWT"
 * - kid = provided key id (if not empty)
 *
 * Claims:
 * - from JwtClaims (iss, sub, aud[], iat, nbf?, exp computed as iat+ttl, jti?)
 *
 * @param privateKeyPem Ed25519 private key in PKCS#8 PEM (no passphrase)
 * @param kid           Key ID to place in JWT header (optional; empty -> omitted)
 * @param claims        Claim set (must include issuer, subject, ttl; iat defaults to now if unset)
 * @return Compact JWT string on success
 *         ErrorCode::InvalidArgument if inputs are malformed (e.g., empty issuer/subject or ttl==0)
 *         ErrorCode::InternalError on signing/encoding failures
 */
[[nodiscard]] Result<std::string> signJwtEd25519(const std::string& privateKeyPem,
                                                 const std::string& kid, JwtClaims claims);

/**
 * Validate an Ed25519 public key PEM.
 *
 * Useful for preflight checks when loading keys from disk or config.
 *
 * @param publicKeyPem SubjectPublicKeyInfo PEM
 * @return Success or Error
 *         ErrorCode::InvalidArgument if not a valid Ed25519 public key
 *         ErrorCode::InternalError on decoding/parsing failures
 */
[[nodiscard]] Result<void> validateEd25519PublicKeyPem(const std::string& publicKeyPem);

/**
 * Validate an Ed25519 private key PEM (PKCS#8, unencrypted).
 *
 * @param privateKeyPem PKCS#8 PEM
 * @return Success or Error
 *         ErrorCode::InvalidArgument if not a valid Ed25519 private key
 *         ErrorCode::InternalError on decoding/parsing failures
 */
[[nodiscard]] Result<void> validateEd25519PrivateKeyPem(const std::string& privateKeyPem);

} // namespace yams::cli::crypto