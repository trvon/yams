// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace yams::daemon::p2p {

/// Control-frame size cap (hello/state/delta envelopes) and value-payload cap.
constexpr std::size_t kP2pMaxControlFrameBytes = std::size_t{64} * 1024;
constexpr std::size_t kP2pMaxValuePayloadBytes = std::size_t{16} * 1024 * 1024;

/// Ed25519 TLS identity. Derives a self-signed X.509 certificate from the
/// existing schema-v4 writer keypair, so the memory-sync writer identity and
/// the transport identity are the same key. The certificate's SubjectPublicKeyInfo
/// SHA-256 (hex) is the TOFU pin exchanged and pinned per peer.
class TlsIdentity {
public:
    /// Build an identity from a writer private key PEM (Ed25519).
    static Result<TlsIdentity> fromPrivateKeyPem(std::string nodeId, std::string privateKeyPem);

    TlsIdentity(TlsIdentity&&) noexcept;
    TlsIdentity& operator=(TlsIdentity&&) noexcept;
    ~TlsIdentity();
    TlsIdentity(const TlsIdentity&) = delete;
    TlsIdentity& operator=(const TlsIdentity&) = delete;

    /// Stable writer node id this identity represents.
    std::string nodeId() const;
    /// PEM of the self-signed certificate.
    std::string certPem() const;
    /// Hex SHA-256 of the DER SubjectPublicKeyInfo (the TOFU pin).
    std::string spkiPin() const;
    /// Private key PEM for configuring the TLS context. Transport-internal;
    /// never serialized into frames or surfaced by daemon status.
    std::string_view privateKeyPemForTransport() const;

    class Impl;
    explicit TlsIdentity(std::shared_ptr<Impl> impl);

private:
    std::shared_ptr<Impl> impl_;
};

/// One mutually-authenticated P2P channel (client- or server-role). Provides
/// deadline-bounded framed reads/writes over TLS 1.3. Each channel owns its own
/// io_context, so it is used from one thread at a time and never shares state
/// with other channels or the daemon's global executor.
class P2pConnection {
public:
    P2pConnection(P2pConnection&&) noexcept;
    P2pConnection& operator=(P2pConnection&&) noexcept;
    ~P2pConnection();
    P2pConnection(const P2pConnection&) = delete;
    P2pConnection& operator=(const P2pConnection&) = delete;

    /// Write one length-prefixed frame (u32 BE length + payload), bounded by
    /// `maxFrameBytes` and `writeTimeout`.
    Result<void> writeFrame(std::span<const std::byte> payload,
                            std::chrono::milliseconds writeTimeout = std::chrono::seconds(5),
                            std::size_t maxFrameBytes = kP2pMaxControlFrameBytes);

    /// Read one frame. Returns ErrorCode::NotFound on clean EOF, timeout as
    /// ErrorCode::Timeout, and is interruptible by `close()` from another thread.
    Result<std::vector<std::byte>>
    readFrame(std::chrono::milliseconds readTimeout = std::chrono::seconds(5),
              std::size_t maxFrameBytes = kP2pMaxControlFrameBytes);

    /// Peer certificate's SPKI pin (hex), captured during handshake.
    std::string peerSpkiPin() const;
    /// Peer certificate CN (the peer's writer node id), captured during handshake.
    std::string peerCertCn() const;
    /// The peer identity this channel authenticated against (server-side: from
    /// the pinned client cert; client-side: from the pinned server cert).
    std::string peerNodeId() const;
    /// Our own node id.
    std::string localNodeId() const;

    /// Local socket address (useful for logs/tests).
    std::uint16_t localPort() const;

    /// Close the channel and interrupt any blocked read/write.
    void close() noexcept;

    class Impl;
    explicit P2pConnection(std::shared_ptr<Impl> impl);

private:
    std::shared_ptr<Impl> impl_;
};

/// Client-side connection establishment: TCP connect + TLS 1.3 mutual handshake
/// + peer pin verification, all deadline-bounded and blocking on the caller's
/// thread. `expectedPeerPin` enforces the TOFU pin. First contact without a
/// pin requires explicit `allowUnpinnedPeer=true`; default is fail closed.
struct P2pClientOptions {
    std::string host;
    std::uint16_t port{0};
    TlsIdentity identity;
    std::string expectedPeerPin;
    bool allowUnpinnedPeer{false}; // explicit TOFU first-contact opt-in
    std::chrono::milliseconds connectTimeout{std::chrono::seconds(5)};
    std::chrono::milliseconds handshakeTimeout{std::chrono::seconds(5)};
};

Result<P2pConnection> p2pConnect(P2pClientOptions options);

/// TLS listener. Accepts connections on its own thread, performs the TLS 1.3
/// mutual handshake with a deadline, enforces allowed peer pins (or explicit
/// first-contact TOFU opt-in), and dispatches each authenticated channel to `handler` on a
/// per-session thread (bounded by maxConcurrentSessions). `stop()` closes the acceptor and all
/// active sessions; session handlers are bounded by their read deadlines so shutdown joins
/// promptly.
class P2pListener {
public:
    struct Options {
        std::string host{"0.0.0.0"};
        std::uint16_t port{0}; // 0 = ephemeral; use boundPort() after start
        TlsIdentity identity;
        std::vector<std::string> allowedPeerPins;
        bool allowUnpinnedPeers{false}; // explicit TOFU first-contact opt-in
        std::chrono::milliseconds handshakeTimeout{std::chrono::seconds(5)};
        std::size_t maxConcurrentSessions{16};
    };

    using SessionHandler = std::function<void(P2pConnection channel)>;

    explicit P2pListener(Options options);
    ~P2pListener();

    P2pListener(const P2pListener&) = delete;
    P2pListener& operator=(const P2pListener&) = delete;

    /// Bind + start the accept loop. Idempotent; returns error on bind failure.
    Result<void> start();
    /// The bound port (valid after start(); useful with port=0).
    std::uint16_t boundPort() const;
    /// Stop accepting, close active sessions, join threads. Idempotent, noexcept.
    void stop() noexcept;
    bool started() const noexcept;

    /// Total connections accepted since start (for tests/telemetry).
    [[nodiscard]] std::uint64_t acceptedCount() const noexcept;
    /// Session worker threads retained for joining. Completed workers are reaped while running.
    [[nodiscard]] std::size_t retainedSessionCount() const noexcept;

    /// Add an operator-approved peer pin for subsequent inbound TLS sessions.
    void allowPeerPin(std::string spkiPin);

    /// Install the session handler (required before start()).
    void setHandler(SessionHandler handler);

    class Impl;
    explicit P2pListener(std::shared_ptr<Impl> impl);

private:
    std::shared_ptr<Impl> impl_;
};

/// Free helper: hex-encode bytes (used for pins).
std::string toHex(std::span<const std::byte> bytes);

} // namespace yams::daemon::p2p
