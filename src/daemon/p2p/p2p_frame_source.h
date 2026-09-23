// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// Private framing seam for the direct-P2P handshake and delta state machines. Production code
// runs them over an authenticated P2pConnection; fuzz harnesses and focused tests replay a byte
// buffer of length-prefixed frames through BufferFrameSource without a TLS socket.

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_transport.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <span>
#include <string>
#include <vector>

namespace yams::daemon::p2p::detail {

/// Wire frame prefix: unsigned 32-bit big-endian payload length.
inline constexpr std::size_t kFrameLengthPrefixBytes = 4;

std::array<std::byte, kFrameLengthPrefixBytes> encodeLength(std::size_t length);
std::size_t decodeLength(const std::array<std::byte, kFrameLengthPrefixBytes>& header);

/// Append one length-prefixed frame (u32 BE length + payload) to `out`.
void appendFrame(std::vector<std::byte>& out, std::span<const std::byte> payload);

/// Framed duplex channel. Error semantics follow P2pConnection: NotFound on end of stream,
/// InvalidData (and the channel closes) on an oversized declared length, OperationCancelled once
/// closed.
class FrameSource {
public:
    FrameSource() = default;
    FrameSource(const FrameSource&) = delete;
    FrameSource& operator=(const FrameSource&) = delete;
    virtual ~FrameSource() = default;

    virtual Result<std::vector<std::byte>> readFrame(std::chrono::milliseconds timeout,
                                                     std::size_t maxFrameBytes) = 0;
    virtual Result<void> writeFrame(std::span<const std::byte> payload,
                                    std::chrono::milliseconds timeout,
                                    std::size_t maxFrameBytes) = 0;
    virtual void close() noexcept = 0;
};

/// Production adapter: forwards every call to the TLS channel.
class ConnectionFrameSource final : public FrameSource {
public:
    explicit ConnectionFrameSource(P2pConnection& connection) : connection_(connection) {}

    Result<std::vector<std::byte>> readFrame(std::chrono::milliseconds timeout,
                                             std::size_t maxFrameBytes) override {
        return connection_.readFrame(timeout, maxFrameBytes);
    }
    Result<void> writeFrame(std::span<const std::byte> payload, std::chrono::milliseconds timeout,
                            std::size_t maxFrameBytes) override {
        return connection_.writeFrame(payload, timeout, maxFrameBytes);
    }
    void close() noexcept override { connection_.close(); }

private:
    P2pConnection& connection_;
};

/// TLS-authenticated identities of one channel: the values the handshake and delta exchange
/// read from P2pConnection once the TLS handshake has completed.
struct ChannelIdentity {
    std::string localNodeId;
    std::string peerCertCn;
    std::string peerSpkiPin;
};

inline ChannelIdentity channelIdentity(const P2pConnection& connection) {
    return ChannelIdentity{.localNodeId = connection.localNodeId(),
                           .peerCertCn = connection.peerCertCn(),
                           .peerSpkiPin = connection.peerSpkiPin()};
}

/// In-memory FrameSource over a byte buffer of length-prefixed frames. Reads consume the buffer;
/// writes are recorded (as framed bytes) when `recordWrites` is set, otherwise only counted.
class BufferFrameSource final : public FrameSource {
public:
    explicit BufferFrameSource(std::span<const std::byte> input, bool recordWrites = false)
        : input_(input), recordWrites_(recordWrites) {}

    Result<std::vector<std::byte>> readFrame(std::chrono::milliseconds timeout,
                                             std::size_t maxFrameBytes) override;
    Result<void> writeFrame(std::span<const std::byte> payload, std::chrono::milliseconds timeout,
                            std::size_t maxFrameBytes) override;
    void close() noexcept override { closed_ = true; }

    [[nodiscard]] bool closed() const noexcept { return closed_; }
    [[nodiscard]] std::size_t consumedBytes() const noexcept { return offset_; }
    [[nodiscard]] std::size_t remainingBytes() const noexcept { return input_.size() - offset_; }
    [[nodiscard]] std::size_t framesRead() const noexcept { return framesRead_; }
    [[nodiscard]] std::size_t framesWritten() const noexcept { return framesWritten_; }
    [[nodiscard]] const std::vector<std::byte>& written() const noexcept { return written_; }

private:
    std::span<const std::byte> input_;
    std::size_t offset_{0};
    std::size_t framesRead_{0};
    std::size_t framesWritten_{0};
    std::vector<std::byte> written_;
    bool recordWrites_{false};
    bool closed_{false};
};

} // namespace yams::daemon::p2p::detail
