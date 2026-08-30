// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors
#pragma once

// pi-lens-ignore: fatal error
#include <yams/core/types.h>
// pi-lens-ignore: fatal error
#include <yams/daemon/components/test_hooks.h>
#include <yams/daemon/p2p/peer_registry.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace yams::memory_sync {
class MemorySyncService;
}

namespace yams::daemon::p2p {

constexpr auto kP2pMaxReconnectInterval = std::chrono::minutes(5);

struct P2pConnectionSpec {
    std::string host;
    std::uint16_t port{0};
    std::optional<std::string> corpusId;
    std::optional<std::uint64_t> corpusEpoch;
    std::optional<std::string> peerPin;
    bool remember{true};

    [[nodiscard]] std::string endpoint() const;
};

/// Parse `host:port` or `yams://host:port` with optional corpus, epoch, pin,
/// and remember query parameters. Bracketed IPv6 is supported.
Result<P2pConnectionSpec> parseP2pConnectionString(std::string_view connectionString);

struct P2pManagerOptions {
    std::string nodeId;
    std::string corpusId;
    std::uint64_t corpusEpoch{0};
    std::string privateKeyPem;
    std::filesystem::path databasePath;
    std::string listenHost{"127.0.0.1"};
    std::uint16_t listenPort{0};
    bool allowFirstContact{false};
    std::size_t maxPeers{1024};
    std::chrono::milliseconds reconnectInterval{std::chrono::seconds(5)};
    std::chrono::milliseconds timeout{std::chrono::seconds(10)};
    std::chrono::milliseconds sessionTimeout{std::chrono::seconds(30)};
};

struct P2pLocalIdentity {
    std::string nodeId;
    std::string spkiPin;
};

struct P2pSyncResult {
    std::string peerNodeId;
    std::string peerPin;
    std::size_t deltasSent{0};
    std::size_t deltasReceived{0};
    std::size_t merged{0};
    std::size_t quarantined{0};
};

class P2pManager {
public:
    static Result<std::unique_ptr<P2pManager>> create(P2pManagerOptions options,
                                                      memory_sync::MemorySyncService& service);
    ~P2pManager();

    P2pManager(const P2pManager&) = delete;
    P2pManager& operator=(const P2pManager&) = delete;

    Result<void> start();
    void stop() noexcept;
    [[nodiscard]] bool started() const noexcept;
    [[nodiscard]] std::uint16_t boundPort() const noexcept;

    Result<P2pSyncResult> connect(std::string_view connectionString);
    Result<void> disconnect(std::string_view nodeId);
    Result<void> enrollPeer(std::string_view nodeId, std::string_view spkiPin);
    Result<void> forget(std::string_view nodeId);
    Result<P2pLocalIdentity> localIdentity() const;
    Result<std::vector<PeerRegistryRecord>> peers() const;

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
    /// Inject a reconnect-loop action for deterministic thread-boundary tests.
    YAMS_DAEMON_TEST_HOOK static void testing_setReconnectLoopHook(std::function<void()> hook);
#endif

private:
    class Impl;
    explicit P2pManager(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

} // namespace yams::daemon::p2p
