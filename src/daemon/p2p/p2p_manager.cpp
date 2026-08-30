// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#define YAMS_DAEMON_TEST_HOOKS_IMPL 1
// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_manager.h>
#undef YAMS_DAEMON_TEST_HOOKS_IMPL

// pi-lens-ignore: fatal error
#include <yams/daemon/p2p/p2p_delta.h>
#include <yams/daemon/p2p/p2p_transport.h>
// pi-lens-ignore: fatal error
#include <yams/memory_sync/memory_sync_config.h>
// pi-lens-ignore: fatal error
#include <yams/memory_sync/memory_sync_service.h>

#include <spdlog/spdlog.h>

#include <atomic>
#include <charconv>
#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <utility>

namespace yams::daemon::p2p {
namespace {

template <typename T> Result<T> parseUnsigned(std::string_view value, std::string_view label) {
    T parsed{};
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (value.empty() || error != std::errc{} || end != value.data() + value.size()) {
        return Error{ErrorCode::InvalidArgument, "invalid P2P connection " + std::string(label)};
    }
    return parsed;
}

Result<bool> parseRemember(std::string_view value) {
    if (value == "true" || value == "1") {
        return true;
    }
    if (value == "false" || value == "0") {
        return false;
    }
    return Error{ErrorCode::InvalidArgument, "invalid P2P connection remember value"};
}

std::int64_t unixTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
std::mutex gReconnectLoopHookMutex;
std::function<void()> gReconnectLoopHook;

void invokeReconnectLoopHook() {
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(gReconnectLoopHookMutex);
        hook = gReconnectLoopHook;
    }
    if (hook) {
        hook();
    }
}
#endif

} // namespace

std::string P2pConnectionSpec::endpoint() const {
    const bool bracket = host.find(':') != std::string::npos;
    return (bracket ? "[" + host + "]" : host) + ":" + std::to_string(port);
}

Result<P2pConnectionSpec> parseP2pConnectionString(std::string_view connectionString) {
    constexpr std::string_view scheme = "yams://";
    if (connectionString.starts_with(scheme)) {
        connectionString.remove_prefix(scheme.size());
    }
    if (connectionString.empty() || connectionString.find('#') != std::string_view::npos ||
        connectionString.find('@') != std::string_view::npos ||
        connectionString.find('/') != std::string_view::npos) {
        return Error{ErrorCode::InvalidArgument, "invalid P2P connection string"};
    }

    const auto queryAt = connectionString.find('?');
    const auto authority = connectionString.substr(0, queryAt);
    const auto query = queryAt == std::string_view::npos ? std::string_view{}
                                                         : connectionString.substr(queryAt + 1);

    P2pConnectionSpec spec;
    std::string_view portText;
    if (authority.starts_with('[')) {
        const auto close = authority.find(']');
        if (close == std::string_view::npos || close == 1 || close + 1 >= authority.size() ||
            authority[close + 1] != ':') {
            return Error{ErrorCode::InvalidArgument, "invalid bracketed P2P address"};
        }
        spec.host = authority.substr(1, close - 1);
        portText = authority.substr(close + 2);
    } else {
        const auto colon = authority.rfind(':');
        if (colon == std::string_view::npos || colon == 0 || colon + 1 >= authority.size() ||
            authority.find(':') != colon) {
            return Error{ErrorCode::InvalidArgument,
                         "P2P address must be host:port; bracket IPv6 addresses"};
        }
        spec.host = authority.substr(0, colon);
        portText = authority.substr(colon + 1);
    }
    auto port = parseUnsigned<std::uint32_t>(portText, "port");
    if (!port || port.value() == 0 || port.value() > 65535) {
        return Error{ErrorCode::InvalidArgument, "P2P port must be between 1 and 65535"};
    }
    spec.port = static_cast<std::uint16_t>(port.value());

    std::set<std::string> seen;
    std::size_t offset = 0;
    while (offset < query.size()) {
        const auto separator = query.find('&', offset);
        const auto pair =
            query.substr(offset, separator == std::string_view::npos ? query.size() - offset
                                                                     : separator - offset);
        const auto equals = pair.find('=');
        if (equals == std::string_view::npos || equals == 0 || equals + 1 >= pair.size()) {
            return Error{ErrorCode::InvalidArgument, "invalid P2P connection query"};
        }
        const std::string key(pair.substr(0, equals));
        const auto value = pair.substr(equals + 1);
        if (!seen.insert(key).second) {
            return Error{ErrorCode::InvalidArgument, "duplicate P2P connection parameter: " + key};
        }
        if (key == "corpus") {
            if (!memory_sync::isCanonicalCorpusId(value)) {
                return Error{ErrorCode::InvalidArgument, "invalid P2P corpus id"};
            }
            spec.corpusId = std::string(value);
        } else if (key == "epoch") {
            auto epoch = parseUnsigned<std::uint64_t>(value, "epoch");
            if (!epoch || epoch.value() == 0) {
                return Error{ErrorCode::InvalidArgument, "P2P epoch must be positive"};
            }
            spec.corpusEpoch = epoch.value();
        } else if (key == "pin") {
            auto pin = normalizePeerSpkiPin(value);
            if (!pin) {
                return pin.error();
            }
            spec.peerPin = std::move(pin.value());
        } else if (key == "remember") {
            auto remember = parseRemember(value);
            if (!remember) {
                return remember.error();
            }
            spec.remember = remember.value();
        } else {
            return Error{ErrorCode::InvalidArgument, "unknown P2P connection parameter: " + key};
        }
        if (separator == std::string_view::npos) {
            break;
        }
        offset = separator + 1;
        if (offset == query.size()) {
            return Error{ErrorCode::InvalidArgument, "invalid trailing P2P query separator"};
        }
    }
    return spec;
}

class P2pManager::Impl {
public:
    Impl(P2pManagerOptions options, memory_sync::MemorySyncService& service,
         std::unique_ptr<PeerRegistry> registry)
        : options_(std::move(options)), service_(service), registry_(std::move(registry)) {}

    ~Impl() { stop(); }

    Result<void> start() {
        try {
            return startImpl();
        } catch (const std::exception& error) {
            stop();
            return Error{ErrorCode::InternalError,
                         std::string("p2p manager start threw: ") + error.what()};
        } catch (...) {
            stop();
            return Error{ErrorCode::InternalError, "p2p manager start threw an unknown exception"};
        }
    }

    Result<void> startImpl() {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        if (started_) {
            return Result<void>();
        }
        auto identity = TlsIdentity::fromPrivateKeyPem(options_.nodeId, options_.privateKeyPem);
        if (!identity) {
            return identity.error();
        }
        auto known = registry_->listPeers();
        if (!known) {
            return known.error();
        }
        std::vector<std::string> allowedPins;
        allowedPins.reserve(known.value().size());
        for (const auto& peer : known.value()) {
            allowedPins.push_back(peer.spkiPin);
        }
        listener_ = std::make_unique<P2pListener>(
            P2pListener::Options{.host = options_.listenHost,
                                 .port = options_.listenPort,
                                 .identity = std::move(identity.value()),
                                 .allowedPeerPins = std::move(allowedPins),
                                 // Unknown certificates reach the application handshake only under
                                 // the explicit legacy first-contact compatibility policy.
                                 .allowUnpinnedPeers = options_.allowFirstContact,
                                 .handshakeTimeout = options_.timeout,
                                 .sessionTimeout = options_.sessionTimeout,
                                 .maxConcurrentHandshakes = 32,
                                 .maxConcurrentSessions = 16});
        listener_->setHandler(
            [this](P2pConnection connection) { handleInbound(std::move(connection)); });
        auto listening = listener_->start();
        if (!listening) {
            listener_.reset();
            return listening.error();
        }
        stopRequested_ = false;
        started_ = true;
        reconnectThread_ = std::thread([this] {
            try {
                reconnectLoop();
            } catch (const std::exception& error) {
                spdlog::error("[p2p] reconnect thread contained exception: {}", error.what());
            } catch (...) {
                spdlog::error("[p2p] reconnect thread contained unknown exception");
            }
        });
        return Result<void>();
    }

    void stop() noexcept {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        if (!started_ && !listener_ && !reconnectThread_.joinable()) {
            return;
        }
        stopRequested_ = true;
        reconnectCv_.notify_all();
        if (listener_) {
            listener_->stop();
        }
        if (reconnectThread_.joinable()) {
            reconnectThread_.join();
        }
        listener_.reset();
        started_ = false;
    }

    Result<P2pSyncResult> connect(std::string_view connectionString) {
        auto spec = parseP2pConnectionString(connectionString);
        if (!spec) {
            return spec.error();
        }
        return connectSpec(spec.value(), std::nullopt, std::nullopt);
    }

    Result<void> disconnect(std::string_view nodeId) {
        auto records = registry_->listPeers();
        if (!records) {
            return records.error();
        }
        for (const auto& peer : records.value()) {
            if (peer.nodeId == nodeId) {
                if (!peer.remembered) {
                    return {};
                }
                return registry_->updatePeerState(peer.nodeId, peer.corpusId, peer.corpusEpoch,
                                                  peer.lastSeenVersion, peer.lastConnectedMs,
                                                  peer.endpoint, false);
            }
        }
        return Error{ErrorCode::NotFound, "P2P peer is not registered"};
    }

    Result<void> enrollPeer(std::string_view nodeId, std::string_view spkiPin) {
        auto normalized = normalizePeerSpkiPin(spkiPin);
        if (!normalized) {
            return normalized.error();
        }
        if (auto enrolled = registry_->enrollOperatorPeer(nodeId, normalized.value()); !enrolled) {
            return enrolled.error();
        }
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        if (listener_) {
            listener_->allowPeerPin(normalized.value());
        }
        return {};
    }

    Result<void> forget(std::string_view nodeId) { return registry_->removePeer(nodeId); }

    Result<P2pLocalIdentity> localIdentity() const {
        auto identity = TlsIdentity::fromPrivateKeyPem(options_.nodeId, options_.privateKeyPem);
        if (!identity) {
            return identity.error();
        }
        return P2pLocalIdentity{.nodeId = options_.nodeId, .spkiPin = identity.value().spkiPin()};
    }

    Result<std::vector<PeerRegistryRecord>> peers() const { return registry_->listPeers(); }

    bool started() const noexcept { return started_.load(); }

    std::uint16_t boundPort() const noexcept {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex_);
        return listener_ ? listener_->boundPort() : 0;
    }

private:
    PeerHandshakeConfig handshakeConfig() const {
        const auto state = service_.replicationState();
        return PeerHandshakeConfig{.nodeId = options_.nodeId,
                                   .corpusId = options_.corpusId,
                                   .corpusEpoch = options_.corpusEpoch,
                                   .localVersion = state.version,
                                   .localCommitments = state.commitments,
                                   .localQuarantinedWriters = state.quarantinedWriters,
                                   .resolveLocalCommitment =
                                       [this](std::uint64_t counter) {
                                           return service_.localHistoryCommitmentAt(counter);
                                       },
                                   .resolveLocalWindow =
                                       [this](std::uint64_t peerCounter, std::size_t maxRecords,
                                              std::size_t maxWireBytes) {
                                           return service_.localHistoryWindowAfter(
                                               peerCounter, maxRecords, maxWireBytes);
                                       },
                                   .allowFirstContact = options_.allowFirstContact,
                                   .timeout = options_.timeout};
    }

    Result<void> enforcePeerHistory(const PeerHandshakeResult& peer) {
        const auto localState = service_.replicationState();
        if (localState.quarantinedWriters.contains(peer.peerNodeId)) {
            return Error{ErrorCode::InvalidData, "peer writer is durably quarantined"};
        }
        auto mismatch = requiresPeerWriterQuarantine(localState, peer);
        if (!mismatch) {
            return mismatch.error();
        }
        if (!mismatch.value()) {
            return {};
        }
        auto quarantined = service_.quarantineWriter(peer.peerNodeId, options_.nodeId);
        if (!quarantined) {
            return quarantined.error();
        }
        return Error{ErrorCode::InvalidData,
                     "authenticated peer writer history commitment mismatch"};
    }

    Result<P2pSyncResult> connectSpec(const P2pConnectionSpec& spec,
                                      const std::optional<std::string>& expectedNode,
                                      const std::optional<std::string>& expectedPin) {
        if (spec.corpusId && *spec.corpusId != options_.corpusId) {
            return Error{ErrorCode::InvalidArgument, "P2P connection corpus differs from daemon"};
        }
        if (spec.corpusEpoch && *spec.corpusEpoch != options_.corpusEpoch) {
            return Error{ErrorCode::InvalidArgument, "P2P connection epoch differs from daemon"};
        }
        auto identity = TlsIdentity::fromPrivateKeyPem(options_.nodeId, options_.privateKeyPem);
        if (!identity) {
            return identity.error();
        }
        const std::string pin = spec.peerPin.value_or(expectedPin.value_or(std::string{}));
        auto connection = p2pConnect(
            P2pClientOptions{.host = spec.host,
                             .port = spec.port,
                             .identity = std::move(identity.value()),
                             .expectedPeerPin = pin,
                             .allowUnpinnedPeer = pin.empty() && options_.allowFirstContact,
                             .connectTimeout = options_.timeout,
                             .handshakeTimeout = options_.timeout,
                             .sessionTimeout = options_.sessionTimeout});
        if (!connection) {
            return connection.error();
        }
        auto config = handshakeConfig();
        config.allowFirstContact = !expectedNode.has_value() && options_.allowFirstContact;
        auto handshake = initiatePeerHandshake(connection.value(), config, *registry_);
        if (!handshake) {
            return handshake.error();
        }
        if (expectedNode && handshake.value().peerNodeId != *expectedNode) {
            return Error{ErrorCode::Unauthorized, "P2P endpoint identity changed"};
        }
        if (auto history = enforcePeerHistory(handshake.value()); !history) {
            return history.error();
        }
        auto exchanged = initiateDeltaExchange(connection.value(), service_, handshake.value(),
                                               DeltaExchangeOptions{.maxDeltasPerBatch = 128,
                                                                    .maxBatches = 4096,
                                                                    .timeout = options_.timeout});
        if (!exchanged) {
            return exchanged.error();
        }
        if (auto history = enforcePeerHistory(handshake.value()); !history) {
            return history.error();
        }
        auto persisted = registry_->updatePeerState(handshake.value().peerNodeId, options_.corpusId,
                                                    options_.corpusEpoch, service_.currentVersion(),
                                                    unixTimeMs(), spec.endpoint(), spec.remember);
        if (!persisted) {
            return persisted.error();
        }
        return P2pSyncResult{.peerNodeId = handshake.value().peerNodeId,
                             .peerPin = handshake.value().peerSpkiPin,
                             .deltasSent = exchanged.value().deltasSent,
                             .deltasReceived = exchanged.value().deltasReceived,
                             .merged = exchanged.value().merged,
                             .quarantined = exchanged.value().quarantined};
    }

    void handleInbound(P2pConnection connection) {
        auto handshake = acceptPeerHandshake(connection, handshakeConfig(), *registry_);
        if (!handshake) {
            return;
        }
        if (auto history = enforcePeerHistory(handshake.value()); !history) {
            return;
        }
        auto exchanged = acceptDeltaExchange(connection, service_, handshake.value(),
                                             DeltaExchangeOptions{.maxDeltasPerBatch = 128,
                                                                  .maxBatches = 4096,
                                                                  .timeout = options_.timeout});
        if (!exchanged) {
            return;
        }
        if (auto history = enforcePeerHistory(handshake.value()); !history) {
            return;
        }
        std::string endpoint;
        bool remembered = false;
        auto records = registry_->listPeers();
        if (records) {
            for (const auto& peer : records.value()) {
                if (peer.nodeId == handshake.value().peerNodeId) {
                    endpoint = peer.endpoint;
                    remembered = peer.remembered;
                    break;
                }
            }
        }
        (void)registry_->updatePeerState(handshake.value().peerNodeId, options_.corpusId,
                                         options_.corpusEpoch, service_.currentVersion(),
                                         unixTimeMs(), endpoint, remembered);
    }

    void reconnectLoop() {
        struct RetryState {
            unsigned failures{0};
            std::chrono::steady_clock::time_point nextAttempt;
        };
        std::unordered_map<std::string, RetryState> retries;
        std::unique_lock<std::mutex> waitLock(reconnectMutex_);
        while (!stopRequested_) {
            if (reconnectCv_.wait_for(waitLock, options_.reconnectInterval,
                                      [this] { return stopRequested_.load(); })) {
                break;
            }
            waitLock.unlock();
            try {
#if YAMS_DAEMON_TEST_HOOKS_ENABLED
                invokeReconnectLoopHook();
#endif
                auto records = registry_->listPeers();
                if (records) {
                    for (const auto& peer : records.value()) {
                        if (stopRequested_ || !peer.remembered || peer.endpoint.empty()) {
                            continue;
                        }
                        auto& retry = retries[peer.nodeId];
                        const auto now = std::chrono::steady_clock::now();
                        if (retry.nextAttempt > now) {
                            continue;
                        }
                        auto spec = parseP2pConnectionString(peer.endpoint);
                        if (!spec) {
                            continue;
                        }
                        spec.value().remember = true;
                        auto connected = connectSpec(spec.value(), peer.nodeId, peer.spkiPin);
                        if (connected) {
                            retries.erase(peer.nodeId);
                            continue;
                        }
                        retry.failures = std::min(retry.failures + 1U, 6U);
                        const auto multiplier = std::int64_t{1} << retry.failures;
                        const std::chrono::milliseconds delay{options_.reconnectInterval.count() *
                                                              multiplier};
                        const auto jitterWindow =
                            std::max<std::int64_t>(1, options_.reconnectInterval.count() / 4);
                        const std::chrono::milliseconds jitter{
                            static_cast<std::int64_t>(std::hash<std::string>{}(peer.nodeId) %
                                                      static_cast<std::size_t>(jitterWindow))};
                        retry.nextAttempt =
                            now + std::min(delay + jitter,
                                           std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::minutes(5)));
                    }
                }
            } catch (const std::exception& error) {
                spdlog::warn("[p2p] reconnect iteration contained exception: {}", error.what());
            } catch (...) {
                spdlog::warn("[p2p] reconnect iteration contained unknown exception");
            }
            waitLock.lock();
        }
    }

    P2pManagerOptions options_;
    memory_sync::MemorySyncService& service_;
    std::unique_ptr<PeerRegistry> registry_;
    mutable std::mutex lifecycleMutex_;
    std::mutex reconnectMutex_;
    std::condition_variable reconnectCv_;
    std::unique_ptr<P2pListener> listener_;
    std::thread reconnectThread_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stopRequested_{false};
};

Result<std::unique_ptr<P2pManager>> P2pManager::create(P2pManagerOptions options,
                                                       memory_sync::MemorySyncService& service) {
    if (options.nodeId.empty() || !memory_sync::isCanonicalCorpusId(options.corpusId) ||
        options.corpusEpoch == 0 || options.privateKeyPem.empty() || options.databasePath.empty() ||
        options.reconnectInterval <= std::chrono::milliseconds::zero() ||
        options.reconnectInterval > kP2pMaxReconnectInterval ||
        options.timeout <= std::chrono::milliseconds::zero() ||
        options.timeout > kP2pMaxOperationTimeout ||
        options.sessionTimeout <= std::chrono::milliseconds::zero() ||
        options.sessionTimeout > kP2pMaxSessionTimeout) {
        return Error{ErrorCode::InvalidArgument, "invalid P2P manager configuration"};
    }
    if (!service.directP2pReady(options.nodeId, options.corpusId, options.corpusEpoch)) {
        return Error{ErrorCode::InvalidArgument,
                     "direct P2P requires a fully recovered writer-authenticated service with "
                     "matching node, corpus, and epoch"};
    }
    auto identity = TlsIdentity::fromPrivateKeyPem(options.nodeId, options.privateKeyPem);
    if (!identity) {
        return identity.error();
    }
    auto registry = PeerRegistry::open(options.databasePath, options.maxPeers);
    if (!registry) {
        return registry.error();
    }
    return std::unique_ptr<P2pManager>{new P2pManager(
        std::make_unique<Impl>(std::move(options), service, std::move(registry.value())))};
}

P2pManager::P2pManager(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
P2pManager::~P2pManager() = default;
Result<void> P2pManager::start() {
    return impl_->start();
}
void P2pManager::stop() noexcept {
    impl_->stop();
}
bool P2pManager::started() const noexcept {
    return impl_->started();
}
std::uint16_t P2pManager::boundPort() const noexcept {
    return impl_->boundPort();
}
Result<P2pSyncResult> P2pManager::connect(std::string_view connectionString) {
    return impl_->connect(connectionString);
}
Result<void> P2pManager::disconnect(std::string_view nodeId) {
    return impl_->disconnect(nodeId);
}
Result<void> P2pManager::enrollPeer(std::string_view nodeId, std::string_view spkiPin) {
    return impl_->enrollPeer(nodeId, spkiPin);
}
Result<void> P2pManager::forget(std::string_view nodeId) {
    return impl_->forget(nodeId);
}
Result<P2pLocalIdentity> P2pManager::localIdentity() const {
    return impl_->localIdentity();
}
Result<std::vector<PeerRegistryRecord>> P2pManager::peers() const {
    return impl_->peers();
}

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
void P2pManager::testing_setReconnectLoopHook(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(gReconnectLoopHookMutex);
    gReconnectLoopHook = std::move(hook);
}
#endif

} // namespace yams::daemon::p2p
