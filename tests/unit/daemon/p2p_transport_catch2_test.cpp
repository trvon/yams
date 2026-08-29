// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/p2p/p2p_transport.h>
#include <yams/memory_sync/writer_auth.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using yams::daemon::p2p::P2pClientOptions;
using yams::daemon::p2p::P2pConnection;
using yams::daemon::p2p::P2pListener;
using yams::daemon::p2p::TlsIdentity;

namespace {

std::vector<std::byte> bytes(std::string_view text) {
    std::vector<std::byte> result(text.size());
    std::memcpy(result.data(), text.data(), text.size());
    return result;
}

std::string text(const std::vector<std::byte>& data) {
    return std::string(reinterpret_cast<const char*>(data.data()), data.size());
}

TlsIdentity identity(std::string nodeId, const yams::memory_sync::WriterKeyPair& keyPair) {
    auto result = TlsIdentity::fromPrivateKeyPem(std::move(nodeId), keyPair.privateKeyPem);
    REQUIRE(result.has_value());
    return std::move(result.value());
}

bool waitUntil(std::function<bool()> predicate, std::chrono::milliseconds timeout = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    return predicate();
}

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
struct AcceptLoopHookGuard {
    ~AcceptLoopHookGuard() {
        P2pListener::testing_setAcceptLoopHook({});
        P2pListener::testing_setHandshakeStartedHook({});
        P2pListener::testing_setAfterAcceptJoinHook({});
    }
};
#endif

} // namespace

TEST_CASE("P2P TLS identity derives a stable SPKI pin from writer key", "[daemon][p2p][tls]") {
    auto firstKey = yams::memory_sync::generateWriterKeyPair();
    auto secondKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(firstKey.has_value());
    REQUIRE(secondKey.has_value());

    auto first = identity("node-a", firstKey.value());
    auto reloaded = identity("node-a", firstKey.value());
    auto other = identity("node-b", secondKey.value());

    CHECK(first.nodeId() == "node-a");
    CHECK(first.certPem().find("BEGIN CERTIFICATE") != std::string::npos);
    CHECK(first.spkiPin().size() == 64);
    CHECK(first.spkiPin() == reloaded.spkiPin());
    CHECK(first.spkiPin() != other.spkiPin());
}

TEST_CASE("P2P transport trust defaults fail closed", "[daemon][p2p][tls]") {
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(clientKey.has_value());
    REQUIRE(serverKey.has_value());

    auto unpinned = yams::daemon::p2p::p2pConnect(
        P2pClientOptions{.host = "127.0.0.1",
                         .port = 1,
                         .identity = identity("client-node", clientKey.value()),
                         .expectedPeerPin = {},
                         .connectTimeout = 100ms,
                         .handshakeTimeout = 100ms});
    REQUIRE_FALSE(unpinned.has_value());
    CHECK(unpinned.error().code == yams::ErrorCode::InvalidArgument);

    auto invalidDeadline = yams::daemon::p2p::p2pConnect(
        P2pClientOptions{.host = "127.0.0.1",
                         .port = 1,
                         .identity = identity("client-node", clientKey.value()),
                         .expectedPeerPin = std::string(64, 'a'),
                         .connectTimeout = 100ms,
                         .handshakeTimeout = 100ms,
                         .sessionTimeout = 0ms});
    REQUIRE_FALSE(invalidDeadline.has_value());
    CHECK(invalidDeadline.error().code == yams::ErrorCode::InvalidArgument);

    auto oversizedDeadline = yams::daemon::p2p::p2pConnect(
        P2pClientOptions{.host = "127.0.0.1",
                         .port = 1,
                         .identity = identity("client-node", clientKey.value()),
                         .expectedPeerPin = std::string(64, 'a'),
                         .connectTimeout = 100ms,
                         .handshakeTimeout = 100ms,
                         .sessionTimeout = yams::daemon::p2p::kP2pMaxSessionTimeout + 1ms});
    REQUIRE_FALSE(oversizedDeadline.has_value());
    CHECK(oversizedDeadline.error().code == yams::ErrorCode::InvalidArgument);

    P2pListener listener(P2pListener::Options{
        .host = "127.0.0.1",
        .port = 0,
        .identity = identity("server-node", serverKey.value()),
        .allowedPeerPins = {std::string(64, 'a')},
        .handshakeTimeout = 100ms,
        .maxConcurrentSessions = 1,
    });
    auto started = listener.start();
    REQUIRE_FALSE(started.has_value());
    CHECK(started.error().code == yams::ErrorCode::InvalidState);

    P2pListener noHandshakeSlots(P2pListener::Options{
        .host = "127.0.0.1",
        .port = 0,
        .identity = identity("server-node", serverKey.value()),
        .allowedPeerPins = {},
        .handshakeTimeout = 100ms,
        .sessionTimeout = 1s,
        .maxConcurrentHandshakes = 0,
        .maxConcurrentSessions = 1,
    });
    noHandshakeSlots.setHandler([](P2pConnection) {});
    auto noCapacity = noHandshakeSlots.start();
    REQUIRE_FALSE(noCapacity.has_value());
    CHECK(noCapacity.error().code == yams::ErrorCode::InvalidArgument);

    P2pListener excessiveHandshakeSlots(P2pListener::Options{
        .host = "127.0.0.1",
        .port = 0,
        .identity = identity("server-node", serverKey.value()),
        .allowedPeerPins = {},
        .handshakeTimeout = 100ms,
        .sessionTimeout = 1s,
        .maxConcurrentHandshakes = yams::daemon::p2p::kP2pMaxConcurrentHandshakes + 1,
        .maxConcurrentSessions = 1,
    });
    excessiveHandshakeSlots.setHandler([](P2pConnection) {});
    auto excessiveCapacity = excessiveHandshakeSlots.start();
    REQUIRE_FALSE(excessiveCapacity.has_value());
    CHECK(excessiveCapacity.error().code == yams::ErrorCode::InvalidArgument);
}

TEST_CASE("P2P TLS listener and client mutually authenticate and exchange frames",
          "[daemon][p2p][tls][network]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());

    auto serverIdentity = identity("server-node", serverKey.value());
    auto clientIdentity = identity("client-node", clientKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();

    P2pListener::Options listenerOptions{.host = "127.0.0.1",
                                         .port = 0,
                                         .identity = std::move(serverIdentity),
                                         .allowedPeerPins = {clientPin},
                                         .handshakeTimeout = 2s,
                                         .maxConcurrentSessions = 2};
    P2pListener listener(std::move(listenerOptions));
    std::promise<yams::Result<void>> serverDone;
    auto serverFuture = serverDone.get_future();
    listener.setHandler([&](P2pConnection channel) {
        if (channel.peerSpkiPin() != clientPin || channel.peerCertCn() != "client-node") {
            serverDone.set_value(
                yams::Error{yams::ErrorCode::Unauthorized, "unexpected client identity"});
            return;
        }
        auto request = channel.readFrame(2s);
        if (!request) {
            serverDone.set_value(request.error());
            return;
        }
        if (text(request.value()) != "ping") {
            serverDone.set_value(
                yams::Error{yams::ErrorCode::InvalidData, "unexpected request frame"});
            return;
        }
        auto written = channel.writeFrame(bytes("pong"), 2s);
        serverDone.set_value(written ? yams::Result<void>{} : yams::Result<void>{written.error()});
    });
    REQUIRE(listener.start().has_value());
    REQUIRE(listener.boundPort() != 0);

    auto client =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = serverPin,
                                                       .connectTimeout = 2s,
                                                       .handshakeTimeout = 2s});
    REQUIRE(client.has_value());
    CHECK(client.value().peerSpkiPin() == serverPin);
    CHECK(client.value().peerCertCn() == "server-node");
    CHECK(client.value().localNodeId() == "client-node");

    auto excessiveWrite =
        client.value().writeFrame(bytes("ping"), yams::daemon::p2p::kP2pMaxOperationTimeout + 1ms);
    REQUIRE_FALSE(excessiveWrite.has_value());
    CHECK(excessiveWrite.error().code == yams::ErrorCode::InvalidArgument);
    auto excessiveRead = client.value().readFrame(yams::daemon::p2p::kP2pMaxOperationTimeout + 1ms);
    REQUIRE_FALSE(excessiveRead.has_value());
    CHECK(excessiveRead.error().code == yams::ErrorCode::InvalidArgument);

    REQUIRE(client.value().writeFrame(bytes("ping"), 2s).has_value());
    auto response = client.value().readFrame(2s);
    REQUIRE(response.has_value());
    CHECK(text(response.value()) == "pong");

    REQUIRE(serverFuture.wait_for(3s) == std::future_status::ready);
    CHECK(serverFuture.get().has_value());
    CHECK(listener.acceptedCount() == 1);

    auto oversized = client.value().writeFrame(bytes("too-large"), 2s, 4);
    REQUIRE_FALSE(oversized.has_value());
    CHECK(oversized.error().code == yams::ErrorCode::InvalidArgument);

    client.value().close();
    listener.stop();
    CHECK_FALSE(listener.started());
}

TEST_CASE("P2P TLS read deadline interrupts a stalled peer", "[daemon][p2p][tls][network]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());

    auto serverIdentity = identity("server-node", serverKey.value());
    auto clientIdentity = identity("client-node", clientKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();
    std::promise<void> entered;
    auto enteredFuture = entered.get_future();
    std::promise<void> release;
    auto releaseFuture = release.get_future().share();

    P2pListener::Options listenerOptions{.host = "127.0.0.1",
                                         .port = 0,
                                         .identity = std::move(serverIdentity),
                                         .allowedPeerPins = {clientPin},
                                         .handshakeTimeout = 2s,
                                         .maxConcurrentSessions = 1};
    P2pListener listener(std::move(listenerOptions));
    listener.setHandler([&](P2pConnection) {
        entered.set_value();
        releaseFuture.wait();
    });
    REQUIRE(listener.start().has_value());

    auto client =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = serverPin,
                                                       .connectTimeout = 2s,
                                                       .handshakeTimeout = 2s});
    REQUIRE(client.has_value());
    REQUIRE(enteredFuture.wait_for(2s) == std::future_status::ready);

    auto frame = client.value().readFrame(100ms);
    REQUIRE_FALSE(frame.has_value());
    CHECK(frame.error().code == yams::ErrorCode::Timeout);

    release.set_value();
    listener.stop();
}

TEST_CASE("P2P TLS listener reaps completed session workers while running",
          "[daemon][p2p][tls][network][lifecycle]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());

    auto serverIdentity = identity("server-node", serverKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {},
                                              .allowUnpinnedPeers = true,
                                              .handshakeTimeout = 2s,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([](P2pConnection channel) { channel.close(); });
    REQUIRE(listener.start().has_value());

    constexpr std::size_t kSessionCount = 20;
    for (std::size_t index = 0; index < kSessionCount; ++index) {
        auto client = yams::daemon::p2p::p2pConnect(
            P2pClientOptions{.host = "127.0.0.1",
                             .port = listener.boundPort(),
                             .identity = identity("client-node", clientKey.value()),
                             .expectedPeerPin = serverPin,
                             .connectTimeout = 2s,
                             .handshakeTimeout = 2s});
        REQUIRE(client.has_value());
        auto closed = client.value().readFrame(2s);
        REQUIRE_FALSE(closed.has_value());
    }

    CHECK(listener.acceptedCount() == kSessionCount);
    CHECK(listener.retainedSessionCount() <= 2);
    listener.stop();
    CHECK(listener.retainedSessionCount() == 0);
}

TEST_CASE("P2P TLS listener rejects a mismatched client pin", "[daemon][p2p][tls][network]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());

    auto serverIdentity = identity("server-node", serverKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    std::promise<void> handlerCalled;
    auto handlerFuture = handlerCalled.get_future();

    P2pListener::Options listenerOptions{.host = "127.0.0.1",
                                         .port = 0,
                                         .identity = std::move(serverIdentity),
                                         .allowedPeerPins = {std::string(64, 'f')},
                                         .handshakeTimeout = 2s,
                                         .maxConcurrentSessions = 1};
    P2pListener listener(std::move(listenerOptions));
    listener.setHandler([&](P2pConnection) { handlerCalled.set_value(); });
    REQUIRE(listener.start().has_value());

    auto client = yams::daemon::p2p::p2pConnect(
        P2pClientOptions{.host = "127.0.0.1",
                         .port = listener.boundPort(),
                         .identity = identity("client-node", clientKey.value()),
                         .expectedPeerPin = serverPin,
                         .connectTimeout = 2s,
                         .handshakeTimeout = 2s});
    if (client) {
        auto closed = client.value().readFrame(2s);
        REQUIRE_FALSE(closed.has_value());
    }
    CHECK(listener.acceptedCount() == 1);
    CHECK(handlerFuture.wait_for(0s) == std::future_status::timeout);

    listener.stop();
}

TEST_CASE("P2P listener keeps unauthenticated handshakes out of authenticated capacity",
          "[daemon][p2p][tls][network][capacity]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());

    auto serverIdentity = identity("server-node", serverKey.value());
    auto clientIdentity = identity("client-node", clientKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();
    std::promise<void> authenticated;
    auto authenticatedFuture = authenticated.get_future();
    std::promise<void> release;
    auto releaseFuture = release.get_future().share();

    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {clientPin},
                                              .handshakeTimeout = 2s,
                                              .sessionTimeout = 3s,
                                              .maxConcurrentHandshakes = 2,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection) {
        authenticated.set_value();
        releaseFuture.wait();
    });
    REQUIRE(listener.start().has_value());

    boost::asio::io_context rawIo;
    boost::asio::ip::tcp::socket stalled(rawIo);
    stalled.connect({boost::asio::ip::make_address("127.0.0.1"), listener.boundPort()});
    REQUIRE(waitUntil([&] { return listener.preAuthSessionCount() == 1; }));
    CHECK(listener.authenticatedSessionCount() == 0);

    auto client =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = serverPin,
                                                       .connectTimeout = 2s,
                                                       .handshakeTimeout = 2s,
                                                       .sessionTimeout = 3s});
    REQUIRE(client.has_value());
    REQUIRE(authenticatedFuture.wait_for(2s) == std::future_status::ready);
    CHECK(listener.preAuthSessionCount() == 1);
    CHECK(listener.authenticatedSessionCount() == 1);

    release.set_value();
    client.value().close();
    boost::system::error_code ignored;
    stalled.close(ignored);
    listener.stop();
}

TEST_CASE("P2P listener bounds and reuses unauthenticated handshake capacity",
          "[daemon][p2p][tls][network][capacity]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    P2pListener listener(
        P2pListener::Options{.host = "127.0.0.1",
                             .port = 0,
                             .identity = identity("server-node", serverKey.value()),
                             .allowedPeerPins = {},
                             .handshakeTimeout = 2s,
                             .sessionTimeout = 3s,
                             .maxConcurrentHandshakes = 1,
                             .maxConcurrentSessions = 1});
    listener.setHandler([](P2pConnection) {});
    REQUIRE(listener.start().has_value());

    boost::asio::io_context rawIo;
    boost::asio::ip::tcp::socket first(rawIo);
    boost::asio::ip::tcp::socket queued(rawIo);
    const boost::asio::ip::tcp::endpoint endpoint{boost::asio::ip::make_address("127.0.0.1"),
                                                  listener.boundPort()};
    first.connect(endpoint);
    REQUIRE(waitUntil([&] { return listener.preAuthSessionCount() == 1; }));
    queued.connect(endpoint);
    std::this_thread::sleep_for(50ms);
    CHECK(listener.acceptedCount() == 1);
    CHECK(listener.preAuthSessionCount() == 1);

    boost::system::error_code ignored;
    first.close(ignored);
    REQUIRE(waitUntil([&] { return listener.acceptedCount() == 2; }));
    CHECK(listener.preAuthSessionCount() == 1);
    queued.close(ignored);
    listener.stop();
    CHECK(listener.preAuthSessionCount() == 0);
}

TEST_CASE("P2P listener bounds and reuses authenticated session capacity",
          "[daemon][p2p][tls][network][capacity]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());
    auto serverIdentity = identity("server-node", serverKey.value());
    const auto serverPin = serverIdentity.spkiPin();
    std::atomic<unsigned> invocations{0};
    std::promise<void> firstEntered;
    auto firstEnteredFuture = firstEntered.get_future();
    std::promise<void> releaseFirst;
    auto releaseFirstFuture = releaseFirst.get_future().share();
    std::promise<void> reused;
    auto reusedFuture = reused.get_future();

    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {},
                                              .allowUnpinnedPeers = true,
                                              .handshakeTimeout = 2s,
                                              .sessionTimeout = 3s,
                                              .maxConcurrentHandshakes = 2,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection) {
        if (invocations.fetch_add(1, std::memory_order_relaxed) == 0) {
            firstEntered.set_value();
            releaseFirstFuture.wait();
            return;
        }
        reused.set_value();
    });
    REQUIRE(listener.start().has_value());

    auto connectClient = [&] {
        return yams::daemon::p2p::p2pConnect(
            P2pClientOptions{.host = "127.0.0.1",
                             .port = listener.boundPort(),
                             .identity = identity("client-node", clientKey.value()),
                             .expectedPeerPin = serverPin,
                             .connectTimeout = 2s,
                             .handshakeTimeout = 2s,
                             .sessionTimeout = 3s});
    };
    auto first = connectClient();
    REQUIRE(first.has_value());
    REQUIRE(firstEnteredFuture.wait_for(1s) == std::future_status::ready);
    CHECK(listener.authenticatedSessionCount() == 1);

    auto rejected = connectClient();
    REQUIRE(rejected.has_value());
    REQUIRE_FALSE(rejected.value().readFrame(1s).has_value());
    CHECK(invocations.load(std::memory_order_relaxed) == 1);
    CHECK(listener.authenticatedSessionCount() == 1);

    releaseFirst.set_value();
    first.value().close();
    REQUIRE(waitUntil([&] { return listener.authenticatedSessionCount() == 0; }));
    auto third = connectClient();
    REQUIRE(third.has_value());
    REQUIRE(reusedFuture.wait_for(1s) == std::future_status::ready);
    CHECK(invocations.load(std::memory_order_relaxed) == 2);
    listener.stop();
}

TEST_CASE("P2P connection enforces one aggregate session deadline",
          "[daemon][p2p][tls][network][deadline]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());

    auto serverIdentity = identity("server-node", serverKey.value());
    auto clientIdentity = identity("client-node", clientKey.value());
    const std::string serverPin = serverIdentity.spkiPin();
    const std::string clientPin = clientIdentity.spkiPin();

    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {clientPin},
                                              .handshakeTimeout = 2s,
                                              .sessionTimeout = 2s,
                                              .maxConcurrentHandshakes = 2,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([](P2pConnection channel) {
        for (int exchange = 0; exchange < 2; ++exchange) {
            auto request = channel.readFrame(2s);
            if (!request || !channel.writeFrame(request.value(), 2s)) {
                return;
            }
        }
        (void)channel.readFrame(2s);
    });
    REQUIRE(listener.start().has_value());

    auto client =
        yams::daemon::p2p::p2pConnect(P2pClientOptions{.host = "127.0.0.1",
                                                       .port = listener.boundPort(),
                                                       .identity = std::move(clientIdentity),
                                                       .expectedPeerPin = serverPin,
                                                       .connectTimeout = 2s,
                                                       .handshakeTimeout = 2s,
                                                       .sessionTimeout = 500ms});
    REQUIRE(client.has_value());
    const auto started = std::chrono::steady_clock::now();
    for (int exchange = 0; exchange < 2; ++exchange) {
        REQUIRE(client.value().writeFrame(bytes("ping"), 2s).has_value());
        auto response = client.value().readFrame(2s);
        REQUIRE(response.has_value());
        CHECK(text(response.value()) == "ping");
    }
    std::this_thread::sleep_until(started + 600ms);
    auto expired = client.value().writeFrame(bytes("late"), 2s);
    REQUIRE_FALSE(expired.has_value());
    CHECK(expired.error().code == yams::ErrorCode::Timeout);
    listener.stop();
}

TEST_CASE("P2P listener stop interrupts an in-flight unauthenticated handshake",
          "[daemon][p2p][tls][network][lifecycle]") {
#if YAMS_DAEMON_TEST_HOOKS_ENABLED
    AcceptLoopHookGuard guard;
    std::promise<void> handshakeStarted;
    auto handshakeStartedFuture = handshakeStarted.get_future();
    P2pListener::testing_setHandshakeStartedHook([&] { handshakeStarted.set_value(); });
#endif
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());

    P2pListener listener(
        P2pListener::Options{.host = "127.0.0.1",
                             .port = 0,
                             .identity = identity("server-node", serverKey.value()),
                             .allowedPeerPins = {},
                             .handshakeTimeout = 5s,
                             .sessionTimeout = 6s,
                             .maxConcurrentHandshakes = 1,
                             .maxConcurrentSessions = 1});
    listener.setHandler([](P2pConnection) {});
    REQUIRE(listener.start().has_value());

    boost::asio::io_context rawIo;
    boost::asio::ip::tcp::socket stalled(rawIo);
    stalled.connect({boost::asio::ip::make_address("127.0.0.1"), listener.boundPort()});
#if YAMS_DAEMON_TEST_HOOKS_ENABLED
    REQUIRE(handshakeStartedFuture.wait_for(2s) == std::future_status::ready);
#else
    REQUIRE(waitUntil([&] { return listener.preAuthSessionCount() == 1; }));
#endif
    CHECK(listener.preAuthSessionCount() == 1);
    const auto started = std::chrono::steady_clock::now();
    listener.stop();
    CHECK(std::chrono::steady_clock::now() - started < 1s);
    CHECK(listener.preAuthSessionCount() == 0);
}

TEST_CASE("P2P listener contains handler exceptions and reuses authenticated capacity",
          "[daemon][p2p][network][lifecycle]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());
    auto serverIdentity = identity("server-node", serverKey.value());
    const auto serverPin = serverIdentity.spkiPin();
    std::atomic<unsigned> invocations{0};
    std::promise<void> recovered;
    auto recoveredFuture = recovered.get_future();

    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {},
                                              .allowUnpinnedPeers = true,
                                              .handshakeTimeout = 1s,
                                              .sessionTimeout = 2s,
                                              .maxConcurrentHandshakes = 2,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection) {
        if (invocations.fetch_add(1, std::memory_order_relaxed) == 0) {
            throw std::runtime_error("injected handler failure");
        }
        recovered.set_value();
    });
    REQUIRE(listener.start().has_value());

    auto connectClient = [&] {
        return yams::daemon::p2p::p2pConnect(
            P2pClientOptions{.host = "127.0.0.1",
                             .port = listener.boundPort(),
                             .identity = identity("client-node", clientKey.value()),
                             .expectedPeerPin = serverPin,
                             .connectTimeout = 1s,
                             .handshakeTimeout = 1s,
                             .sessionTimeout = 2s});
    };
    auto first = connectClient();
    REQUIRE(first.has_value());
    REQUIRE_FALSE(first.value().readFrame(1s).has_value());
    REQUIRE(waitUntil([&] { return listener.authenticatedSessionCount() == 0; }));

    auto second = connectClient();
    REQUIRE(second.has_value());
    REQUIRE(recoveredFuture.wait_for(1s) == std::future_status::ready);
    CHECK(invocations.load(std::memory_order_relaxed) == 2);
    listener.stop();
}

TEST_CASE("P2P listener permits a session handler to request stop",
          "[daemon][p2p][network][lifecycle]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());
    auto serverIdentity = identity("server-node", serverKey.value());
    const auto serverPin = serverIdentity.spkiPin();
    std::promise<void> handlerReturnedFromStop;
    auto handlerReturnedFromStopFuture = handlerReturnedFromStop.get_future();

    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {},
                                              .allowUnpinnedPeers = true,
                                              .handshakeTimeout = 1s,
                                              .sessionTimeout = 2s,
                                              .maxConcurrentHandshakes = 1,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection) {
        listener.stop();
        handlerReturnedFromStop.set_value();
    });
    REQUIRE(listener.start().has_value());
    auto client = yams::daemon::p2p::p2pConnect(
        P2pClientOptions{.host = "127.0.0.1",
                         .port = listener.boundPort(),
                         .identity = identity("client-node", clientKey.value()),
                         .expectedPeerPin = serverPin,
                         .connectTimeout = 1s,
                         .handshakeTimeout = 1s,
                         .sessionTimeout = 2s});
    REQUIRE(client.has_value());
    REQUIRE(handlerReturnedFromStopFuture.wait_for(1s) == std::future_status::ready);
    CHECK_FALSE(listener.started());
    // External cleanup joins the worker that requested cancellation.
    listener.stop();
    CHECK(listener.retainedSessionCount() == 0);
}

#if YAMS_DAEMON_TEST_HOOKS_ENABLED
TEST_CASE("P2P listener contains accept-thread exceptions", "[daemon][p2p][lifecycle]") {
    AcceptLoopHookGuard guard;
    P2pListener::testing_setAcceptLoopHook(
        [] { throw std::runtime_error("injected accept-loop failure"); });
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());
    auto serverIdentity = identity("server-node", serverKey.value());
    const auto serverPin = serverIdentity.spkiPin();
    std::promise<void> connectedAfterRestart;
    auto connectedAfterRestartFuture = connectedAfterRestart.get_future();

    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {},
                                              .allowUnpinnedPeers = true,
                                              .handshakeTimeout = 1s,
                                              .sessionTimeout = 2s,
                                              .maxConcurrentHandshakes = 1,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection) { connectedAfterRestart.set_value(); });
    REQUIRE(listener.start().has_value());
    REQUIRE(waitUntil([&] { return !listener.started(); }));

    auto unsafeRestart = listener.start();
    REQUIRE_FALSE(unsafeRestart.has_value());
    CHECK(unsafeRestart.error().code == yams::ErrorCode::InvalidState);

    P2pListener::testing_setAcceptLoopHook({});
    listener.stop();
    REQUIRE(listener.start().has_value());
    auto client = yams::daemon::p2p::p2pConnect(
        P2pClientOptions{.host = "127.0.0.1",
                         .port = listener.boundPort(),
                         .identity = identity("client-node", clientKey.value()),
                         .expectedPeerPin = serverPin,
                         .connectTimeout = 1s,
                         .handshakeTimeout = 1s,
                         .sessionTimeout = 2s});
    REQUIRE(client.has_value());
    CHECK(connectedAfterRestartFuture.wait_for(1s) == std::future_status::ready);
    listener.stop();
}

TEST_CASE("P2P listener serializes restart with external teardown", "[daemon][p2p][lifecycle]") {
    AcceptLoopHookGuard guard;
    std::promise<void> acceptJoined;
    auto acceptJoinedFuture = acceptJoined.get_future();
    std::promise<void> releaseTeardown;
    auto releaseTeardownFuture = releaseTeardown.get_future().share();
    P2pListener::testing_setAfterAcceptJoinHook([&] {
        acceptJoined.set_value();
        releaseTeardownFuture.wait();
    });
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    P2pListener listener(
        P2pListener::Options{.host = "127.0.0.1",
                             .port = 0,
                             .identity = identity("server-node", serverKey.value()),
                             .allowedPeerPins = {},
                             .handshakeTimeout = 1s,
                             .sessionTimeout = 2s,
                             .maxConcurrentHandshakes = 1,
                             .maxConcurrentSessions = 1});
    listener.setHandler([](P2pConnection) {});
    REQUIRE(listener.start().has_value());

    auto stopping = std::async(std::launch::async, [&] { listener.stop(); });
    REQUIRE(acceptJoinedFuture.wait_for(1s) == std::future_status::ready);
    auto restarting = std::async(std::launch::async, [&] { return listener.start(); });
    CHECK(restarting.wait_for(50ms) == std::future_status::timeout);
    P2pListener::testing_setAfterAcceptJoinHook({});
    releaseTeardown.set_value();
    stopping.get();
    auto restarted = restarting.get();
    REQUIRE(restarted.has_value());
    CHECK(listener.started());
    listener.stop();
}

TEST_CASE("P2P listener contains overlapping worker and external stop requests",
          "[daemon][p2p][network][lifecycle]") {
    AcceptLoopHookGuard guard;
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());
    auto serverIdentity = identity("server-node", serverKey.value());
    const auto serverPin = serverIdentity.spkiPin();
    std::promise<void> handlerEntered;
    auto handlerEnteredFuture = handlerEntered.get_future();
    std::promise<void> requestWorkerStop;
    auto requestWorkerStopFuture = requestWorkerStop.get_future().share();
    std::promise<void> acceptJoined;
    auto acceptJoinedFuture = acceptJoined.get_future();
    P2pListener::testing_setAfterAcceptJoinHook([&] { acceptJoined.set_value(); });

    P2pListener listener(P2pListener::Options{.host = "127.0.0.1",
                                              .port = 0,
                                              .identity = std::move(serverIdentity),
                                              .allowedPeerPins = {},
                                              .allowUnpinnedPeers = true,
                                              .handshakeTimeout = 1s,
                                              .sessionTimeout = 2s,
                                              .maxConcurrentHandshakes = 1,
                                              .maxConcurrentSessions = 1});
    listener.setHandler([&](P2pConnection) {
        handlerEntered.set_value();
        requestWorkerStopFuture.wait();
        listener.stop();
    });
    REQUIRE(listener.start().has_value());
    auto client = yams::daemon::p2p::p2pConnect(
        P2pClientOptions{.host = "127.0.0.1",
                         .port = listener.boundPort(),
                         .identity = identity("client-node", clientKey.value()),
                         .expectedPeerPin = serverPin,
                         .connectTimeout = 1s,
                         .handshakeTimeout = 1s,
                         .sessionTimeout = 2s});
    REQUIRE(client.has_value());
    REQUIRE(handlerEnteredFuture.wait_for(1s) == std::future_status::ready);

    auto externalStop = std::async(std::launch::async, [&] { listener.stop(); });
    REQUIRE(acceptJoinedFuture.wait_for(1s) == std::future_status::ready);
    requestWorkerStop.set_value();
    externalStop.get();
    CHECK_FALSE(listener.started());
    CHECK(listener.retainedSessionCount() == 0);
}
#endif

TEST_CASE("P2P TLS client rejects a mismatched server pin", "[daemon][p2p][tls][network]") {
    auto serverKey = yams::memory_sync::generateWriterKeyPair();
    auto clientKey = yams::memory_sync::generateWriterKeyPair();
    REQUIRE(serverKey.has_value());
    REQUIRE(clientKey.has_value());

    P2pListener::Options listenerOptions{.host = "127.0.0.1",
                                         .port = 0,
                                         .identity = identity("server-node", serverKey.value()),
                                         .allowedPeerPins = {},
                                         .handshakeTimeout = 2s,
                                         .maxConcurrentSessions = 1};
    P2pListener listener(std::move(listenerOptions));
    listener.setHandler([](P2pConnection channel) { channel.close(); });
    REQUIRE(listener.start().has_value());

    auto client = yams::daemon::p2p::p2pConnect(
        P2pClientOptions{.host = "127.0.0.1",
                         .port = listener.boundPort(),
                         .identity = identity("client-node", clientKey.value()),
                         .expectedPeerPin = std::string(64, '0'),
                         .connectTimeout = 2s,
                         .handshakeTimeout = 2s});
    REQUIRE_FALSE(client.has_value());
    CHECK(client.error().code == yams::ErrorCode::Unauthorized);

    listener.stop();
}
