// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

// pi-lens-ignore: fatal error
#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/p2p/p2p_transport.h>
#include <yams/memory_sync/writer_auth.h>

#include <chrono>
#include <cstring>
#include <future>
#include <string>
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
