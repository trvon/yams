// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/process_discovery.h>

namespace yams::cli {

namespace {

Result<std::string> readValueOrFile(std::string_view valueOrFile) {
    if (valueOrFile.empty() || valueOrFile.front() != '@') {
        return std::string(valueOrFile);
    }

    std::ifstream in{std::string(valueOrFile.substr(1)), std::ios::binary};
    if (!in) {
        return Error{ErrorCode::IOError, "cannot open file: " + std::string(valueOrFile.substr(1))};
    }
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

Result<std::string> formatPublish(std::string_view key, std::size_t size, bool json) {
    if (!json) {
        return "published " + std::string(key) + " (" + std::to_string(size) + " bytes)";
    }
    nlohmann::json output;
    output["key"] = key;
    output["published"] = true;
    output["size"] = size;
    return output.dump(2);
}

Result<std::string> formatDelete(std::string_view key, bool json) {
    if (!json) {
        return "deleted " + std::string(key);
    }
    nlohmann::json output;
    output["key"] = key;
    output["deleted"] = true;
    return output.dump(2);
}

Result<std::string> formatRead(std::string_view key, const std::string& value, bool json) {
    if (!json) {
        return value;
    }
    static constexpr char kHex[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(value.size() * 2);
    for (const unsigned char byte : value) {
        hex.push_back(kHex[byte >> 4]);
        hex.push_back(kHex[byte & 0xf]);
    }
    nlohmann::json output;
    output["key"] = key;
    output["size"] = value.size();
    output["value_hex"] = hex;
    return output.dump(2);
}

Result<nlohmann::json> parseControlPayload(const daemon::MemorySyncResponse& response) {
    try {
        return nlohmann::json::parse(response.value);
    } catch (const std::exception& error) {
        return Error{ErrorCode::SerializationError,
                     std::string("daemon returned malformed P2P response: ") + error.what()};
    }
}

Result<std::string> formatIdentity(const daemon::MemorySyncResponse& response, bool json) {
    auto payload = parseControlPayload(response);
    if (!payload) {
        return payload.error();
    }
    if (json) {
        return payload.value().dump(2);
    }
    return "Node ID: " + payload.value().value("node_id", std::string{}) +
           "\nSPKI pin: " + payload.value().value("spki_pin", std::string{});
}

Result<std::string> formatEnroll(const daemon::MemorySyncResponse& response, bool json) {
    auto payload = parseControlPayload(response);
    if (!payload) {
        return payload.error();
    }
    if (json) {
        return payload.value().dump(2);
    }
    return "Enrolled " + payload.value().value("node_id", std::string{}) + " with SPKI pin " +
           payload.value().value("spki_pin", std::string{});
}

Result<std::string> formatConnect(const daemon::MemorySyncResponse& response, bool json) {
    auto payload = parseControlPayload(response);
    if (!payload) {
        return payload.error();
    }
    if (json) {
        return payload.value().dump(2);
    }
    const auto& value = payload.value();
    return "connected " + value.value("peer_node_id", std::string{}) +
           " (sent=" + std::to_string(value.value("deltas_sent", std::uint64_t{0})) +
           ", received=" + std::to_string(value.value("deltas_received", std::uint64_t{0})) + ")";
}

Result<std::string> formatDisconnect(const daemon::MemorySyncResponse& response, bool json) {
    auto payload = parseControlPayload(response);
    if (!payload) {
        return payload.error();
    }
    if (json) {
        return payload.value().dump(2);
    }
    return "disconnected " + payload.value().value("node_id", std::string{});
}

Result<std::string> formatForget(const daemon::MemorySyncResponse& response, bool json) {
    auto payload = parseControlPayload(response);
    if (!payload) {
        return payload.error();
    }
    if (json) {
        return payload.value().dump(2);
    }
    return "forgot " + payload.value().value("node_id", std::string{});
}

Result<std::string> formatPeers(const daemon::MemorySyncResponse& response, bool json) {
    auto payload = parseControlPayload(response);
    if (!payload) {
        return payload.error();
    }
    if (!payload.value().is_array()) {
        return Error{ErrorCode::SerializationError, "daemon P2P peer response is not an array"};
    }
    if (json) {
        return payload.value().dump(2);
    }
    if (payload.value().empty()) {
        return std::string{"no peers"};
    }
    std::ostringstream out;
    for (std::size_t index = 0; index < payload.value().size(); ++index) {
        const auto& peer = payload.value().at(index);
        if (index != 0) {
            out << '\n';
        }
        out << peer.value("node_id", "<unknown>") << "  " << peer.value("endpoint", "<inbound>")
            << "  remembered=" << (peer.value("remembered", false) ? "yes" : "no")
            << "  last_connected_ms=" << peer.value("last_connected_ms", std::int64_t{0});
    }
    return out.str();
}

Result<std::string> formatStatus(const daemon::MemorySyncResponse& response, bool json) {
    if (!response.started) {
        return Error{ErrorCode::InvalidState, "memory sync service is not started"};
    }
    if (!json) {
        std::ostringstream out;
        // pi-lens-ignore: clang:no_member -- additive response field is compiled by Meson.
        out << "backend=" << response.backend << " node_id=" << response.nodeId
            << " corpus_id=" << response.corpusId << " corpus_epoch=" << response.corpusEpoch
            << " mode=" << response.mode << " trust=" << response.trustMode
            << " peers=" << response.peerCount << " records=" << response.records
            << " quarantined=" << response.quarantinedRecords
            << " auth_failures=" << response.authFailures
            << " successful_cycles=" << response.successfulCycles
            << " failed_cycles=" << response.failedCycles
            << " last_success_age_ms=" << response.lastSuccessAgeMs;
        return out.str();
    }
    nlohmann::json output;
    output["backend"] = response.backend;
    output["node_id"] = response.nodeId;
    output["records"] = response.records;
    output["quarantined"] = response.quarantinedRecords;
    output["auth_failures"] = response.authFailures;
    output["successful_cycles"] = response.successfulCycles;
    output["failed_cycles"] = response.failedCycles;
    output["last_success_age_ms"] = response.lastSuccessAgeMs;
    output["corpus_id"] = response.corpusId;
    output["corpus_epoch"] = response.corpusEpoch;
    output["mode"] = response.mode;
    output["trust"] = response.trustMode;
    // pi-lens-ignore: clang:no_member -- additive response field is compiled by Meson.
    output["peer_count"] = response.peerCount;
    output["started"] = response.started;
    return output.dump(2);
}

} // namespace

class P2PCommand : public ICommand {
public:
    std::string getName() const override { return "p2p"; }

    std::string getDescription() const override {
        return "Secure direct daemon-to-daemon corpus sync";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* cmd = app.add_subcommand("p2p", getDescription());
        cmd->require_subcommand(1);

        auto* identity = cmd->add_subcommand("identity", "Show the local node ID and SPKI pin");
        identity->add_flag("--json", jsonOutput_, "Emit JSON");
        identity->callback([this]() { failOn(runIdentity()); });

        auto* enroll =
            cmd->add_subcommand("enroll", "Approve a peer node ID and SPKI pin out of band");
        enroll->add_option("node-id", enrollNodeId_, "Peer node id")->required();
        enroll->add_option("spki-pin", enrollPin_, "Peer SHA-256 SPKI pin")->required();
        enroll->add_flag("--json", jsonOutput_, "Emit JSON");
        enroll->callback([this]() { failOn(runEnroll()); });

        auto* connect = cmd->add_subcommand("connect", "Connect and synchronize with a peer");
        connect
            ->add_option("connection", connectionString_,
                         "host:port or yams://host:port with optional query parameters")
            ->required();
        connect->add_flag("--json", jsonOutput_, "Emit JSON");
        connect->callback([this]() { failOn(runConnect()); });

        auto* disconnect =
            cmd->add_subcommand("disconnect", "Disable automatic reconnect for a peer");
        disconnect->add_option("node-id", disconnectNodeId_, "Peer node id")->required();
        disconnect->add_flag("--json", jsonOutput_, "Emit JSON");
        disconnect->callback([this]() { failOn(runDisconnect()); });

        auto* forget =
            cmd->add_subcommand("forget", "Remove a peer's pinned identity (key rotation)");
        forget->add_option("node-id", forgetNodeId_, "Peer node id")->required();
        forget->add_flag("--json", jsonOutput_, "Emit JSON");
        forget->callback([this]() { failOn(runForget()); });

        auto* peers = cmd->add_subcommand("peers", "List trusted peers and reconnect state");
        peers->add_flag("--json", jsonOutput_, "Emit JSON");
        peers->callback([this]() { failOn(runPeers()); });

        auto* publish = cmd->add_subcommand("publish", "Publish a value under a key");
        publish->add_option("key", publishKey_, "Logical key")->required();
        publish->add_option("value", publishValue_, "Value or @file to read bytes from")
            ->required();
        publish->add_flag("--json", jsonOutput_, "Emit JSON");
        publish->callback([this]() { failOn(runPublish()); });

        auto* remove = cmd->add_subcommand("delete", "Publish a causal tombstone for a key");
        remove->add_option("key", deleteKey_, "Logical key")->required();
        remove->add_flag("--json", jsonOutput_, "Emit JSON");
        remove->callback([this]() { failOn(runDelete()); });

        auto* read = cmd->add_subcommand("read", "Read the daemon's merged value for a key");
        read->add_option("key", readKey_, "Logical key")->required();
        read->add_flag("--json", jsonOutput_, "Emit JSON");
        read->callback([this]() { failOn(runRead()); });

        auto* status = cmd->add_subcommand("status", "Daemon memory-sync status");
        status->add_flag("--json", jsonOutput_, "Emit JSON");
        status->callback([this]() { failOn(runStatus()); });
    }

    Result<void> execute() override { return {}; }

private:
    void failOn(Result<void> result) {
        if (!result) {
            spdlog::error("p2p command failed: {}", result.error().message);
            throw CLI::RuntimeError(1);
        }
    }

    Result<daemon::MemorySyncResponse> call(daemon::MemorySyncRequest request) const {
        if (cli_ == nullptr) {
            return Error{ErrorCode::NotInitialized, "CLI context unavailable"};
        }
        daemon::ClientConfig config;
        const auto configuredSocket = YamsCLI::resolveConfiguredDaemonSocketPath();
        // Discover the live daemon socket (e.g. a packaged/systemd service running on a different
        // socket than the per-user default) before giving up, matching `yams daemon status`.
        const auto liveSocket = daemon::client::discoverLiveDaemonSocket(
            configuredSocket, YamsCLI::resolveConfiguredDaemonPidFilePath(), true);
        config.socketPath = liveSocket && !liveSocket->empty() ? *liveSocket : configuredSocket;
        config.autoStart = false;
        config.requestTimeout = std::chrono::seconds(30);
        if (!daemon::DaemonClient::isDaemonRunning(config.socketPath)) {
            return Error{ErrorCode::NotFound,
                         "daemon is not running on socket: " + config.socketPath.string()};
        }
        if (config.socketPath != configuredSocket) {
            spdlog::info("Using live daemon socket '{}' instead of configured '{}'",
                         config.socketPath.string(), configuredSocket.string());
        }
        auto leaseResult = acquire_cli_daemon_client_shared_with_policy(
            config, CliDaemonAccessPolicy::RequireSocket, 1, 1, config.requestTimeout);
        if (!leaseResult) {
            return leaseResult.error();
        }
        auto lease = std::move(leaseResult.value().lease);
        return run_result((*lease)->call<daemon::MemorySyncRequest>(request),
                          config.requestTimeout);
    }

    Result<void> runIdentity() {
        auto response = call({daemon::MemorySyncOperation::Identity, {}, {}});
        if (!response) {
            return response.error();
        }
        auto output = formatIdentity(response.value(), jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runEnroll() {
        auto response = call({daemon::MemorySyncOperation::Enroll, enrollNodeId_, enrollPin_});
        if (!response) {
            return response.error();
        }
        auto output = formatEnroll(response.value(), jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runConnect() {
        auto response = call({daemon::MemorySyncOperation::Connect, connectionString_, {}});
        if (!response) {
            return response.error();
        }
        auto output = formatConnect(response.value(), jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runDisconnect() {
        auto response = call({daemon::MemorySyncOperation::Disconnect, disconnectNodeId_, {}});
        if (!response) {
            return response.error();
        }
        auto output = formatDisconnect(response.value(), jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runForget() {
        auto response = call({daemon::MemorySyncOperation::Forget, forgetNodeId_, {}});
        if (!response) {
            return response.error();
        }
        auto output = formatForget(response.value(), jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runPeers() {
        auto response = call({daemon::MemorySyncOperation::Peers, {}, {}});
        if (!response) {
            return response.error();
        }
        auto output = formatPeers(response.value(), jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runPublish() {
        auto value = readValueOrFile(publishValue_);
        if (!value) {
            return value.error();
        }
        auto response = call({daemon::MemorySyncOperation::Publish, publishKey_, value.value()});
        if (!response) {
            return response.error();
        }
        auto output = formatPublish(publishKey_, value.value().size(), jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runDelete() {
        auto response = call({daemon::MemorySyncOperation::Delete, deleteKey_, {}});
        if (!response) {
            return response.error();
        }
        auto output = formatDelete(deleteKey_, jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runRead() {
        auto response = call({daemon::MemorySyncOperation::Read, readKey_, {}});
        if (!response) {
            return response.error();
        }
        auto output = formatRead(readKey_, response.value().value, jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    Result<void> runStatus() {
        auto response = call({daemon::MemorySyncOperation::Status, {}, {}});
        if (!response) {
            return response.error();
        }
        auto output = formatStatus(response.value(), jsonOutput_);
        if (!output) {
            return output.error();
        }
        std::cout << output.value() << "\n";
        return {};
    }

    YamsCLI* cli_{nullptr};
    std::string connectionString_;
    std::string disconnectNodeId_;
    std::string forgetNodeId_;
    std::string enrollNodeId_;
    std::string enrollPin_;
    std::string publishKey_;
    std::string publishValue_;
    std::string deleteKey_;
    std::string readKey_;
    bool jsonOutput_{false};
};

std::unique_ptr<ICommand> createP2PCommand() {
    return std::make_unique<P2PCommand>();
}

} // namespace yams::cli
