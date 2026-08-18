// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/yams_cli.h>
#include <yams/core/types.h>
#include <yams/daemon/client/daemon_client.h>

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

Result<std::string> formatStatus(const daemon::MemorySyncResponse& response, bool json) {
    if (!response.started) {
        return Error{ErrorCode::InvalidState, "memory sync service is not started"};
    }
    if (!json) {
        return "backend=" + response.backend + " node_id=" + response.nodeId +
               " corpus_id=" + response.corpusId +
               " corpus_epoch=" + std::to_string(response.corpusEpoch) + " mode=" + response.mode +
               " trust=" + response.trustMode + " records=" + std::to_string(response.records) +
               " quarantined=" + std::to_string(response.quarantinedRecords) +
               " auth_failures=" + std::to_string(response.authFailures) +
               " successful_cycles=" + std::to_string(response.successfulCycles) +
               " failed_cycles=" + std::to_string(response.failedCycles) +
               " last_success_age_ms=" + std::to_string(response.lastSuccessAgeMs);
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
    output["started"] = response.started;
    return output.dump(2);
}

} // namespace

class P2PCommand : public ICommand {
public:
    std::string getName() const override { return "p2p"; }

    std::string getDescription() const override {
        return "P2P memory sync: publish/read records through the running daemon";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* cmd = app.add_subcommand("p2p", getDescription());
        cmd->require_subcommand(1);

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
        config.socketPath = YamsCLI::resolveConfiguredDaemonSocketPath();
        config.autoStart = false;
        config.requestTimeout = std::chrono::seconds(5);
        if (!daemon::DaemonClient::isDaemonRunning(config.socketPath)) {
            return Error{ErrorCode::NotFound,
                         "daemon is not running on socket: " + config.socketPath.string()};
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
