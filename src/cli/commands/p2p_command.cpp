// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/core/types.h>
#include <yams/memory_sync/memory_sync_cli.h>
#include <yams/memory_sync/memory_sync_config.h>
#include <yams/memory_sync/memory_sync_service.h>

namespace yams::cli {

namespace {

/// Resolve the memory-sync backend from the typed [memory_sync] config section,
/// overlaid with optional CLI overrides. Relative filesystem paths resolve
/// against the data directory.
Result<memory_sync::MemorySyncDaemonConfig>
resolveMemorySyncConfig(YamsCLI* cli, const std::string& backendOverride,
                        const std::string& pathOverride, const std::string& nodeIdOverride) {
    memory_sync::MemorySyncDaemonConfig cfg;

    const auto configPath = config::get_config_path();
    if (!configPath.empty() && std::filesystem::exists(configPath)) {
        config::ConfigMigrator migrator;
        auto parsed = migrator.parseTomlConfig(configPath);
        if (!parsed) {
            return parsed.error();
        }
        const auto& sections = parsed.value();
        if (auto it = sections.find("memory_sync"); it != sections.end()) {
            const auto& section = it->second;
            auto get = [&](const char* k) -> const std::string* {
                auto jt = section.find(k);
                return jt == section.end() ? nullptr : &jt->second;
            };
            if (auto* v = get("enabled")) {
                cfg.enabled = (*v == "true");
            }
            if (auto* v = get("node_id")) {
                cfg.nodeId = *v;
            }
            if (auto* v = get("backend")) {
                cfg.backend = *v;
            }
            if (auto* v = get("path")) {
                cfg.path = *v;
            }
            if (auto* v = get("sync_interval_ms")) {
                try {
                    cfg.syncIntervalMs = static_cast<std::uint32_t>(std::stoul(*v));
                } catch (const std::exception& e) {
                    spdlog::warn("p2p: invalid memory_sync.sync_interval_ms '{}': {}", *v,
                                 e.what());
                }
            }
        }
    }

    if (!backendOverride.empty()) {
        cfg.backend = backendOverride;
    }
    if (!pathOverride.empty()) {
        cfg.path = pathOverride;
    }
    if (!nodeIdOverride.empty()) {
        cfg.nodeId = nodeIdOverride;
    }
    cfg.enabled = true; // explicit p2p invocation implies enabled

    if (cfg.backend == "filesystem" && !cfg.path.empty() &&
        !std::filesystem::path(cfg.path).is_absolute() && cli != nullptr) {
        cfg.path = (cli->getDataPath() / cfg.path).string();
    }
    return cfg;
}

} // namespace

class P2PCommand : public ICommand {
public:
    std::string getName() const override { return "p2p"; }

    std::string getDescription() const override {
        return "P2P memory sync: publish/read shared memory records";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* cmd = app.add_subcommand("p2p", getDescription());

        cmd->add_option("--backend", backendOverride_, "Backend: filesystem|s3");
        cmd->add_option("--path", pathOverride_, "Filesystem dir or s3:// URL");
        cmd->add_option("--node-id", nodeIdOverride_, "Writer identity");

        auto* publish = cmd->add_subcommand("publish", "Publish a value under a key");
        publish->add_option("key", publishKey_, "Logical key")->required();
        publish->add_option("value", publishValue_, "Value or @file to read bytes from")
            ->required();
        publish->add_flag("--json", jsonOutput_, "Emit JSON");
        publish->callback([this]() { failOn(runPublish()); });

        auto* read = cmd->add_subcommand("read", "Read the merged value for a key");
        read->add_option("key", readKey_, "Logical key")->required();
        read->add_flag("--json", jsonOutput_, "Emit JSON");
        read->callback([this]() { failOn(runRead()); });

        auto* status = cmd->add_subcommand("status", "Merged index summary + backend identity");
        status->add_flag("--json", jsonOutput_, "Emit JSON");
        status->callback([this]() { failOn(runStatus()); });
    }

    Result<void> execute() override { return Result<void>(); }

private:
    void failOn(Result<void> r) {
        if (!r) {
            spdlog::error("p2p command failed: {}", r.error().message);
            throw CLI::RuntimeError(1);
        }
    }

    Result<memory_sync::MemorySyncDaemonConfig> resolve() {
        return resolveMemorySyncConfig(cli_, backendOverride_, pathOverride_, nodeIdOverride_);
    }

    Result<void> runPublish() {
        auto cfgRes = resolve();
        if (!cfgRes) {
            return cfgRes.error();
        }
        auto svc = memory_sync::createMemorySyncService(cfgRes.value());
        if (!svc) {
            return svc.error();
        }
        auto out = memory_sync::publishValue(*svc.value(), publishKey_, publishValue_, jsonOutput_);
        svc.value()->stop();
        if (!out) {
            return out.error();
        }
        std::cout << out.value() << "\n";
        return Result<void>();
    }

    Result<void> runRead() {
        auto cfgRes = resolve();
        if (!cfgRes) {
            return cfgRes.error();
        }
        auto svc = memory_sync::createMemorySyncService(cfgRes.value());
        if (!svc) {
            return svc.error();
        }
        auto out = memory_sync::readValue(*svc.value(), readKey_, jsonOutput_);
        svc.value()->stop();
        if (!out) {
            return out.error();
        }
        std::cout << out.value() << "\n";
        return Result<void>();
    }

    Result<void> runStatus() {
        auto cfgRes = resolve();
        if (!cfgRes) {
            return cfgRes.error();
        }
        auto svc = memory_sync::createMemorySyncService(cfgRes.value());
        if (!svc) {
            return svc.error();
        }
        auto out = memory_sync::statusSummary(*svc.value(), cfgRes.value().backend,
                                              cfgRes.value().nodeId, jsonOutput_);
        svc.value()->stop();
        if (!out) {
            return out.error();
        }
        std::cout << out.value() << "\n";
        return Result<void>();
    }

    YamsCLI* cli_{nullptr};
    std::string publishKey_;
    std::string publishValue_;
    std::string readKey_;
    std::string backendOverride_;
    std::string pathOverride_;
    std::string nodeIdOverride_;
    bool jsonOutput_{false};
};

std::unique_ptr<ICommand> createP2PCommand() {
    return std::make_unique<P2PCommand>();
}

} // namespace yams::cli
