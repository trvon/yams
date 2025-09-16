#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>

namespace yams::cli {

class GraphCommand : public ICommand {
public:
    std::string getName() const override { return "graph"; }
    std::string getDescription() const override {
        return "Inspect knowledge graph relationships for a document";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* cmd = app.add_subcommand(getName(), getDescription());

        // Target selection (hash or --name)
        auto* group = cmd->add_option_group("target");
        group->add_option("hash", hash_, "SHA-256 of the target document");
        group->add_option("--name", name_, "Path/name of the target document");
        group->require_option(1);

        cmd->add_option("--depth", depth_, "Graph traversal depth (1-5)")
            ->default_val(1)
            ->check(CLI::Range(1, 5));
        cmd->add_flag("-v,--verbose", verbose_, "Verbose output");

        cmd->callback([this]() { cli_->setPendingCommand(this); });
    }

    Result<void> execute() override { return Result<void>(); }

    boost::asio::awaitable<Result<void>> executeAsync() override {
        try {
            using namespace yams::daemon;
            ClientConfig cfg;
            if (cli_)
                if (cli_->hasExplicitDataDir()) {
                    cfg.dataDir = cli_->getDataPath();
                }
            cfg.requestTimeout = std::chrono::milliseconds(60000);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes) {
                co_return leaseRes.error();
            }
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;

            GetRequest req;
            req.hash = hash_;
            req.name = name_;
            req.byName = !name_.empty();
            req.metadataOnly = true; // we only need metadata + graph
            req.showGraph = true;
            req.graphDepth = depth_;
            req.verbose = verbose_;

            auto r = co_await client.get(req);
            if (!r) {
                std::cerr << "Graph error: " << r.error().message << "\n";
                co_return r.error();
            }
            const auto& resp = r.value();

            // Header
            std::cerr << "=== Knowledge Graph ===\n";
            if (!resp.fileName.empty())
                std::cerr << "Document: " << resp.fileName << "\n";
            if (!resp.hash.empty())
                std::cerr << "Hash: "
                          << (resp.hash.size() > 12 ? resp.hash.substr(0, 12) + "..." : resp.hash)
                          << "\n";
            if (!resp.path.empty())
                std::cerr << "Path: " << resp.path << "\n";

            if (!resp.graphEnabled) {
                std::cerr << "Graph data unavailable (graph disabled or empty).\n";
                co_return Result<void>();
            }

            if (resp.related.empty()) {
                std::cerr << "No related documents found at depth " << depth_ << ".\n";
                co_return Result<void>();
            }

            std::cerr << "\nRelated Documents (depth " << depth_ << "):\n";
            for (const auto& rel : resp.related) {
                std::string sh = rel.hash.size() > 8 ? rel.hash.substr(0, 8) + "..." : rel.hash;
                std::cerr << "  - " << rel.name << " (" << rel.relationship
                          << ", distance=" << rel.distance << ")"
                          << "  [" << sh << "]\n";
            }

            co_return Result<void>();
        } catch (const std::exception& e) {
            co_return Error{ErrorCode::Unknown, e.what()};
        }
    }

private:
    YamsCLI* cli_{nullptr};
    std::string hash_;
    std::string name_;
    int depth_{1};
    bool verbose_{false};
};

std::unique_ptr<ICommand> createGraphCommand() {
    return std::make_unique<GraphCommand>();
}

} // namespace yams::cli
