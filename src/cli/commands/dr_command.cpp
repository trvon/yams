#include <nlohmann/json.hpp>
#include <future>
#include <iostream>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::cli {

class DrCommand : public ICommand {
public:
    std::string getName() const override { return "dr"; }
    std::string getDescription() const override { return "Disaster Recovery (gated by plugins)"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* dr = app.add_subcommand(getName(), getDescription());
        // Basic subcommands that gate
        dr->add_subcommand("status", "Show DR status (requires plugin)")->callback([this]() {
            showGate();
        });
        dr->add_subcommand("agent", "Run DR agent (requires plugin)")->callback([this]() {
            showGate();
        });
        dr->add_subcommand("help", "Show DR help")->callback([this]() { showGate(); });
    }

    Result<void> execute() override { return Result<void>(); }

private:
    void showGate() {
        using namespace yams::daemon;
        bool hasObj = false, hasDr = false;
        try {
            ClientConfig cfg;
            cfg.dataDir = cli_->getDataPath();
            cfg.singleUseConnections = true;
            cfg.requestTimeout = std::chrono::milliseconds(4000);
            DaemonClient client(cfg);
            GetStatsRequest req;
            std::promise<Result<yams::daemon::GetStatsResponse>> prom;
            auto fut = prom.get_future();
            boost::asio::co_spawn(
                boost::asio::system_executor{},
                [&]() -> boost::asio::awaitable<void> {
                    auto r = co_await client.call(req);
                    prom.set_value(std::move(r));
                    co_return;
                },
                boost::asio::detached);
            auto res = (fut.wait_for(std::chrono::seconds(4)) == std::future_status::ready)
                           ? fut.get()
                           : Result<yams::daemon::GetStatsResponse>(
                                 Error{ErrorCode::Timeout, "stats timeout"});
            if (res) {
                const auto& r = res.value();
                auto it = r.additionalStats.find("plugins_json");
                if (it != r.additionalStats.end()) {
                    auto pj = nlohmann::json::parse(it->second, nullptr, false);
                    if (!pj.is_discarded()) {
                        for (auto& rec : pj) {
                            if (!rec.contains("interfaces"))
                                continue;
                            auto interfaces = rec["interfaces"];
                            if (interfaces.is_array()) {
                                for (auto& iv : interfaces) {
                                    std::string id = iv.get<std::string>();
                                    if (id == "object_storage_v1")
                                        hasObj = true;
                                    if (id == "dr_provider_v1")
                                        hasDr = true;
                                }
                            }
                        }
                    }
                }
            }
        } catch (...) {
        }

        if (hasObj && hasDr) {
            std::cout << "DR plugins detected (object_storage_v1 + dr_provider_v1).\n";
            std::cout << "DR commands are not yet implemented. Watch this space.\n";
        } else {
            std::cout << "Requires dr_provider_v1 + object_storage_v1 plugin.\n";
            std::cout << "See docs/guide/external_dr_plugins.md for details.\n";
        }
    }

    YamsCLI* cli_{nullptr};
};

std::unique_ptr<ICommand> createDrCommand() {
    return std::make_unique<DrCommand>();
}

} // namespace yams::cli
