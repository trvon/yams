#include <spdlog/spdlog.h>

#include <iostream>
#include <string>

#include <yams/cli/command.h>
#include <yams/cli/tune_runner.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>

namespace yams::cli {

class TuneCommand : public ICommand {
public:
    std::string getName() const override { return "tune"; }

    std::string getDescription() const override {
        return "Interactive relevance tuner and policy state management";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("tune", getDescription());
        cmd->require_subcommand(0, 1);

        cmd->add_option("-q,--queries", opts_.queries, "Synthetic queries per session")
            ->default_val(10);
        cmd->add_option("-k,--top-k", opts_.k, "Results to label per query")->default_val(5);
        cmd->add_option("--seed", opts_.seed, "Random seed (0 = system time)")->default_val(0);
        cmd->add_flag("--json", opts_.json, "Emit the persisted session as JSON to stdout");
        cmd->add_flag("--non-interactive", opts_.nonInteractive,
                      "Print setup instructions and exit (for scripts / CI)");

        cmd->callback([this, cmd]() {
            if (cmd->get_subcommands().empty()) {
                invoked_ = Mode::Interactive;
                auto r = execute();
                if (!r) {
                    spdlog::error("tune: {}", r.error().message);
                    std::exit(1);
                }
            }
        });

        auto* statusCmd =
            cmd->add_subcommand("status", "Print current tuner state (observations, reward EWMA)");
        statusCmd->add_flag("--json", statusJson_, "Emit JSON instead of formatted output");
        statusCmd->callback([this]() {
            invoked_ = Mode::Status;
            auto r = execute();
            if (!r) {
                spdlog::error("tune status: {}", r.error().message);
                std::exit(1);
            }
        });

        auto* resetCmd = cmd->add_subcommand("reset", "Delete persisted tuner state");
        resetCmd
            ->add_option("--policy", resetPolicy_,
                         "Which state to delete: rules | contextual | all")
            ->default_val("rules");
        resetCmd->add_flag("-y,--yes", resetYes_, "Skip confirmation prompt");
        resetCmd->callback([this]() {
            invoked_ = Mode::Reset;
            auto r = execute();
            if (!r) {
                spdlog::error("tune reset: {}", r.error().message);
                std::exit(1);
            }
        });

        auto* replayCmd = cmd->add_subcommand(
            "replay", "Replay labeled sessions against available policies (R6 — stub)");
        replayCmd->callback([this]() {
            invoked_ = Mode::Replay;
            auto r = execute();
            if (!r) {
                spdlog::error("tune replay: {}", r.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        switch (invoked_) {
            case Mode::Interactive:
                runInteractiveTune(cli_, opts_);
                return Result<void>{};
            case Mode::Status:
                runTuneStatus(cli_, statusJson_);
                return Result<void>{};
            case Mode::Reset: {
                int rc = runTuneReset(cli_, resetPolicy_, resetYes_);
                if (rc != 0) {
                    return Error{ErrorCode::InternalError, "tune reset failed"};
                }
                return Result<void>{};
            }
            case Mode::Replay:
                runTuneReplayStub();
                return Result<void>{};
            case Mode::None:
            default:
                runInteractiveTune(cli_, opts_);
                return Result<void>{};
        }
    }

private:
    enum class Mode { None, Interactive, Status, Reset, Replay };

    YamsCLI* cli_ = nullptr;
    TuneOptions opts_;
    Mode invoked_ = Mode::None;
    bool statusJson_ = false;
    std::string resetPolicy_{"rules"};
    bool resetYes_ = false;
};

std::unique_ptr<ICommand> createTuneCommand() {
    return std::make_unique<TuneCommand>();
}

} // namespace yams::cli
