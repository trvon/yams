#include <spdlog/spdlog.h>
#include <filesystem>
#include <iostream>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>

namespace yams::cli {

namespace fs = std::filesystem;

class UninstallCommand : public ICommand {
public:
    std::string getName() const override { return "uninstall"; }

    std::string getDescription() const override { return "Remove YAMS from your system"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("uninstall", getDescription());

        cmd->add_flag("--purge", purge_, "Remove all data, config, and cache directories");
        cmd->add_flag("--keep-config", keepConfig_, "Preserve configuration files");
        cmd->add_option("--backup-to", backupPath_, "Create backup before removing")
            ->check(CLI::NonexistentPath);
        cmd->add_flag("-y,--yes", skipConfirmation_, "Skip confirmation prompt");

        cmd->callback([this]() {
            // Phase 2 implementation
            std::cout << "\n╔══════════════════════════════════════════════╗\n";
            std::cout << "║          YAMS Uninstall - Phase 2           ║\n";
            std::cout << "╚══════════════════════════════════════════════╝\n\n";

            std::cout << "Status: Coming in Phase 2\n\n";

            std::cout << "This command will:\n";
            std::cout << "  • Remove ~/.config/yams/ (configuration & keys)\n";
            std::cout << "  • Remove ~/.local/share/yams/ (data & storage)\n";
            std::cout << "  • Remove ~/.cache/yams/ (if exists)\n";
            std::cout << "  • Clean up any custom storage locations\n\n";

            std::cout << "Options:\n";
            std::cout << "  --purge        Remove everything (default)\n";
            std::cout << "  --keep-config  Preserve config files\n";
            std::cout << "  --backup-to    Create backup before removal\n";
            std::cout << "  -y, --yes      Skip confirmation\n\n";

            std::cout << "Expected usage:\n";
            std::cout << "  yams uninstall                    # Interactive removal\n";
            std::cout << "  yams uninstall --purge -y         # Remove everything\n";
            std::cout << "  yams uninstall --keep-config      # Keep configuration\n";
            std::cout << "  yams uninstall --backup-to backup.tar.gz\n\n";

            // Show what would be removed (dry run)
            std::cout << "Directories that would be removed:\n";

            fs::path homeDir =
                std::getenv("HOME") ? fs::path(std::getenv("HOME")) : fs::current_path();
            fs::path configDir = homeDir / ".config" / "yams";
            fs::path dataDir = homeDir / ".local" / "share" / "yams";
            fs::path cacheDir = homeDir / ".cache" / "yams";

            if (fs::exists(configDir) && !keepConfig_) {
                std::cout << "  ✓ " << configDir << " (config & keys)\n";
            }
            if (fs::exists(dataDir)) {
                std::cout << "  ✓ " << dataDir << " (data & storage)\n";
            }
            if (fs::exists(cacheDir)) {
                std::cout << "  ✓ " << cacheDir << " (cache)\n";
            }

            std::cout << "\nNote: In Phase 2, this will actually remove these directories.\n";
            std::cout << "      For now, you can manually remove them if needed.\n";

            std::exit(0);
        });
    }

    Result<void> execute() override {
        return Error{ErrorCode::NotImplemented, "Uninstall command coming in Phase 2"};
    }

private:
    YamsCLI* cli_ = nullptr;
    bool purge_ = false;
    bool keepConfig_ = false;
    bool skipConfirmation_ = false;
    std::string backupPath_;
};

// Factory function
std::unique_ptr<ICommand> createUninstallCommand() {
    return std::make_unique<UninstallCommand>();
}

} // namespace yams::cli