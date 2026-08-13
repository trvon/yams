#include <algorithm>
#include <array>
#include <optional>
#include <string_view>
#include <yams/cli/command_registry.h>
#include <yams/cli/yams_cli.h>

namespace yams::cli {

// Factory functions provided by the individual command implementation units.
std::unique_ptr<ICommand> createInitCommand();
std::unique_ptr<ICommand> createAddCommand();
std::unique_ptr<ICommand> createGetCommand();
std::unique_ptr<ICommand> createRestoreCommand();
std::unique_ptr<ICommand> createCatCommand();
std::unique_ptr<ICommand> createDeleteCommand();
std::unique_ptr<ICommand> createListCommand();
std::unique_ptr<ICommand> createSearchCommand();
std::unique_ptr<ICommand> createGrepCommand();
std::unique_ptr<ICommand> createConfigCommand();
std::unique_ptr<ICommand> createAuthCommand();
std::unique_ptr<ICommand> createStatusCommand();
std::unique_ptr<ICommand> createUpdateCommand();
std::unique_ptr<ICommand> createDownloadCommand();
std::unique_ptr<ICommand> createSessionCommand();
std::unique_ptr<ICommand> createWatchCommand();
std::unique_ptr<ICommand> createCompletionCommand();
std::unique_ptr<ICommand> createRepairCommand();
std::unique_ptr<ICommand> createModelCommand();
std::unique_ptr<ICommand> createDaemonCommand();
std::unique_ptr<ICommand> createPluginCommand();
std::unique_ptr<ICommand> createDoctorCommand();
std::unique_ptr<ICommand> createGraphCommand();
std::unique_ptr<ICommand> createDiffCommand();
std::unique_ptr<ICommand> createTuneCommand();
std::unique_ptr<ICommand> createP2PCommand();
#ifdef YAMS_BUILD_MCP_SERVER
std::unique_ptr<ICommand> createServeCommand();
#endif

namespace {

using CommandFactory = std::unique_ptr<ICommand> (*)();

struct CommandRegistration {
    std::string_view name;
    CommandFactory factory;
};

constexpr auto kCoreCommandRegistrations = std::to_array<CommandRegistration>({
    {"init", &createInitCommand},
    {"add", &createAddCommand},
    {"get", &createGetCommand},
    {"restore", &createRestoreCommand},
    {"cat", &createCatCommand},
    {"delete", &createDeleteCommand},
    {"list", &createListCommand},
    {"search", &createSearchCommand},
    {"grep", &createGrepCommand},
});

constexpr auto kConfigCommandRegistrations = std::to_array<CommandRegistration>({
    {"config", &createConfigCommand},
    {"auth", &createAuthCommand},
    {"status", &createStatusCommand},
    {"update", &createUpdateCommand},
    {"p2p", &createP2PCommand},
});

constexpr auto kWorkflowCommandRegistrations = std::to_array<CommandRegistration>({
    {"download", &createDownloadCommand},
    {"session", &createSessionCommand},
    {"watch", &createWatchCommand},
    {"completion", &createCompletionCommand},
    {"repair", &createRepairCommand},
});

constexpr auto kRuntimeCommandRegistrations = std::to_array<CommandRegistration>({
    {"model", &createModelCommand},
    {"daemon", &createDaemonCommand},
    {"plugin", &createPluginCommand},
    {"doctor", &createDoctorCommand},
});

constexpr auto kAnalysisCommandRegistrations = std::to_array<CommandRegistration>({
    {"graph", &createGraphCommand},
    {"diff", &createDiffCommand},
    {"tune", &createTuneCommand},
});

constexpr auto kTransportCommandRegistrations = std::to_array<CommandRegistration>({
#ifdef YAMS_BUILD_MCP_SERVER
    {"serve", &createServeCommand},
#endif
});

constexpr auto kMinimalCommandRegistrations = std::to_array<CommandRegistration>({
    {"status", &createStatusCommand},
    {"list", &createListCommand},
    {"search", &createSearchCommand},
});

void registerCommandSet(YamsCLI* cli, const auto& registrations) {
    for (const auto& entry : registrations) {
        cli->registerCommand(entry.factory());
    }
}

std::optional<CommandRegistration> findRegistration(std::string_view commandName,
                                                    const auto& registrations) {
    for (const auto& entry : registrations) {
        if (entry.name == commandName) {
            return entry;
        }
    }
    return std::nullopt;
}

bool containsRegistration(std::string_view commandName, const auto& registrations) {
    return std::any_of(registrations.begin(), registrations.end(),
                       [commandName](const auto& entry) { return entry.name == commandName; });
}

} // namespace

void CommandRegistry::registerAllCommands(YamsCLI* cli) {
    if (cli == nullptr) {
        return;
    }

    registerCommandSet(cli, kCoreCommandRegistrations);
    registerCommandSet(cli, kConfigCommandRegistrations);
    registerCommandSet(cli, kWorkflowCommandRegistrations);
    registerCommandSet(cli, kRuntimeCommandRegistrations);
    registerCommandSet(cli, kAnalysisCommandRegistrations);
    registerCommandSet(cli, kTransportCommandRegistrations);
}

bool CommandRegistry::registerMinimalCommandSet(YamsCLI* cli, std::string_view commandName) {
    if (cli == nullptr) {
        return false;
    }

    if (const auto entry = findRegistration(commandName, kMinimalCommandRegistrations)) {
        cli->registerCommand(entry->factory());
        return true;
    }
    return false;
}

bool CommandRegistry::isRegisteredTopLevelCommand(std::string_view commandName) {
    if (commandName == "rm" || commandName == "ls") {
        return true;
    }
    return containsRegistration(commandName, kCoreCommandRegistrations) ||
           containsRegistration(commandName, kConfigCommandRegistrations) ||
           containsRegistration(commandName, kWorkflowCommandRegistrations) ||
           containsRegistration(commandName, kRuntimeCommandRegistrations) ||
           containsRegistration(commandName, kAnalysisCommandRegistrations) ||
           containsRegistration(commandName, kTransportCommandRegistrations);
}

} // namespace yams::cli
