#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <iostream>
#include <sstream>
#include <string_view>
#include <vector>

#include <yams/cli/command_catalog.h>
#include <yams/cli/completion_command.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>

namespace yams::cli {

namespace {

struct CompletionSubcommand {
    std::string name;
    std::string description;
};

struct CompletionOption {
    std::vector<std::string> names;
    std::string description;
    bool takesValue = false;
};

struct CompletionValueHint {
    std::string path;
    std::string trigger;
    std::vector<std::string> values;
};

struct CompletionPathEntry {
    std::string path;
    std::vector<CompletionSubcommand> children;
    std::vector<CompletionOption> options;
};

std::string canonicalizePath(std::string_view path) {
    if (path == "plugins" || path.starts_with("plugins ")) {
        std::string normalized(path);
        normalized.replace(0, std::string("plugins").size(), "plugin");
        return normalized;
    }
    return std::string(path);
}

bool isGlobalFlagName(std::string_view name) {
    static constexpr std::array<std::string_view, 9> kGlobalFlagNames = {
        "--help",    "-h", "--version", "--data-dir", "--storage",
        "--verbose", "-v", "--json",    "--help-all",
    };
    return std::find(kGlobalFlagNames.begin(), kGlobalFlagNames.end(), name) !=
           kGlobalFlagNames.end();
}

std::string escapeSingleQuotedShell(std::string_view text) {
    std::string escaped;
    escaped.reserve(text.size());
    for (char ch : text) {
        if (ch == '\'') {
            escaped += "'\\''";
        } else {
            escaped.push_back(ch);
        }
    }
    return escaped;
}

std::string escapePowerShellSingleQuoted(std::string_view text) {
    std::string escaped;
    escaped.reserve(text.size());
    for (char ch : text) {
        if (ch == '\'') {
            escaped += "''";
        } else {
            escaped.push_back(ch);
        }
    }
    return escaped;
}

bool isManualOptionPath(std::string_view path) {
    static constexpr std::array<std::string_view, 8> kManualPaths = {
        "add", "get", "list", "grep", "config", "auth", "status", "stats",
    };
    return std::find(kManualPaths.begin(), kManualPaths.end(), path) != kManualPaths.end();
}

std::vector<CompletionValueHint> manualValueHints() {
    return {
        {"daemon start", "--log-level", {"trace", "debug", "info", "warn", "error"}},
        {"daemon log", "--level", {"trace", "debug", "info", "warn", "error"}},
        {"config search path-tree enable", "--mode", {"fallback", "preferred"}},
        {"config search path-tree mode", "__positional__", {"fallback", "preferred"}},
        {"config embeddings tune", "__positional__", {"performance", "quality", "balanced"}},
        {"config tuning preset", "__positional__", {"efficient", "balanced", "aggressive"}},
        {"completion", "__positional__", {"bash", "zsh", "fish", "powershell", "pwsh"}},
    };
}

std::vector<CompletionOption> collectDirectOptions(const CLI::App* app) {
    std::vector<CompletionOption> options;
    if (app == nullptr) {
        return options;
    }

    for (const auto* option : app->get_options(
             [](const CLI::Option* opt) { return opt != nullptr && opt->nonpositional(); })) {
        if (option == nullptr) {
            continue;
        }

        CompletionOption completionOption;
        for (const auto& longName : option->get_lnames()) {
            const std::string rendered = "--" + longName;
            if (!isGlobalFlagName(rendered)) {
                completionOption.names.push_back(rendered);
            }
        }
        for (const auto& shortName : option->get_snames()) {
            const std::string rendered = "-" + shortName;
            if (!isGlobalFlagName(rendered)) {
                completionOption.names.push_back(rendered);
            }
        }

        if (completionOption.names.empty()) {
            continue;
        }

        completionOption.description = option->get_description();
        if (completionOption.description.empty()) {
            completionOption.description = completionOption.names.front();
        }
        completionOption.takesValue = option->get_items_expected_min() > 0;
        options.push_back(std::move(completionOption));
    }

    return options;
}

std::string joinCommandPath(const std::vector<std::string>& path) {
    std::ostringstream oss;
    for (size_t i = 0; i < path.size(); ++i) {
        if (i > 0) {
            oss << ' ';
        }
        oss << path[i];
    }
    return oss.str();
}

std::string describeCommand(std::string_view name) {
    return std::string(topLevelCommandDescription(name));
}

std::vector<CompletionSubcommand> collectDirectSubcommands(const CLI::App* app) {
    std::vector<CompletionSubcommand> children;
    if (app == nullptr) {
        return children;
    }

    for (const auto* sub : app->get_subcommands([](const CLI::App*) { return true; })) {
        if (sub == nullptr) {
            continue;
        }
        const std::string name = sub->get_name();
        if (name.empty()) {
            continue;
        }
        std::string description = sub->get_description();
        if (description.empty()) {
            description = describeCommand(name);
        }
        children.push_back({name, std::move(description)});
    }
    return children;
}

void mergeChildren(std::vector<CompletionSubcommand>& target,
                   const std::vector<CompletionSubcommand>& source) {
    for (const auto& child : source) {
        auto it =
            std::find_if(target.begin(), target.end(), [&](const CompletionSubcommand& existing) {
                return existing.name == child.name;
            });
        if (it == target.end()) {
            target.push_back(child);
        }
    }
}

void mergeOptions(std::vector<CompletionOption>& target,
                  const std::vector<CompletionOption>& source) {
    for (const auto& option : source) {
        auto it = std::find_if(target.begin(), target.end(), [&](const CompletionOption& existing) {
            return existing.names == option.names;
        });
        if (it == target.end()) {
            target.push_back(option);
        }
    }
}

void collectCommandTreeEntriesRecursive(const CLI::App* app, std::vector<std::string>& path,
                                        std::vector<CompletionPathEntry>& out) {
    const auto children = collectDirectSubcommands(app);
    const auto options = collectDirectOptions(app);
    if (!children.empty() || !options.empty()) {
        const auto normalizedPath = canonicalizePath(joinCommandPath(path));
        auto it = std::find_if(out.begin(), out.end(), [&](const CompletionPathEntry& existing) {
            return existing.path == normalizedPath;
        });
        if (it == out.end()) {
            out.push_back({normalizedPath, children, options});
        } else {
            mergeChildren(it->children, children);
            mergeOptions(it->options, options);
        }
    }

    if (app == nullptr) {
        return;
    }

    for (const auto* sub : app->get_subcommands([](const CLI::App*) { return true; })) {
        if (sub == nullptr) {
            continue;
        }
        const std::string name = sub->get_name();
        if (name.empty()) {
            continue;
        }
        path.push_back(name);
        collectCommandTreeEntriesRecursive(sub, path, out);
        path.pop_back();
    }
}

std::vector<CompletionPathEntry> collectCommandTreeEntries(const CLI::App* rootApp) {
    std::vector<CompletionPathEntry> out;
    if (rootApp != nullptr) {
        std::vector<std::string> path;
        collectCommandTreeEntriesRecursive(rootApp, path, out);
        if (!out.empty()) {
            return out;
        }
    }

    CompletionPathEntry rootEntry;
    for (const auto& name : topLevelCommandNames()) {
        rootEntry.children.push_back({name, describeCommand(name)});
    }
    out.push_back(std::move(rootEntry));
    return out;
}
} // namespace

std::string CompletionCommand::getName() const {
    return "completion";
}

std::string CompletionCommand::getDescription() const {
    return "Generate shell completion scripts for bash, zsh, fish, and PowerShell";
}

void CompletionCommand::registerCommand(CLI::App& app, YamsCLI* cli) {
    cli_ = cli;
    rootApp_ = &app;

    auto* cmd = app.add_subcommand("completion", getDescription());

    cmd->add_option("shell", shell_, "Shell type (bash, zsh, fish, powershell)")
        ->required()
        ->check(CLI::IsMember({"bash", "zsh", "fish", "powershell", "pwsh"}));

    cmd->callback([this]() { cli_->setPendingCommand(this); });
}

Result<void> CompletionCommand::execute() {
    try {
        std::string completionScript;

        if (shell_ == "bash") {
            completionScript = generateBashCompletion();
        } else if (shell_ == "zsh") {
            completionScript = generateZshCompletion();
        } else if (shell_ == "fish") {
            completionScript = generateFishCompletion();
        } else if (shell_ == "powershell" || shell_ == "pwsh") {
            completionScript = generatePowerShellCompletion();
        } else {
            return Error{ErrorCode::InvalidArgument, "Unsupported shell: " + shell_};
        }

        std::cout << completionScript << std::endl;
        return Result<void>();

    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Failed to generate completion: ") + e.what()};
    }
}

std::vector<std::string> CompletionCommand::getAvailableCommands() const {
    std::vector<std::string> commands;
    const auto tree = collectCommandTreeEntries(rootApp_);
    if (!tree.empty()) {
        commands.reserve(tree.front().children.size());
        for (const auto& child : tree.front().children) {
            commands.push_back(child.name);
        }
    }
    if (!commands.empty()) {
        return commands;
    }
    return topLevelCommandNames();
}

std::vector<std::string> CompletionCommand::getGlobalFlags() const {
    return {"--help",    "-h", "--version", "--data-dir", "--storage",
            "--verbose", "-v", "--json",    "--help-all"};
}

std::string CompletionCommand::generateBashCompletion() const {
    std::ostringstream oss;
    const auto commandTree = collectCommandTreeEntries(rootApp_);

    oss << "#!/bin/bash\n";
    oss << "# Bash completion script for YAMS CLI\n";
    oss << "# Generated by: yams completion bash\n";
    oss << "\n";
    oss << "# Safety: provide minimal fallbacks if bash-completion isn't loaded\n";
    oss << "if ! declare -F _init_completion >/dev/null 2>&1; then\n";
    oss << "  _init_completion() { cur=\"${COMP_WORDS[COMP_CWORD]}\"; "
           "prev=\"${COMP_WORDS[COMP_CWORD-1]}\"; words=(\"${COMP_WORDS[@]}\"); "
           "cword=${COMP_CWORD}; "
           "}\n";
    oss << "fi\n";
    oss << "if ! declare -F _filedir >/dev/null 2>&1; then\n";
    oss << "  _filedir() { COMPREPLY=(); }\n";
    oss << "fi\n\n";

    oss << "_yams_normalize_path() {\n";
    oss << "    case \"$1\" in\n";
    oss << "        plugins|plugins\\ *)\n";
    oss << "            printf '%s' \"${1/plugins/plugin}\"\n";
    oss << "            ;;\n";
    oss << "        *)\n";
    oss << "            printf '%s' \"$1\"\n";
    oss << "            ;;\n";
    oss << "    esac\n";
    oss << "}\n\n";

    oss << "_yams_subcommands_for() {\n";
    oss << "    case \"$1\" in\n";
    for (const auto& entry : commandTree) {
        if (entry.children.empty()) {
            continue;
        }
        oss << "        \"" << entry.path << "\")\n";
        oss << "            printf '%s\\n'";
        for (const auto& child : entry.children) {
            oss << " '" << child.name << "'";
        }
        oss << "\n";
        oss << "            ;;\n";
    }
    oss << "    esac\n";
    oss << "}\n\n";

    oss << "_yams_options_for() {\n";
    oss << "    case \"$1\" in\n";
    for (const auto& entry : commandTree) {
        if (entry.options.empty()) {
            continue;
        }
        oss << "        \"" << entry.path << "\")\n";
        oss << "            printf '%s\\n'";
        for (const auto& option : entry.options) {
            for (const auto& name : option.names) {
                oss << " '" << name << "'";
            }
        }
        oss << "\n";
        oss << "            ;;\n";
    }
    oss << "    esac\n";
    oss << "}\n\n";

    oss << "_yams_values_for() {\n";
    oss << "    case \"$1:$2\" in\n";
    for (const auto& hint : manualValueHints()) {
        oss << "        \"" << hint.path << ":" << hint.trigger << "\")\n";
        oss << "            printf '%s\\n'";
        for (const auto& value : hint.values) {
            oss << " '" << value << "'";
        }
        oss << "\n";
        oss << "            ;;\n";
    }
    oss << "    esac\n";
    oss << "}\n\n";

    oss << "_yams_command_path() {\n";
    oss << "    local limit=\"$1\"\n";
    oss << "    local path=\"\"\n";
    oss << "    local i word subcommands\n";
    oss << "    for ((i=1; i<limit; ++i)); do\n";
    oss << "        word=\"${words[i]}\"\n";
    oss << "        [[ \"$word\" == -* ]] && continue\n";
    oss << "        subcommands=\"$(_yams_subcommands_for \"$(_yams_normalize_path \"$path\")\" | "
           "tr '\\n' ' ')\"\n";
    oss << "        [[ -z \"$subcommands\" ]] && break\n";
    oss << "        if [[ \" $subcommands \" == *\" $word \"* ]]; then\n";
    oss << "            path=\"${path:+$path }$word\"\n";
    oss << "        else\n";
    oss << "            break\n";
    oss << "        fi\n";
    oss << "    done\n";
    oss << "    printf '%s' \"$(_yams_normalize_path \"$path\")\"\n";
    oss << "}\n\n";

    oss << "_yams_completion() {\n";
    oss << "    local cur prev words cword\n";
    oss << "    _init_completion || return\n";
    oss << "\n";

    oss << "    local global_flags=\"";
    for (const auto& flag : getGlobalFlags()) {
        oss << flag << " ";
    }
    oss << "\"\n";

    oss << "    local command_path\n";
    oss << "    command_path=\"$(_yams_command_path \"$cword\")\"\n";
    oss << "    local available_subcommands\n";
    oss << "    available_subcommands=\"$(_yams_subcommands_for \"$command_path\" | tr '\\n' ' "
           "')\"\n";
    oss << "    local command_flags\n";
    oss << "    command_flags=\"$(_yams_options_for \"$command_path\" | tr '\\n' ' ')\"\n";
    oss << "    local value_hints\n";
    oss << "    value_hints=\"$(_yams_values_for \"$command_path\" \"$prev\" | tr '\\n' ' ')\"\n";
    oss << "    if [[ -n \"$value_hints\" ]]; then\n";
    oss << "        COMPREPLY=($(compgen -W \"$value_hints\" -- \"$cur\"))\n";
    oss << "        if [[ ${#COMPREPLY[@]} -gt 0 ]]; then\n";
    oss << "            return 0\n";
    oss << "        fi\n";
    oss << "    fi\n";
    oss << "    if [[ $cur != -* && -z \"$available_subcommands\" ]]; then\n";
    oss << "        value_hints=\"$(_yams_values_for \"$command_path\" __positional__ | tr '\\n' ' "
           "')\"\n";
    oss << "        if [[ -n \"$value_hints\" ]]; then\n";
    oss << "            COMPREPLY=($(compgen -W \"$value_hints\" -- \"$cur\"))\n";
    oss << "            if [[ ${#COMPREPLY[@]} -gt 0 ]]; then\n";
    oss << "                return 0\n";
    oss << "            fi\n";
    oss << "        fi\n";
    oss << "    fi\n";
    oss << "    if [[ $cur != -* && -n \"$available_subcommands\" ]]; then\n";
    oss << "        COMPREPLY=($(compgen -W \"$available_subcommands\" -- \"$cur\"))\n";
    oss << "        if [[ ${#COMPREPLY[@]} -gt 0 ]]; then\n";
    oss << "            return 0\n";
    oss << "        fi\n";
    oss << "    fi\n\n";

    oss << "    if [[ -z \"$available_subcommands\" && ( $cur == -* || -z \"$cur\" ) ]]; then\n";
    oss << "        COMPREPLY=($(compgen -W \"$command_flags $global_flags\" -- \"$cur\"))\n";
    oss << "        if [[ ${#COMPREPLY[@]} -gt 0 ]]; then\n";
    oss << "            return 0\n";
    oss << "        fi\n";
    oss << "    fi\n\n";

    oss << "    case \"$command_path\" in\n";
    oss << "        add)\n";
    oss << "            case \"$prev\" in\n";
    oss << "                --name|--tags|--metadata)\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                *)\n";
    oss << "                    local add_flags=\"--name --tags --metadata --snapshot-id "
           "--help\"\n";
    oss << "                    if [[ $cur == -* ]]; then\n";
    oss << "                        COMPREPLY=($(compgen -W \"$add_flags $global_flags\" -- "
           "\"$cur\"))\n";
    oss << "                    else\n";
    oss << "                        _filedir\n";
    oss << "                    fi\n";
    oss << "                    ;;\n";
    oss << "            esac\n";
    oss << "            ;;\n";
    oss << "        get)\n";
    oss << "            case \"$prev\" in\n";
    oss << "                --output)\n";
    oss << "                    _filedir\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                --type|--mime|--extension)\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                *)\n";
    oss << "                    local get_flags=\"--output --type --mime --extension --binary "
           "--text --created-after --created-before --modified-after --modified-before "
           "--indexed-after --indexed-before --help\"\n";
    oss << "                    if [[ $cur == -* ]]; then\n";
    oss << "                        COMPREPLY=($(compgen -W \"$get_flags $global_flags\" -- "
           "\"$cur\"))\n";
    oss << "                    fi\n";
    oss << "                    ;;\n";
    oss << "            esac\n";
    oss << "            ;;\n";
    oss << "        list)\n";
    oss << "            case \"$prev\" in\n";
    oss << "                --format)\n";
    oss << "                    COMPREPLY=($(compgen -W \"table json csv minimal\" -- \"$cur\"))\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                --sort)\n";
    oss << "                    COMPREPLY=($(compgen -W \"name size date hash\" -- \"$cur\"))\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                --type|--mime|--extension)\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                *)\n";
    oss << "                    local list_flags=\"--format --sort --reverse --limit --type --mime "
           "--extension --binary --text --created-after --created-before --modified-after "
           "--modified-before --indexed-after --indexed-before --help\"\n";
    oss << "                    if [[ $cur == -* ]]; then\n";
    oss << "                        COMPREPLY=($(compgen -W \"$list_flags $global_flags\" -- "
           "\"$cur\"))\n";
    oss << "                    fi\n";
    oss << "                    ;;\n";
    oss << "            esac\n";
    oss << "            ;;\n";
    oss << "        grep)\n";
    oss << "            case \"$prev\" in\n";
    oss << "                --color)\n";
    oss << "                    COMPREPLY=($(compgen -W \"always never auto\" -- \"$cur\"))\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                *)\n";
    oss << "                    local grep_flags=\"-A --after -B --before -C --context -i "
           "--ignore-case -w --word -v --invert -n --line-numbers -H --with-filename --no-filename "
           "-c --count -l --files-with-matches -L --files-without-match --color -m --max-count "
           "--limit --help\"\n";
    oss << "                    if [[ $cur == -* ]]; then\n";
    oss << "                        COMPREPLY=($(compgen -W \"$grep_flags $global_flags\" -- "
           "\"$cur\"))\n";
    oss << "                    else\n";
    oss << "                        _filedir\n";
    oss << "                    fi\n";
    oss << "                    ;;\n";
    oss << "            esac\n";
    oss << "            ;;\n";
    oss << "        config)\n";
    oss << "            case \"$prev\" in\n";
    oss << "                --format)\n";
    oss << "                    COMPREPLY=($(compgen -W \"toml json\" -- \"$cur\"))\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                *)\n";
    oss << "                    local config_flags=\"--format --help\"\n";
    oss << "                    COMPREPLY=($(compgen -W \"$config_flags $global_flags\" -- "
           "\"$cur\"))\n";
    oss << "                    ;;\n";
    oss << "            esac\n";
    oss << "            ;;\n";
    oss << "        auth)\n";
    oss << "            case \"$prev\" in\n";
    oss << "                --key-type)\n";
    oss << "                    COMPREPLY=($(compgen -W \"ed25519 rsa\" -- \"$cur\"))\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                --format)\n";
    oss << "                    COMPREPLY=($(compgen -W \"table json\" -- \"$cur\"))\n";
    oss << "                    return 0\n";
    oss << "                    ;;\n";
    oss << "                *)\n";
    oss << "                    local auth_flags=\"--key-type --output --format --validity "
           "--key-name --help\"\n";
    oss << "                    COMPREPLY=($(compgen -W \"$auth_flags $global_flags\" -- "
           "\"$cur\"))\n";
    oss << "                    ;;\n";
    oss << "            esac\n";
    oss << "            ;;\n";
    oss << "        status|stats)\n";
    oss << "            local status_flags=\"--no-physical --corpus\"\n";
    oss << "            if [[ $cur == -* ]]; then\n";
    oss << "                COMPREPLY=($(compgen -W \"$status_flags $global_flags\" -- "
           "\"$cur\"))\n";
    oss << "            fi\n";
    oss << "            ;;\n";
    oss << "        completion)\n";
    oss << "            if [[ $cword -eq 2 ]]; then\n";
    oss << "                COMPREPLY=($(compgen -W \"bash zsh fish powershell\" -- \"$cur\"))\n";
    oss << "            else\n";
    oss << "                COMPREPLY=($(compgen -W \"--help\" -- \"$cur\"))\n";
    oss << "            fi\n";
    oss << "            ;;\n";
    oss << "        *)\n";
    oss << "            if [[ $cur == -* ]]; then\n";
    oss << "                COMPREPLY=($(compgen -W \"$global_flags\" -- \"$cur\"))\n";
    oss << "            fi\n";
    oss << "            ;;\n";
    oss << "    esac\n";
    oss << "}\n\n";

    oss << "# Register completion for yams command\n";
    oss << "complete -F _yams_completion yams\n";

    return oss.str();
}

std::string CompletionCommand::generateZshCompletion() const {
    std::ostringstream oss;
    const auto commandTree = collectCommandTreeEntries(rootApp_);

    oss << "#compdef yams\n";
    oss << "# Zsh completion script for YAMS CLI\n";
    oss << "# Generated by: yams completion zsh\n";
    oss << "\n";
    oss << "[[ -n ${ZSH_VERSION:-} ]] || return\n";
    oss << "if ! typeset -f compinit >/dev/null 2>&1; then\n";
    oss << "  autoload -U compinit\n";
    oss << "  compinit -C\n";
    oss << "fi\n\n";
    oss << "if ! type _arguments >/dev/null 2>&1; then\n";
    oss << "  _arguments() { return 0 }\n";
    oss << "fi\n\n";

    oss << "_yams_normalize_path() {\n";
    oss << "    case \"$1\" in\n";
    oss << "        plugins|plugins\\ *)\n";
    oss << "            print -r -- \"${1/plugins/plugin}\"\n";
    oss << "            ;;\n";
    oss << "        *)\n";
    oss << "            print -r -- \"$1\"\n";
    oss << "            ;;\n";
    oss << "    esac\n";
    oss << "}\n\n";

    oss << "_yams_subcommand_names_for() {\n";
    oss << "    case \"$1\" in\n";
    for (const auto& entry : commandTree) {
        if (entry.children.empty()) {
            continue;
        }
        oss << "        '" << entry.path << "')\n";
        oss << "            print -l";
        for (const auto& child : entry.children) {
            oss << " '" << child.name << "'";
        }
        oss << "\n";
        oss << "            ;;\n";
    }
    oss << "    esac\n";
    oss << "}\n\n";

    oss << "_yams_describe_subcommands() {\n";
    oss << "    local path=\"$1\"\n";
    oss << "    local -a commands\n";
    oss << "    case \"$path\" in\n";
    for (const auto& entry : commandTree) {
        if (entry.children.empty()) {
            continue;
        }
        oss << "        '" << entry.path << "')\n";
        oss << "            commands=(\n";
        for (const auto& child : entry.children) {
            oss << "                '" << child.name << ":" << child.description << "'\n";
        }
        oss << "            )\n";
        oss << "            ;;\n";
    }
    oss << "        *)\n";
    oss << "            return 1\n";
    oss << "            ;;\n";
    oss << "    esac\n";
    oss << "    _describe 'commands' commands\n";
    oss << "}\n\n";

    oss << "_yams_describe_options() {\n";
    oss << "    local path=\"$1\"\n";
    oss << "    local -a options\n";
    oss << "    case \"$path\" in\n";
    for (const auto& entry : commandTree) {
        if (entry.options.empty()) {
            continue;
        }
        oss << "        '" << entry.path << "')\n";
        oss << "            options=(\n";
        for (const auto& option : entry.options) {
            for (const auto& name : option.names) {
                oss << "                '" << name << ":"
                    << escapeSingleQuotedShell(option.description) << "'\n";
            }
        }
        oss << "            )\n";
        oss << "            ;;\n";
    }
    oss << "        *)\n";
    oss << "            return 1\n";
    oss << "            ;;\n";
    oss << "    esac\n";
    oss << "    _describe 'options' options\n";
    oss << "}\n\n";

    oss << "_yams_describe_values() {\n";
    oss << "    local path=\"$1\"\n";
    oss << "    local trigger=\"$2\"\n";
    oss << "    local -a values\n";
    oss << "    case \"$path:$trigger\" in\n";
    for (const auto& hint : manualValueHints()) {
        oss << "        '" << hint.path << ":" << hint.trigger << "')\n";
        oss << "            values=(";
        for (const auto& value : hint.values) {
            oss << " '" << value << "'";
        }
        oss << " )\n";
        oss << "            ;;\n";
    }
    oss << "        *)\n";
    oss << "            return 1\n";
    oss << "            ;;\n";
    oss << "    esac\n";
    oss << "    _describe 'values' values\n";
    oss << "}\n\n";

    oss << "_yams_command_path() {\n";
    oss << "    local limit=\"$1\"\n";
    oss << "    local path=\"\"\n";
    oss << "    local -a path_parts subcommands\n";
    oss << "    local i word\n";
    oss << "    for ((i = 2; i < limit; ++i)); do\n";
    oss << "        word=\"$words[i]\"\n";
    oss << "        [[ \"$word\" == -* ]] && continue\n";
    oss << "        subcommands=(${(f)$(_yams_subcommand_names_for \"$(_yams_normalize_path "
           "\"$path\")\")})\n";
    oss << "        (( ${#subcommands[@]} == 0 )) && break\n";
    oss << "        if (( ${subcommands[(Ie)$word]} )); then\n";
    oss << "            path_parts+=(\"$word\")\n";
    oss << "            path=\"${(j: :)path_parts}\"\n";
    oss << "        else\n";
    oss << "            break\n";
    oss << "        fi\n";
    oss << "    done\n";
    oss << "    print -r -- \"$(_yams_normalize_path \"$path\")\"\n";
    oss << "}\n\n";

    oss << "_yams() {\n";
    oss << "    local context state state_descr line\n";
    oss << "    typeset -A opt_args\n\n";

    oss << "    local global_flags=(\n";
    for (const auto& flag : getGlobalFlags()) {
        if (flag == "--help" || flag == "-h") {
            oss << "        '(-h --help)'{-h,--help}'[Show help information]'\n";
        } else if (flag == "--verbose" || flag == "-v") {
            oss << "        '(-v --verbose)'{-v,--verbose}'[Enable verbose output]'\n";
        } else if (flag == "--version") {
            oss << "        '--version[Show version information]'\n";
        } else if (flag == "--data-dir") {
            oss << "        '--data-dir[Data directory for storage]:directory:_directories'\n";
        } else if (flag == "--storage") {
            oss << "        '--storage[Data directory for storage]:directory:_directories'\n";
        } else if (flag == "--json") {
            oss << "        '--json[Output in JSON format]'\n";
        } else if (flag == "--help-all") {
            oss << "        '--help-all[Show extended help]'\n";
        }
    }
    oss << "    )\n\n";

    oss << "    _arguments -C \\\n";
    oss << "        \"$global_flags[@]\" \\\n";
    oss << "        '*:: :->args'\n\n";

    oss << "    local command_path\n";
    oss << "    command_path=\"$(_yams_command_path $CURRENT)\"\n";
    oss << "    local prev_word=\"\"\n";
    oss << "    if (( CURRENT > 2 )); then\n";
    oss << "        prev_word=\"$words[CURRENT-1]\"\n";
    oss << "    fi\n";
    oss << "    if [[ -n \"$prev_word\" ]]; then\n";
    oss << "        if _yams_describe_values \"$command_path\" \"$prev_word\"; then\n";
    oss << "            return 0\n";
    oss << "        fi\n";
    oss << "    fi\n";
    oss << "    if [[ \"$PREFIX\" != -* ]]; then\n";
    oss << "        if _yams_describe_values \"$command_path\" __positional__; then\n";
    oss << "            return 0\n";
    oss << "        fi\n";
    oss << "    fi\n";
    oss << "    if [[ \"$PREFIX\" != -* ]]; then\n";
    oss << "        if _yams_describe_subcommands \"$command_path\"; then\n";
    oss << "            return 0\n";
    oss << "        fi\n";
    oss << "    fi\n\n";

    oss << "    case \"$command_path\" in\n";
    oss << "        add)\n";
    oss << "            _arguments \\\n";
    oss << "                '--name[Name for the content]:name:' \\\n";
    oss << "                '--tags[Tags for the content]:tags:' \\\n";
    oss << "                '--metadata[Additional metadata]:metadata:' \\\n";
    oss << "                '--snapshot-id[Snapshot ID]:id:' \\\n";
    oss << "                '*:file:_files'\n";
    oss << "            ;;\n";
    oss << "        get)\n";
    oss << "            _arguments \\\n";
    oss << "                '--output[Output file]:file:_files' \\\n";
    oss << "                '--type[File type filter]:type:' \\\n";
    oss << "                '--mime[MIME type filter]:mime:' \\\n";
    oss << "                '--extension[Extension filter]:extension:' \\\n";
    oss << "                '--binary[Binary files only]' \\\n";
    oss << "                '--text[Text files only]' \\\n";
    oss << "                '--created-after[Created after]:date:' \\\n";
    oss << "                '--created-before[Created before]:date:' \\\n";
    oss << "                '--modified-after[Modified after]:date:' \\\n";
    oss << "                '--modified-before[Modified before]:date:' \\\n";
    oss << "                '--indexed-after[Indexed after]:date:' \\\n";
    oss << "                '--indexed-before[Indexed before]:date:'\n";
    oss << "            ;;\n";
    oss << "        list)\n";
    oss << "            _arguments \\\n";
    oss << "                '--format[Output format]:format:(table json csv minimal)' \\\n";
    oss << "                '--sort[Sort by]:sort:(name size date hash)' \\\n";
    oss << "                '--reverse[Reverse sort order]' \\\n";
    oss << "                '--limit[Limit results]:limit:' \\\n";
    oss << "                '--type[File type filter]:type:' \\\n";
    oss << "                '--mime[MIME type filter]:mime:' \\\n";
    oss << "                '--extension[Extension filter]:extension:' \\\n";
    oss << "                '--binary[Binary files only]' \\\n";
    oss << "                '--text[Text files only]' \\\n";
    oss << "                '--created-after[Created after]:date:' \\\n";
    oss << "                '--created-before[Created before]:date:' \\\n";
    oss << "                '--modified-after[Modified after]:date:' \\\n";
    oss << "                '--modified-before[Modified before]:date:' \\\n";
    oss << "                '--indexed-after[Indexed after]:date:' \\\n";
    oss << "                '--indexed-before[Indexed before]:date:'\n";
    oss << "            ;;\n";
    oss << "        grep)\n";
    oss << "            _arguments \\\n";
    oss << "                '(-A --after)'{-A,--after}'[Show N lines after match]:N:' \\\n";
    oss << "                '(-B --before)'{-B,--before}'[Show N lines before match]:N:' \\\n";
    oss << "                '(-C --context)'{-C,--context}'[Show N lines before and after "
           "match]:N:' \\\n";
    oss << "                '(-i --ignore-case)'{-i,--ignore-case}'[Case-insensitive search]' \\\n";
    oss << "                '(-w --word)'{-w,--word}'[Match whole words only]' \\\n";
    oss << "                '(-v --invert)'{-v,--invert}'[Invert match]' \\\n";
    oss << "                '(-n --line-numbers)'{-n,--line-numbers}'[Show line numbers]' \\\n";
    oss << "                '(-H --with-filename)'{-H,--with-filename}'[Show filename with "
           "matches]' \\\n";
    oss << "                '--no-filename[Never show filename]' \\\n";
    oss << "                '(-c --count)'{-c,--count}'[Show only count of matching lines]' \\\n";
    oss << "                '(-l --files-with-matches)'{-l,--files-with-matches}'[Show only "
           "filenames with matches]' \\\n";
    oss << "                '(-L --files-without-match)'{-L,--files-without-match}'[Show only "
           "filenames without matches]' \\\n";
    oss << "                '--color[Color mode]:mode:(always never auto)' \\\n";
    oss << "                '(-m --max-count)'{-m,--max-count}'[Stop after N matches per file]:N:' "
           "\\\n";
    oss << "                '--limit[Alias of --max-count]:N:' \\\n";
    oss << "                '*:file:_files'\n";
    oss << "            ;;\n";
    oss << "        status|stats)\n";
    oss << "            _arguments \\\n";
    oss << "                '--no-physical[Use daemon stats only without fallback scan]' \\\n";
    oss << "                '--corpus[Show corpus statistics for search tuning]'\n";
    oss << "            ;;\n";
    oss << "        config)\n";
    oss << "            _arguments '--format[Output format]:format:(toml json)'\n";
    oss << "            ;;\n";
    oss << "        auth)\n";
    oss << "            _arguments \\\n";
    oss << "                '--key-type[Key type]:type:(ed25519 rsa)' \\\n";
    oss << "                '--output[Output directory]:directory:_directories' \\\n";
    oss << "                '--format[Output format]:format:(table json)' \\\n";
    oss << "                '--validity[Token validity]:validity:' \\\n";
    oss << "                '--key-name[Key name]:name:'\n";
    oss << "            ;;\n";
    oss << "        completion)\n";
    oss << "            _arguments '1:shell:(bash zsh fish powershell)'\n";
    oss << "            ;;\n";
    oss << "        *)\n";
    oss << "            _yams_describe_options \"$command_path\"\n";
    oss << "            ;;\n";
    oss << "    esac\n";
    oss << "}\n\n";

    oss << "if typeset -f compdef >/dev/null 2>&1; then\n";
    oss << "    compdef _yams yams\n";
    oss << "fi\n";

    return oss.str();
}

std::string CompletionCommand::generateFishCompletion() const {
    std::ostringstream oss;
    const auto commandTree = collectCommandTreeEntries(rootApp_);

    oss << "# Fish completion script for YAMS CLI\n";
    oss << "# Generated by: yams completion fish\n";
    oss << "\n";

    oss << "function __fish_yams_normalize_path\n";
    oss << "    switch \"$argv[1]\"\n";
    oss << "        case plugins 'plugins *'\n";
    oss << "            string replace -r '^plugins' 'plugin' -- \"$argv[1]\"\n";
    oss << "        case '*'\n";
    oss << "            string join '' \"$argv[1]\"\n";
    oss << "    end\n";
    oss << "end\n\n";

    oss << "function __fish_yams_subcommand_names_for\n";
    oss << "    switch \"$argv[1]\"\n";
    for (const auto& entry : commandTree) {
        if (entry.children.empty()) {
            continue;
        }
        oss << "        case '" << entry.path << "'\n";
        oss << "            printf '%s\\n'";
        for (const auto& child : entry.children) {
            oss << " '" << child.name << "'";
        }
        oss << "\n";
    }
    oss << "    end\n";
    oss << "end\n\n";

    oss << "function __fish_yams_current_path\n";
    oss << "    set -l cmd (commandline -opc)\n";
    oss << "    if test (count $cmd) -eq 0\n";
    oss << "        return 1\n";
    oss << "    end\n";
    oss << "    set -e cmd[1]\n";
    oss << "    set -l path\n";
    oss << "    for token in $cmd\n";
    oss << "        if string match -qr '^-' -- $token\n";
    oss << "            continue\n";
    oss << "        end\n";
    oss << "        set -l current (string join ' ' $path)\n";
    oss << "        set -l children (__fish_yams_subcommand_names_for (__fish_yams_normalize_path "
           "\"$current\"))\n";
    oss << "        if test (count $children) -eq 0\n";
    oss << "            break\n";
    oss << "        end\n";
    oss << "        if contains -- $token $children\n";
    oss << "            set path $path $token\n";
    oss << "        else\n";
    oss << "            break\n";
    oss << "        end\n";
    oss << "    end\n";
    oss << "    __fish_yams_normalize_path (string join ' ' $path)\n";
    oss << "end\n\n";

    oss << "function __fish_yams_path_is\n";
    oss << "    test (__fish_yams_current_path) = (string join ' ' $argv)\n";
    oss << "end\n\n";

    oss << "function __fish_yams_has_subcommands\n";
    oss << "    if string match -qr '^-' -- (commandline -ct)\n";
    oss << "        return 1\n";
    oss << "    end\n";
    oss << "    set -l path (__fish_yams_current_path)\n";
    oss << "    set -l children (__fish_yams_subcommand_names_for \"$path\")\n";
    oss << "    test (count $children) -gt 0\n";
    oss << "end\n\n";

    oss << "function __fish_yams_subcommands\n";
    oss << "    switch (__fish_yams_current_path)\n";
    for (const auto& entry : commandTree) {
        if (entry.children.empty()) {
            continue;
        }
        oss << "        case '" << entry.path << "'\n";
        oss << "            printf '%s\\t%s\\n'";
        for (const auto& child : entry.children) {
            oss << " '" << child.name << "' '" << child.description << "'";
        }
        oss << "\n";
    }
    oss << "    end\n";
    oss << "end\n\n";

    oss << "# Global options\n";
    oss << "complete -c yams -s h -l help -d 'Show help information'\n";
    oss << "complete -c yams -l version -d 'Show version information'\n";
    oss << "complete -c yams -s v -l verbose -d 'Enable verbose output'\n";
    oss << "complete -c yams -l json -d 'Output in JSON format'\n";
    oss << "complete -c yams -l data-dir -d 'Data directory for storage' -r\n";
    oss << "complete -c yams -l storage -d 'Data directory for storage' -r\n";
    oss << "complete -c yams -l help-all -d 'Show extended help'\n\n";

    oss << "# Dynamic subcommand tree\n";
    oss << "complete -c yams -f -n '__fish_yams_has_subcommands' -a "
           "'(__fish_yams_subcommands)'\n\n";

    oss << "# Generated exact-path value hints\n";
    for (const auto& hint : manualValueHints()) {
        if (hint.trigger == "__positional__") {
            oss << "complete -c yams -f -n '__fish_yams_path_is " << hint.path << "' -a '";
            for (size_t i = 0; i < hint.values.size(); ++i) {
                if (i > 0) {
                    oss << ' ';
                }
                oss << hint.values[i];
            }
            oss << "'\n";
            continue;
        }
        oss << "complete -c yams -f -n '__fish_yams_path_is " << hint.path << "'";
        if (hint.trigger.rfind("--", 0) == 0) {
            oss << " -l " << hint.trigger.substr(2);
        } else if (hint.trigger.rfind("-", 0) == 0 && hint.trigger.size() == 2) {
            oss << " -s " << hint.trigger.substr(1);
        }
        oss << " -a '";
        for (size_t i = 0; i < hint.values.size(); ++i) {
            if (i > 0) {
                oss << ' ';
            }
            oss << hint.values[i];
        }
        oss << "'\n";
    }
    oss << "\n";

    oss << "# Generated exact-path options\n";
    for (const auto& entry : commandTree) {
        if (entry.options.empty() || isManualOptionPath(entry.path) || entry.path == "completion") {
            continue;
        }
        for (const auto& option : entry.options) {
            oss << "complete -c yams -f -n '__fish_yams_path_is " << entry.path << "'";
            bool emittedName = false;
            for (const auto& name : option.names) {
                if (name.rfind("--", 0) == 0 && !emittedName) {
                    oss << " -l " << name.substr(2);
                    emittedName = true;
                } else if (name.rfind("-", 0) == 0 && name.size() == 2) {
                    oss << " -s " << name.substr(1);
                }
            }
            oss << " -d '" << escapeSingleQuotedShell(option.description) << "'";
            if (option.takesValue) {
                oss << " -r";
            }
            oss << "\n";
        }
    }
    oss << "\n";

    oss << "# add command\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is add' -l name -d 'Name for the content' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is add' -l tags -d 'Tags for the content' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is add' -l metadata -d 'Additional metadata' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is add' -l snapshot-id -d 'Snapshot ID' "
           "-r\n\n";

    oss << "# get command\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l output -d 'Output file' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l type -d 'File type filter' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l mime -d 'MIME type filter' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l extension -d 'Extension filter' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l binary -d 'Binary files only'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l text -d 'Text files only'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l created-after -d 'Created after' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l created-before -d 'Created before' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l modified-after -d 'Modified after' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l modified-before -d 'Modified "
           "before' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l indexed-after -d 'Indexed after' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is get' -l indexed-before -d 'Indexed before' "
           "-r\n\n";

    oss << "# list command\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l format -d 'Output format' -a "
           "'table json csv minimal'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l sort -d 'Sort by' -a 'name size "
           "date hash'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l reverse -d 'Reverse sort order'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l limit -d 'Limit results' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l type -d 'File type filter' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l mime -d 'MIME type filter' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l extension -d 'Extension filter' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l binary -d 'Binary files only'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l text -d 'Text files only'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l created-after -d 'Created after' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l created-before -d 'Created "
           "before' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l modified-after -d 'Modified "
           "after' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l modified-before -d 'Modified "
           "before' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l indexed-after -d 'Indexed after' "
           "-r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is list' -l indexed-before -d 'Indexed "
           "before' -r\n\n";

    oss << "# grep command\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l after -s A -d 'Show N lines after "
           "match' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l before -s B -d 'Show N lines "
           "before match' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l context -s C -d 'Show N lines "
           "before and after match' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l ignore-case -s i -d "
           "'Case-insensitive search'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l word -s w -d 'Match whole words "
           "only'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l invert -s v -d 'Invert match'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l line-numbers -s n -d 'Show line "
           "numbers'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l with-filename -s H -d 'Show "
           "filename with matches'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l no-filename -d 'Never show "
           "filename'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l count -s c -d 'Show only count of "
           "matching lines'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l files-with-matches -s l -d 'Show "
           "only filenames with matches'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l files-without-match -s L -d 'Show "
           "only filenames without matches'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l color -d 'Color mode' -a 'always "
           "never auto'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l max-count -s m -d 'Stop after N "
           "matches per file' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is grep' -l limit -d 'Alias of --max-count' "
           "-r\n\n";

    oss << "# status/stats commands\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is status; or __fish_yams_path_is stats' -l "
           "no-physical -d 'Use daemon stats only without fallback scan'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is status; or __fish_yams_path_is stats' -l "
           "corpus -d 'Show corpus statistics for search tuning'\n\n";

    oss << "# config command\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is config' -l format -d 'Output format' -a "
           "'toml json'\n\n";

    oss << "# auth command\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is auth' -l key-type -d 'Key type' -a "
           "'ed25519 rsa'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is auth' -l output -d 'Output directory' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is auth' -l format -d 'Output format' -a "
           "'table json'\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is auth' -l validity -d 'Token validity' -r\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is auth' -l key-name -d 'Key name' -r\n\n";

    oss << "# completion command\n";
    oss << "complete -c yams -f -n '__fish_yams_path_is completion' -a 'bash zsh fish powershell' "
           "-d 'Shell type'\n";

    return oss.str();
}

std::string CompletionCommand::generatePowerShellCompletion() const {
    std::ostringstream oss;
    const auto commandTree = collectCommandTreeEntries(rootApp_);

    oss << "# PowerShell completion script for YAMS CLI\n";
    oss << "# Generated by: yams completion powershell\n";
    oss << "# To install: yams completion powershell | Out-File -Encoding utf8 "
           "$PROFILE.CurrentUserAllHosts -Append\n";
    oss << "# Or add to your $PROFILE manually\n\n";

    oss << "Register-ArgumentCompleter -Native -CommandName yams -ScriptBlock {\n";
    oss << "    param($wordToComplete, $commandAst, $cursorPosition)\n\n";

    oss << "    $commands = @(\n";
    for (const auto& cmd : getAvailableCommands()) {
        oss << "        @{ Name = '" << cmd << "'; Description = '" << describeCommand(cmd)
            << "' }\n";
    }
    oss << "    )\n\n";

    oss << "    $subcommands = @{\n";
    for (const auto& entry : commandTree) {
        oss << "        '" << entry.path << "' = @(\n";
        for (const auto& child : entry.children) {
            oss << "            @{ Name = '" << child.name << "'; Description = '"
                << child.description << "' }\n";
        }
        oss << "        )\n";
    }
    oss << "    }\n\n";

    oss << "    $globalFlags = @(\n";
    oss << "        @{ Name = '--help'; Alias = '-h'; Description = 'Show help message' }\n";
    oss << "        @{ Name = '--version'; Description = 'Show version' }\n";
    oss << "        @{ Name = '--data-dir'; Description = 'Data directory path' }\n";
    oss << "        @{ Name = '--storage'; Description = 'Storage path' }\n";
    oss << "        @{ Name = '--verbose'; Alias = '-v'; Description = 'Verbose output' }\n";
    oss << "        @{ Name = '--json'; Description = 'JSON output format' }\n";
    oss << "    )\n\n";

    oss << "    $valueHints = @{\n";
    for (const auto& hint : manualValueHints()) {
        oss << "        '" << hint.path << ":" << hint.trigger << "' = @(";
        for (size_t i = 0; i < hint.values.size(); ++i) {
            if (i > 0) {
                oss << ',';
            }
            oss << "'" << escapePowerShellSingleQuoted(hint.values[i]) << "'";
        }
        oss << ")\n";
    }
    oss << "    }\n\n";

    // Command-specific flags
    oss << "    $commandFlags = @{\n";

    for (const auto& entry : commandTree) {
        if (entry.options.empty()) {
            continue;
        }
        oss << "        '" << entry.path << "' = @(\n";
        for (const auto& option : entry.options) {
            const std::string primaryName = option.names.front();
            oss << "            @{ Name = '" << escapePowerShellSingleQuoted(primaryName)
                << "'; Description = '" << escapePowerShellSingleQuoted(option.description) << "'";
            if (option.names.size() > 1) {
                oss << "; Alias = '" << escapePowerShellSingleQuoted(option.names[1]) << "'";
            }
            oss << " }\n";
        }
        oss << "        )\n";
    }

    // add command
    oss << "        'add' = @(\n";
    oss << "            @{ Name = '--name'; Description = 'Document name' }\n";
    oss << "            @{ Name = '--tags'; Description = 'Tags (comma-separated)' }\n";
    oss << "            @{ Name = '--metadata'; Description = 'Key=value metadata' }\n";
    oss << "            @{ Name = '-r'; Alias = '--recursive'; Description = 'Recursive add' }\n";
    oss << "            @{ Name = '--snapshot-id'; Description = 'Snapshot ID' }\n";
    oss << "        )\n";

    // get command
    oss << "        'get' = @(\n";
    oss << "            @{ Name = '--output'; Alias = '-o'; Description = 'Output file' }\n";
    oss << "            @{ Name = '--type'; Description = 'File type filter' }\n";
    oss << "            @{ Name = '--mime'; Description = 'MIME type filter' }\n";
    oss << "            @{ Name = '--binary'; Description = 'Binary files only' }\n";
    oss << "            @{ Name = '--text'; Description = 'Text files only' }\n";
    oss << "        )\n";

    // delete command
    oss << "        'delete' = @(\n";
    oss << "            @{ Name = '--force'; Alias = '-f'; Description = 'Force delete' }\n";
    oss << "            @{ Name = '--dry-run'; Description = 'Preview without deleting' }\n";
    oss << "            @{ Name = '--all'; Description = 'Delete all documents' }\n";
    oss << "        )\n";

    // list command
    oss << "        'list' = @(\n";
    oss << "            @{ Name = '--format'; Description = 'Output format'; Values = "
           "@('table','json','csv','minimal') }\n";
    oss << "            @{ Name = '--sort'; Description = 'Sort field'; Values = "
           "@('name','size','date','hash') }\n";
    oss << "            @{ Name = '--reverse'; Description = 'Reverse sort order' }\n";
    oss << "            @{ Name = '--limit'; Description = 'Limit results' }\n";
    oss << "            @{ Name = '--binary'; Description = 'Binary files only' }\n";
    oss << "            @{ Name = '--text'; Description = 'Text files only' }\n";
    oss << "        )\n";

    // search command
    oss << "        'search' = @(\n";
    oss << "            @{ Name = '--limit'; Alias = '-n'; Description = 'Max results' }\n";
    oss << "            @{ Name = '--offset'; Description = 'Skip results' }\n";
    oss << "            @{ Name = '--json'; Description = 'JSON output' }\n";
    oss << "            @{ Name = '--type'; Description = 'File type filter' }\n";
    oss << "            @{ Name = '--mime'; Description = 'MIME type filter' }\n";
    oss << "            @{ Name = '--threshold'; Description = 'Similarity threshold' }\n";
    oss << "        )\n";

    // grep command
    oss << "        'grep' = @(\n";
    oss << "            @{ Name = '-A'; Alias = '--after'; Description = 'Lines after match' }\n";
    oss << "            @{ Name = '-B'; Alias = '--before'; Description = 'Lines before match' }\n";
    oss << "            @{ Name = '-C'; Alias = '--context'; Description = 'Context lines' }\n";
    oss << "            @{ Name = '-i'; Alias = '--ignore-case'; Description = "
           "'Case-insensitive' }\n";
    oss << "            @{ Name = '-w'; Alias = '--word'; Description = 'Whole word match' }\n";
    oss << "            @{ Name = '-v'; Alias = '--invert'; Description = 'Invert match' }\n";
    oss << "            @{ Name = '-n'; Alias = '--line-numbers'; Description = 'Show line "
           "numbers' }\n";
    oss << "            @{ Name = '-c'; Alias = '--count'; Description = 'Count matches' }\n";
    oss << "            @{ Name = '-l'; Alias = '--files-with-matches'; Description = 'Files with "
           "matches' }\n";
    oss << "            @{ Name = '--color'; Description = 'Color output'; Values = "
           "@('always','never','auto') }\n";
    oss << "        )\n";

    // config command
    oss << "        'config' = @(\n";
    oss << "            @{ Name = '--format'; Description = 'Output format'; Values = "
           "@('toml','json') }\n";
    oss << "        )\n";

    // doctor command
    oss << "        'doctor' = @(\n";
    oss << "            @{ Name = '--fix-embeddings'; Description = 'Fix missing embeddings' }\n";
    oss << "            @{ Name = '--fix-fts5'; Description = 'Fix FTS5 index' }\n";
    oss << "            @{ Name = '--fix-graph'; Description = 'Fix knowledge graph' }\n";
    oss << "            @{ Name = '--fix-all'; Description = 'Fix all issues' }\n";
    oss << "            @{ Name = '--json'; Description = 'JSON output' }\n";
    oss << "        )\n";

    // repair command
    oss << "        'repair' = @(\n";
    oss << "            @{ Name = '--path-tree'; Description = 'Repair path tree' }\n";
    oss << "            @{ Name = '--optimize'; Description = 'Optimize database' }\n";
    oss << "            @{ Name = '--dry-run'; Description = 'Preview changes' }\n";
    oss << "        )\n";

    // status and stats commands
    oss << "        'status' = @(\n";
    oss << "            @{ Name = '--no-physical'; Description = 'Use daemon stats only without "
           "fallback scan' }\n";
    oss << "            @{ Name = '--corpus'; Description = 'Show corpus statistics for search "
           "tuning' }\n";
    oss << "        )\n";
    oss << "        'stats' = @(\n";
    oss << "            @{ Name = '--no-physical'; Description = 'Use daemon stats only without "
           "fallback scan' }\n";
    oss << "            @{ Name = '--corpus'; Description = 'Show corpus statistics for search "
           "tuning' }\n";
    oss << "        )\n";

    // auth command
    oss << "        'auth' = @(\n";
    oss << "            @{ Name = '--key-type'; Description = 'Key type'; Values = "
           "@('ed25519','rsa') }\n";
    oss << "            @{ Name = '--output'; Description = 'Output directory' }\n";
    oss << "            @{ Name = '--format'; Description = 'Output format'; Values = "
           "@('table','json') }\n";
    oss << "        )\n";

    // completion command
    oss << "        'completion' = @(\n";
    oss << "            @{ Name = 'bash'; Description = 'Bash completion' }\n";
    oss << "            @{ Name = 'zsh'; Description = 'Zsh completion' }\n";
    oss << "            @{ Name = 'fish'; Description = 'Fish completion' }\n";
    oss << "            @{ Name = 'powershell'; Description = 'PowerShell completion' }\n";
    oss << "        )\n";

    // tree command
    oss << "        'tree' = @(\n";
    oss << "            @{ Name = '--depth'; Alias = '-L'; Description = 'Max depth' }\n";
    oss << "            @{ Name = '--json'; Description = 'JSON output' }\n";
    oss << "        )\n";

    // diff command
    oss << "        'diff' = @(\n";
    oss << "            @{ Name = '--format'; Description = 'Output format'; Values = "
           "@('unified','json') }\n";
    oss << "        )\n";

    oss << "    }\n\n";

    oss << "    function Resolve-YamsCommandPath {\n";
    oss << "        param($Tokens, $SubcommandMap)\n";
    oss << "        $pathParts = @()\n";
    oss << "        $currentPath = ''\n";
    oss << "        foreach ($token in $Tokens) {\n";
    oss << "            if ($token -like '-*') {\n";
    oss << "                continue\n";
    oss << "            }\n";
    oss << "            if (-not $SubcommandMap.ContainsKey($currentPath)) {\n";
    oss << "                break\n";
    oss << "            }\n";
    oss << "            $names = @($SubcommandMap[$currentPath] | ForEach-Object { $_.Name })\n";
    oss << "            if ($names -contains $token) {\n";
    oss << "                $pathParts += $token\n";
    oss << "                $currentPath = [string]::Join(' ', $pathParts)\n";
    oss << "            } else {\n";
    oss << "                break\n";
    oss << "            }\n";
    oss << "        }\n";
    oss << "        return $currentPath\n";
    oss << "    }\n\n";

    oss << "    function Normalize-YamsPath {\n";
    oss << "        param([string]$Path)\n";
    oss << "        if ($Path -eq 'plugins' -or $Path.StartsWith('plugins ')) {\n";
    oss << "            return ('plugin' + $Path.Substring('plugins'.Length))\n";
    oss << "        }\n";
    oss << "        return $Path\n";
    oss << "    }\n\n";

    oss << "    $elements = $commandAst.CommandElements\n";
    oss << "    $tokens = @()\n";
    oss << "    for ($i = 1; $i -lt $elements.Count; $i++) {\n";
    oss << "        $tokens += $elements[$i].Extent.Text\n";
    oss << "    }\n";
    oss << "    $commandPath = Normalize-YamsPath (Resolve-YamsCommandPath $tokens "
           "$subcommands)\n\n";
    oss << "    $prevElement = $null\n";
    oss << "    if ($elements.Count -ge 2) {\n";
    oss << "        $prevElement = $elements[$elements.Count - 2].Extent.Text\n";
    oss << "    }\n";

    oss << "    if ($prevElement) {\n";
    oss << "        $valueKey = \"$commandPath:$prevElement\"\n";
    oss << "        if ($valueHints.ContainsKey($valueKey)) {\n";
    oss << "            $valueHints[$valueKey] | Where-Object { $_ -like \"$wordToComplete*\" } | "
           "ForEach-Object {\n";
    oss << "                [System.Management.Automation.CompletionResult]::new($_, $_, "
           "'ParameterValue', $_)\n";
    oss << "            }\n";
    oss << "            return\n";
    oss << "        }\n";
    oss << "    }\n\n";

    oss << "    if ($wordToComplete -notlike '-*') {\n";
    oss << "        $positionalKey = \"$commandPath:__positional__\"\n";
    oss << "        if ($valueHints.ContainsKey($positionalKey)) {\n";
    oss << "            $valueHints[$positionalKey] | Where-Object { $_ -like \"$wordToComplete*\" "
           "} | ForEach-Object {\n";
    oss << "                [System.Management.Automation.CompletionResult]::new($_, $_, "
           "'ParameterValue', $_)\n";
    oss << "            }\n";
    oss << "            if (($valueHints[$positionalKey] | Where-Object { $_ -like "
           "\"$wordToComplete*\" }).Count -gt 0) {\n";
    oss << "                return\n";
    oss << "            }\n";
    oss << "        }\n";
    oss << "    }\n\n";

    oss << "    if ($wordToComplete -notlike '-*' -and $subcommands.ContainsKey($commandPath)) {\n";
    oss << "        $subcommands[$commandPath] | Where-Object { $_.Name -like \"$wordToComplete*\" "
           "} | "
           "ForEach-Object {\n";
    oss << "            [System.Management.Automation.CompletionResult]::new(\n";
    oss << "                $_.Name, $_.Name, 'ParameterValue', $_.Description\n";
    oss << "            )\n";
    oss << "        }\n";
    oss << "        if (($subcommands[$commandPath] | Where-Object { $_.Name -like "
           "\"$wordToComplete*\" }).Count -gt 0) {\n";
    oss << "            return\n";
    oss << "        }\n";
    oss << "    }\n\n";

    oss << "    if ($commandPath -eq 'completion' -and $wordToComplete -notlike '-*') {\n";
    oss << "        $commandFlags['completion'] | Where-Object { $_.Name -like "
           "\"$wordToComplete*\" } | ForEach-Object {\n";
    oss << "            [System.Management.Automation.CompletionResult]::new(\n";
    oss << "                $_.Name, $_.Name, 'ParameterValue', $_.Description\n";
    oss << "            )\n";
    oss << "        }\n";
    oss << "        return\n";
    oss << "    }\n\n";

    oss << "    # Complete flags and options\n";
    oss << "    if ($wordToComplete -like '-*' -or $wordToComplete -eq '') {\n";
    oss << "        $flags = @()\n";
    oss << "        $flags += $globalFlags\n";
    oss << "        if ($commandPath -and $commandFlags.ContainsKey($commandPath)) {\n";
    oss << "            $flags += $commandFlags[$commandPath]\n";
    oss << "        }\n";
    oss << "        $flags | Where-Object { $_.Name -like \"$wordToComplete*\" } | ForEach-Object "
           "{\n";
    oss << "            [System.Management.Automation.CompletionResult]::new(\n";
    oss << "                $_.Name, $_.Name, 'ParameterName', $_.Description\n";
    oss << "            )\n";
    oss << "        }\n";
    oss << "    }\n\n";

    oss << "    # Complete flag values\n";
    oss << "    if ($prevElement -and $commandPath -and $commandFlags.ContainsKey($commandPath)) "
           "{\n";
    oss << "        $flag = $commandFlags[$commandPath] | Where-Object { $_.Name -eq $prevElement "
           "-or $_.Alias -eq $prevElement }\n";
    oss << "        if ($flag -and $flag.Values) {\n";
    oss << "            $flag.Values | Where-Object { $_ -like \"$wordToComplete*\" } | "
           "ForEach-Object {\n";
    oss << "                [System.Management.Automation.CompletionResult]::new($_, $_, "
           "'ParameterValue', $_)\n";
    oss << "            }\n";
    oss << "        }\n";
    oss << "    }\n";
    oss << "}\n";

    return oss.str();
}

// Factory function
std::unique_ptr<ICommand> createCompletionCommand() {
    return std::make_unique<CompletionCommand>();
}

} // namespace yams::cli
