#pragma once

#include <functional>
#include <string>

#include <yams/cli/tui/browse_state.hpp>

namespace yams::cli::tui {

/**
 * CommandActions
 *
 * Optional callbacks that allow command execution to trigger side effects
 * outside of pure state mutation. This decouples the command parser from
 * I/O operations such as spawning a pager or performing data refreshes.
 */
struct CommandActions {
    // Open the selected document in an external pager.
    // Should return true on success. The implementation may update
    // BrowseState::statusMessage on failure or success.
    std::function<bool(BrowseState&)> openPager;

    // Refresh the document list and/or related data.
    // The implementation should update the state as necessary and set a
    // user-friendly status message if appropriate.
    std::function<void(BrowseState&)> refresh;

    // Show help overlay. If not provided, execute() will set state.showHelp = true.
    std::function<void(BrowseState&)> showHelp;
};

/**
 * BrowseCommands
 *
 * Parses and executes colon-prefixed commands in the TUI browser's command mode.
 *
 * Recognized commands (baseline):
 * - q, quit, wq       : Exit the TUI (exit_requested = true)
 * - help              : Show help overlay
 * - open              : Open selected document in external pager (uses actions.openPager if
 * provided)
 * - hex               : Switch preview mode to Hex
 * - text              : Switch preview mode to Text
 * - refresh           : Refresh listings (uses actions.refresh if provided)
 *
 * Notes:
 * - Unknown commands will return false and set a helpful status message.
 * - The execute function tries to keep side effects minimal; it primarily
 *   mutates BrowseState and defers I/O to CommandActions where applicable.
 */
class BrowseCommands {
public:
    /**
     * Execute a colon-style command.
     *
     * @param cmd             Command string without leading ':' (e.g., "open", "q", "refresh").
     * @param state           Browser UI state to mutate based on the command.
     * @param exit_requested  Set to true if the command requests exiting the TUI.
     * @param actions         Optional side-effect implementations for commands that need I/O.
     * @return                true if a known command was executed; false if unknown.
     */
    bool execute(const std::string& cmd, BrowseState& state, bool& exit_requested,
                 const CommandActions& actions = {});
};

} // namespace yams::cli::tui