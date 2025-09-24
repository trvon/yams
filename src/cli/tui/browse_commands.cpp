#include <yams/cli/tui/browse_commands.hpp>
#include <yams/cli/tui/browse_state.hpp>

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace yams::cli::tui {

namespace {

// Trim helpers
inline void ltrim(std::string& s) {
    s.erase(s.begin(),
            std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
}
inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}
inline void trim(std::string& s) {
    ltrim(s);
    rtrim(s);
}

inline std::string toLower(std::string_view sv) {
    std::string out;
    out.reserve(sv.size());
    for (char c : sv)
        out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    return out;
}

} // namespace

bool BrowseCommands::execute(const std::string& raw_cmd, BrowseState& state,
                             const CommandActions& actions) {
    // Normalize command: strip leading ':', trim spaces, lowercase
    std::string cmd = raw_cmd;
    if (!cmd.empty() && cmd.front() == ':') {
        cmd.erase(cmd.begin());
    }
    trim(cmd);
    if (cmd.empty()) {
        return false;
    }

    const std::string lc = toLower(cmd);

    // Quit commands
    if (lc == "q" || lc == "q!" || lc == "quit" || lc == "wq") {
        state.quit();
        return true;
    }

    // Help
    if (lc == "help" || lc == "h" || lc == "?") {
        if (actions.showHelp) {
            actions.showHelp(state);
        } else {
            state.showHelp = true;
        }
        return true;
    }

    // Refresh
    if (lc == "refresh" || lc == "reload") {
        if (actions.refresh) {
            actions.refresh(state);
        } else {
            state.statusMessage = "Refresh not available";
        }
        return true;
    }

    // Open in external pager
    if (lc == "open" || lc == "pager") {
        if (actions.openPager) {
            bool ok = actions.openPager(state);
            if (!ok) {
                state.statusMessage = "Failed to open in pager";
            }
        } else {
            state.statusMessage = "Pager not available";
        }
        return true;
    }

    // Preview mode controls
    if (lc == "hex") {
        state.previewMode = PreviewMode::Hex;
        state.previewScrollOffset = 0;
        state.viewerScrollOffset = 0;
        state.statusMessage = "Preview mode: hex";
        return true;
    }
    if (lc == "text") {
        state.previewMode = PreviewMode::Text;
        state.previewScrollOffset = 0;
        state.viewerScrollOffset = 0;
        state.statusMessage = "Preview mode: text";
        return true;
    }
    if (lc == "auto") {
        state.previewMode = PreviewMode::Auto;
        state.previewScrollOffset = 0;
        state.viewerScrollOffset = 0;
        state.statusMessage = "Preview mode: auto";
        return true;
    }

    // Viewer options (optional ergonomics)
    if (lc == "wrap") {
        state.viewerWrap = true;
        state.statusMessage = "Viewer: wrap enabled";
        return true;
    }
    if (lc == "nowrap") {
        state.viewerWrap = false;
        state.statusMessage = "Viewer: wrap disabled";
        return true;
    }

    // Unknown command
    return false;
}

} // namespace yams::cli::tui
