#pragma once

namespace yams::app::services {

enum class GrepExecMode {
    Unset = -1,
    Auto = 0,
    HotOnly = 1,
    ColdOnly = 2,
};

// Thread-local override for grep execution mode used by daemon streaming fast/full paths.
void set_grep_mode_tls(GrepExecMode mode) noexcept;
GrepExecMode get_grep_mode_tls() noexcept;

} // namespace yams::app::services
