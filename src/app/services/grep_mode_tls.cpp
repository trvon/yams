#include <yams/app/services/grep_mode_tls.h>

namespace yams::app::services {

static thread_local GrepExecMode t_mode = GrepExecMode::Unset;

void set_grep_mode_tls(GrepExecMode mode) noexcept { t_mode = mode; }
GrepExecMode get_grep_mode_tls() noexcept { return t_mode; }

} // namespace yams::app::services

