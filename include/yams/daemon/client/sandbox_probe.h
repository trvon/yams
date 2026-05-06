#pragma once

#include <chrono>
#include <functional>

namespace yams::daemon {

bool unix_socket_io_permitted();

using UnixSocketProbeFn = std::function<bool()>;

void set_unix_socket_probe_for_tests(UnixSocketProbeFn probe);

std::chrono::nanoseconds last_unix_socket_probe_duration();

} // namespace yams::daemon
