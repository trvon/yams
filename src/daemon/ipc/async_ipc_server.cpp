#include <yams/daemon/ipc/async_ipc_server.h>
#include <yams/daemon/ipc/streaming_processor.h>

#include <spdlog/spdlog.h>
#include <yams/profiling.h>

#include <cerrno>
#include <coroutine>
#include <cstring>
#include <future>
#include <optional>
#include <string>

#ifdef _WIN32
#include <afunix.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#ifndef SHUT_RDWR
#define SHUT_RDWR SD_BOTH
#endif
// Provide minimal fcntl-style nonblocking handling only within this TU
#ifndef F_GETFL
#define F_GETFL 0
#endif
#ifndef F_SETFL
#define F_SETFL 1
#endif
#ifndef O_NONBLOCK
#define O_NONBLOCK 0x800
#endif
namespace yams::daemon::detail {
inline int win32_fcntl(int fd, int cmd, int arg = 0) {
    if (cmd == F_SETFL) {
        u_long mode = (arg & O_NONBLOCK) ? 1 : 0;
        return ::ioctlsocket(fd, FIONBIO, &mode) == 0 ? 0 : -1;
    }
    // F_GETFL: return 0 (no flags)
    return 0;
}
} // namespace yams::daemon::detail
#define fcntl(fd, cmd, ...) yams::daemon::detail::win32_fcntl((fd), (cmd), ##__VA_ARGS__)
#define close(fd) ::closesocket(fd)
#else
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#endif

// No out-of-line template definitions here; see header for template bodies.