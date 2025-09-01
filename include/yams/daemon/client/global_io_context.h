#pragma once

#include <yams/daemon/ipc/async_socket.h>
#include <thread>

namespace yams::daemon {

class GlobalIOContext {
public:
    static GlobalIOContext& instance();

    yams::daemon::AsyncIOContext& get_io_context();

private:
    GlobalIOContext();
    ~GlobalIOContext();

    yams::daemon::AsyncIOContext io_context_;
    std::thread io_thread_;
};

} // namespace yams::daemon
