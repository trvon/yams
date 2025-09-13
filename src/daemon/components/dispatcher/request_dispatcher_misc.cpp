// Misc small handlers split from RequestDispatcher.cpp
#include <chrono>
#include <yams/daemon/components/RequestDispatcher.h>

namespace yams::daemon {

boost::asio::awaitable<Response> RequestDispatcher::handlePingRequest(const PingRequest& /*req*/) {
    PongResponse res;
    res.serverTime = std::chrono::steady_clock::now();
    co_return res;
}

} // namespace yams::daemon
