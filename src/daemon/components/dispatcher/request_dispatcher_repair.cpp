/// @file request_dispatcher_repair.cpp
/// @brief RequestDispatcher handler for RepairRequest — delegates to RepairService.

#include <yams/daemon/components/dispatch_response.hpp>
#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

#include <spdlog/spdlog.h>

namespace yams::daemon {

boost::asio::awaitable<Response> RequestDispatcher::handleRepairRequest(const RepairRequest& req) {
    (void)req;
    spdlog::warn("[RepairDispatcher] legacy unary repair path invoked; RepairRequest must use "
                 "streaming IPC");
    co_return dispatch::makeErrorResponse(
        ErrorCode::InvalidArgument,
        "RepairRequest requires streaming IPC; use the daemon client's streaming repair API");
}

} // namespace yams::daemon
