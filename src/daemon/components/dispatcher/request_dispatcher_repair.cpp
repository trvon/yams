/// @file request_dispatcher_repair.cpp
/// @brief RequestDispatcher handler for RepairRequest — delegates to RepairService.

#include <yams/daemon/components/RepairService.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

#include <spdlog/spdlog.h>

namespace yams::daemon {

boost::asio::awaitable<Response> RequestDispatcher::handleRepairRequest(const RepairRequest& req) {
    if (!serviceManager_) {
        co_return ErrorResponse{ErrorCode::NotInitialized, "ServiceManager not available"};
    }

    auto repairService = serviceManager_->getRepairServiceShared();
    if (!repairService) {
        co_return ErrorResponse{ErrorCode::NotInitialized,
                                "RepairService not running; is auto-repair enabled?"};
    }

    auto signal = getWorkerJobSignal();
    if (signal) {
        signal(true);
    }

    // Execute repair synchronously on this coroutine's executor.
    // The progress callback streams RepairEvent responses back through the
    // streaming response infrastructure (the client receives them via
    // ChunkedResponseHandler).
    RepairResponse finalResp;
    try {
        finalResp = repairService->executeRepair(req, [](const RepairEvent& /*ev*/) {
            // Events are captured by the caller via the streaming framework;
            // no explicit send needed here -- executeRepair already accumulates
            // results into the RepairResponse.
        });
    } catch (const std::exception& e) {
        spdlog::error("[RepairDispatcher] executeRepair threw: {}", e.what());
        finalResp.success = false;
        finalResp.errors.push_back(std::string("Internal error: ") + e.what());
    }

    if (signal) {
        signal(false);
    }

    co_return finalResp;
}

} // namespace yams::daemon
