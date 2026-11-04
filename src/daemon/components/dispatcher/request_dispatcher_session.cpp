#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>

namespace yams::daemon {

boost::asio::awaitable<Response>
RequestDispatcher::handleUseSessionRequest(const UseSessionRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "use_session", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto sessionService = app::services::makeSessionService(&appContext);
            if (!sessionService->exists(req.session_name)) {
                co_return dispatch::makeErrorResponse(ErrorCode::NotFound, "Session not found");
            }
            sessionService->use(req.session_name);
            co_return SuccessResponse{"OK"};
        });
}

} // namespace yams::daemon
