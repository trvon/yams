#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>

namespace yams::daemon {

boost::asio::awaitable<Response>
RequestDispatcher::handleListSessionsRequest(const ListSessionsRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "list_sessions", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto sessionService = app::services::makeSessionService(&appContext);
            auto sessions = sessionService->listSessions();
            auto current = sessionService->current();
            ListSessionsResponse resp;
            for (const auto& s : sessions) {
                resp.session_names.push_back(s);
            }
            if (current) {
                resp.current_session = *current;
            }
            co_return resp;
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleUseSessionRequest(const UseSessionRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "use_session", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto sessionService = app::services::makeSessionService(&appContext);
            if (!sessionService->exists(req.session_name)) {
                co_return ErrorResponse{ErrorCode::NotFound, "Session not found"};
            }
            sessionService->use(req.session_name);
            co_return SuccessResponse{"OK"};
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleAddPathSelectorRequest(const AddPathSelectorRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "add_path_selector", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto sessionService = app::services::makeSessionService(&appContext);
            std::string sessionName = req.session_name;
            if (sessionName.empty()) {
                auto current = sessionService->current();
                if (current) {
                    sessionName = *current;
                } else {
                    co_return ErrorResponse{ErrorCode::InvalidState, "No active session"};
                }
            }
            if (!sessionService->exists(sessionName)) {
                co_return ErrorResponse{ErrorCode::NotFound, "Session not found"};
            }
            std::vector<std::pair<std::string, std::string>> meta;
            for (const auto& kv : req.metadata) {
                meta.push_back({kv.first, kv.second});
            }
            sessionService->addPathSelector(req.path, req.tags, meta);
            co_return SuccessResponse{"OK"};
        });
}

boost::asio::awaitable<Response>
RequestDispatcher::handleRemovePathSelectorRequest(const RemovePathSelectorRequest& req) {
    co_return co_await yams::daemon::dispatch::guard_await(
        "remove_path_selector", [this, req]() -> boost::asio::awaitable<Response> {
            auto appContext = serviceManager_->getAppContext();
            auto sessionService = app::services::makeSessionService(&appContext);
            std::string sessionName = req.session_name;
            if (sessionName.empty()) {
                auto current = sessionService->current();
                if (current) {
                    sessionName = *current;
                } else {
                    co_return ErrorResponse{ErrorCode::InvalidState, "No active session"};
                }
            }
            if (!sessionService->exists(sessionName)) {
                co_return ErrorResponse{ErrorCode::NotFound, "Session not found"};
            }
            sessionService->removePathSelector(req.path);
            co_return SuccessResponse{"OK"};
        });
}

} // namespace yams::daemon
