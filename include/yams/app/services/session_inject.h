#pragma once

#include <string>
#include <type_traits>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::app::services {

/// Inject session into a daemon request based on its type.
/// SearchRequest/GrepRequest: sets useSession + sessionName
/// ListRequest/AddDocumentRequest/DeleteRequest/UpdateDocumentRequest: sets sessionId
/// Others: no-op
template <typename Req> void injectSession(Req& req, const std::string& session) {
    if (session.empty())
        return;

    if constexpr (std::is_same_v<Req, yams::daemon::SearchRequest>) {
        req.useSession = true;
        req.sessionName = session;
    } else if constexpr (std::is_same_v<Req, yams::daemon::GrepRequest>) {
        req.useSession = true;
        req.sessionName = session;
    } else if constexpr (std::is_same_v<Req, yams::daemon::ListRequest>) {
        req.sessionId = session;
    } else if constexpr (std::is_same_v<Req, yams::daemon::AddDocumentRequest>) {
        req.sessionId = session;
    } else if constexpr (std::is_same_v<Req, yams::daemon::DeleteRequest>) {
        req.sessionId = session;
    } else if constexpr (std::is_same_v<Req, yams::daemon::UpdateDocumentRequest>) {
        // UpdateDocumentRequest doesn't have sessionId yet — no-op for now
    }
    // All other request types: no-op
}

} // namespace yams::app::services
