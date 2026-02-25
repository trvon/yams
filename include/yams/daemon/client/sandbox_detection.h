#pragma once

#include <yams/daemon/client/daemon_client.h>

namespace yams::daemon {

ClientTransportMode resolve_transport_mode(const ClientConfig& config);

} // namespace yams::daemon
