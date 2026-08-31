#pragma once

#include <yams/daemon/client/client_config.h>

namespace yams::daemon {

ClientTransportMode resolve_transport_mode(const ClientConfig& config);

} // namespace yams::daemon
