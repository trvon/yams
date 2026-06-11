#pragma once

// Umbrella header for the daemon IPC protocol. Prefer including the
// narrower part headers when only a subset of the protocol is needed:
//   - ipc_protocol_common.h    serialization concepts and helpers
//   - ipc_protocol_requests.h  request structs and the Request variant
//   - ipc_protocol_responses.h response structs and the Response variant
//   - ipc_protocol_envelope.h  batch types, Message envelope, MessageType

#include <yams/daemon/ipc/ipc_protocol_common.h>
#include <yams/daemon/ipc/ipc_protocol_envelope.h>
#include <yams/daemon/ipc/ipc_protocol_requests.h>
#include <yams/daemon/ipc/ipc_protocol_responses.h>
