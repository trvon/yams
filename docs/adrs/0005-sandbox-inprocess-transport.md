# ADR-0005: Dual-Mode Runtime with In-Process Transport Shim

- Status: Accepted
- Date: 2026-02-24
- Decision Makers: @trvon
- Links: yams-sandbox-mode

## Context

YAMS currently assumes a daemon process is reachable over a Unix domain socket.
This works in normal local development but fails in constrained environments
like Codex sandboxes and some container setups where daemon startup and/or
socket IPC is unavailable.

The existing public integration path (`DaemonClient`) is widely used by CLI and
MCP code. Replacing all callers with direct service access would be invasive.

## Decision

Implement dual-mode operation with a transport shim:

- Keep `DaemonClient` as the stable client API.
- Introduce a transport abstraction used by `DaemonClient`.
- Add an in-process transport implementation that routes requests to a local
  `RequestDispatcher` backed by `ServiceManager`.
- Preserve socket transport as the default for standard daemon deployments.

### Chosen Design Details

1. **Lifecycle decoupling via interface (Option B)**
   - Replace `RequestDispatcher`'s hard dependency on `YamsDaemon*` with an
     `IDaemonLifecycle` interface.
   - Keep current daemon behavior through a concrete adapter.
   - Allow embedded mode to provide an alternate lifecycle implementation.

2. **Dedicated embedded runtime context**
   - Embedded mode owns a dedicated `boost::asio::io_context` and threads.
   - It does not reuse global client IO context.

3. **Process-lifetime persistence**
   - Embedded runtime is a process singleton keyed by data directory.
   - Services stay warm for process lifetime rather than request lifetime.

## Implementation Plan

### Phase 1: Interface Extraction

- Add `IDaemonLifecycle` with required behaviors:
  - lifecycle snapshot access
  - subsystem degradation signaling
  - document removal event hook
  - shutdown initiation
- Update `RequestDispatcher` and split handler files to use `IDaemonLifecycle`.
- Add `DaemonLifecycleAdapter` for normal daemon mode.

### Phase 2: Transport Abstraction

- Add `IClientTransport` abstraction for `DaemonClient`.
- Make `AsioTransportAdapter` implement `IClientTransport`.

### Phase 3: Embedded Runtime

- Add `EmbeddedServiceHost`:
  - owns `ServiceManager`, `StateComponent`, lifecycle implementation
  - owns dedicated `io_context` thread pool
  - starts async initialization and waits for terminal readiness
- Add `InProcessTransport`:
  - dispatches `Request` directly through `RequestDispatcher`
  - bypasses framing/socket layers

### Phase 4: Mode Selection

- Add sandbox detection (`YAMS_EMBEDDED`, config `[daemon].mode`).
- Support `socket`, `embedded`, and `auto` resolution.
- Wire transport selection in `DaemonClient`.

## Consequences

### Positive

- CLI and MCP can run without external daemon/socket in sandboxed environments.
- Existing `DaemonClient` callers remain stable.
- Request path retains shared request/response model (`Request`/`Response`).

### Tradeoffs

- Additional lifecycle complexity (socket mode + embedded mode).
- Embedded mode startup can be heavier on first request.
- Streaming semantics in embedded path are simplified initially.

## Non-Goals

- No behavior change to standard daemon socket mode.
- No immediate rearchitecture of all daemon internals.
- No WASI runtime unification in this ADR.
