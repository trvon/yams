# Architecture: Daemon and Services

This document describes the YAMS daemon runtime strictly in terms of its implementation. Each subsection references the concrete source files that execute the behavior.

## Runtime Responsibilities (`src/daemon/daemon.cpp`)

- `YamsDaemon::YamsDaemon` resolves socket, PID, and log paths, seeds `YAMS_IN_DAEMON`, and installs plugin directory configuration before any service starts.
- `YamsDaemon::start` drives a phased boot sequence: reset the lifecycle FSM, initialize `LifecycleComponent`, start `TuningManager`, bring up `ServiceManager`, and finally bind the IPC socket via `SocketServer`.
- `YamsDaemon::stop` shuts down the IPC server, stops services, and clears lifecycle state, ensuring `running_` flips false exactly once.

## Service Coordination (`src/daemon/components/ServiceManager.cpp`)

- `ServiceManager::startAll` instantiates the content store, metadata database, vector backend, and ingestion services, wiring them through a shared `WorkerPool`.
- The manager also exposes `ServiceManager::autoloadPluginsNow`, which consults the trust store and loads eligible plugins via the `PluginLoader`.
- Maintenance helpers (`ServiceManager::scheduleMaintenance`, `ServiceManager::shutdownAll`) back pressure ingestion and search workers before teardown.

## Lifecycle State Machine (`src/daemon/components/DaemonLifecycleFsm.cpp`)

- The FSM enumerates states such as `Starting`, `Ready`, `Draining`, and `Stopped`; transitions are triggered by explicit events from `YamsDaemon::start`/`stop` and watchdogs.
- Each transition publishes `LifecycleEvent` objects that `DaemonMetrics` ingests to expose readiness levels to clients.

## Metrics and Health (`src/daemon/components/DaemonMetrics.cpp`)

- `DaemonMetrics::recordStartupPhase` attaches timestamps to each startup stage and surfaces them through IPC `Status` replies.
- `DaemonMetrics::snapshot` aggregates worker pool depth, pending ingest counts, vector backlogs, and plugin status so the CLI and API can render health summaries.

## IPC Contract (`include/yams/daemon/ipc/ipc_protocol.h`, `src/daemon/ipc/ipc_protocol.cpp`)

- `IpcEnvelope` defines message framing, version headers, and payload routing identifiers.
- `serialize_envelope`/`deserialize_envelope` enforce length-prefix encoding and emit typed errors used by `connection_fsm` when envelopes are malformed or time out.

## Plugin Hosting (`src/daemon/resource/*.cpp`)

- `PluginLoader::loadPlugin` (`plugin_loader.cpp`) dlopens shared objects, inspects exported factory symbols, and rejects libraries that expose neither model providers nor graph adapters.
- `PluginHost::trust` and `PluginHost::enumerateTrusted` (`plugin_host.cpp`) maintain the on-disk trust manifest and gate which plugin paths `ServiceManager::autoloadPluginsNow` will admit.
- `AbiPluginLoader` and `WasmRuntime` construct ABI shims (`abi_plugin_loader.cpp`) or WASM instances (`wasm_runtime.cpp`) to bind plugin callbacks into daemon services.

Keep this document synchronized with any associated traceability artifacts when responsibilities move or new subsystems are introduced.
