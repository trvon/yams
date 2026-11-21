# TLA+ models

TLA+ models of two YAMS concurrency areas, checked with TLC. Each action in
a model cites the source lines it models (origin/main 7080fe645). A
configuration constant selects old or current behaviour, so TLC shows the
old bug and the current fix side by side.

| Model | Covers |
|-------|--------|
| `DaemonReadiness.tla` | daemon startup readiness gate and request dispatch, client reply mapping |
| `ClientIoReset.tla` | client connection pool, IO threads, `GlobalIOContext::reset()` lock |

## Running

    scripts/formal/check-tla.sh                       # every configuration
    scripts/formal/check-tla.sh DaemonReadiness_new   # one configuration

The script downloads `tla2tools.jar` v1.7.4 into
`${XDG_CACHE_HOME:-~/.cache}/yams/tla` on first use, checks its sha256, runs
TLC on every configuration and compares the outcome with the expected one
(pass, invariant violated, temporal property violated, or deadlock). It
exits non-zero on any mismatch. `TLA_TOOLS_JAR` points it at another copy of
the jar (same sha256); `TLC_WORKERS` sets `-workers` (default `auto`).

    tla2tools.jar  v1.7.4 (TLC 2.19)
    url     https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
    sha256  936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88

By hand:

    cd formal/tla
    java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -workers auto \
        -config DaemonReadiness_new.cfg DaemonReadiness.tla

The jar is not committed. Requires Java 11 or later.

## DaemonReadiness

One client sends Get, Status, Ping and AddDocument while the daemon starts
(content store and metadata repository come up in either order), commits
readiness to Ready or Degraded, may fail, and may shut down. The document
named by Get is stored before the daemon starts.

Constants:

- `DaemonFix`: the gate answers `ErrorResponse InvalidState` (0b48eed50)
  instead of a `StatusResponse`.
- `ClientFix`: `sendRequestStreaming` maps a `StatusResponse` to a
  non-status request into a not-ready error (0b48eed50) instead of passing
  it to `GetHandler`, which drops it.
- `IpcStopsFirst`: `stop()` closes the IPC layer before the database, as the
  code does. FALSE reverses the order.

Properties:

- `GetNeverNotFound`: a get of a stored document yields content, a
  not-ready error or a transport error, never NotFound.
- `StatusPingNeverGated`: Status and Ping are answered in every lifecycle
  state.
- `AddNeverRejectedWithStore`: AddDocument is never rejected for readiness
  once the content store is up.
- `NoAddEarly` (witness): violated when AddDocument reaches its handler with
  the content store up and the metadata repository not, which shows the
  early-accept path is reachable.
- `AllAnswered` (liveness, weak fairness on the request path): every request
  is answered.

| Configuration | Daemon | Client | Expected | Result |
|---------------|--------|--------|----------|--------|
| `_new` | fixed | fixed | pass | pass |
| `_daemonfix` | fixed | old (0.23.0) | pass | pass |
| `_clientfix` | old | fixed | pass | pass |
| `_old` | old | old | `GetNeverNotFound` violated | violated |
| `_witness` | fixed | fixed | `NoAddEarly` violated | violated |
| `_ipclate` | fixed, DB closed before IPC | fixed | `GetNeverNotFound` violated | violated |

Old counterexample (4 states): Get is sent before either service is up;
the gate answers `StatusResponse`; `GetHandler` drops it and returns an
empty `GetResponse`; `yams cat` prints "Document content not found".

`_ipclate` counterexample (10 states): both services up, stop requested,
Get passes the gate, the database clears its flag and drops the
repository, the handler's name lookup gets NotInitialized, which
`DocumentService::retrieve` reports as NotFound. The current stop order
(`shutdownIpcLayer()` before `ServiceManager::shutdown()`) closes the
connection first, so the client sees a transport error instead.

## ClientIoReset

Requests run as coroutines on `NIO` IO threads: each step takes a free IO
thread and returns it at the next `co_await`, except a blocking
`lock_shared()`, which keeps its thread. One `GlobalIOContext::reset()` runs
on a non-IO thread: it takes the operation lock exclusively, closes every
connection (`ConnectionRegistry::closeAll`, whose strand-dispatched close
needs a free IO thread), shuts down the registered pools and clears the
registry. Requests acquire from the pool, create connections under the
shared lock, write, wait for the reply, and retry once on an acquire
failure or EOF, as `AsioTransportAdapter::send_request_streaming` does.

Constants:

- `FailFast`: `create_connection()` uses `try_acquire_operation_guard()` and
  `reset()` announces itself first (6d5c9510a), instead of a blocking
  `acquire_operation_guard()`.
- `WriterPref`: a waiting writer stops new readers (libc++, Windows SRW) or
  not (glibc).
- `Recycle`: finished requests start again, for unbounded load.
- `AnnounceFirst`: hypothetical, not in the code. See Findings.

Properties:

- `NoIOThreadBlocked`: no IO thread waits on the reset lock.
- `LockAccounting`: shared holders are exactly the in-flight creations.
- `InUseOwned`, `AllReleased`: `in_use` is held only between acquire and
  write; once every request ends, no connection is in use.
- `NoStaleAfterReset`: once `reset()` returns, no connection made before it
  is open, and no request succeeds on one.
- `NoShutdownPoolUse`: no request uses an open connection of a shut-down
  pool.
- Deadlock freedom (TLC default).
- `RequestsEnd` / `RequestsEndRecycle`: every request ends (with weak
  fairness per request).
- `ResetReturns`: `reset()` returns (weak fairness, strong fairness on taking
  the lock).

| Configuration | Code | Lock | Load | Expected | Result |
|---------------|------|------|------|----------|--------|
| `_new_wp` | current | writer-pref | 3 requests | pass | pass |
| `_new_rp` | current | reader-pref | 3 requests | pass | pass |
| `_new_wp_load` | current | writer-pref | 2, recycled | pass | pass |
| `_new_rp_load` | current | reader-pref | 2, recycled | `ResetReturns` violated | violated |
| `_announce_rp_load` | AnnounceFirst | reader-pref | 2, recycled | pass | pass |
| `_old_wp` | before 6d5c9510a | writer-pref | 3 requests | deadlock | deadlock |
| `_old_rp` | before 6d5c9510a | reader-pref | 3 requests | deadlock | deadlock |

All with `NIO = 2`, `MaxRetry = 1`.

`_old_wp` counterexample (5 states, the macOS CI hang): r1 holds the shared
lock across its connect; `reset()` waits for the exclusive lock; r2 and r3
block both IO threads in `lock_shared()` behind the waiting writer; r1's
connect completion has no thread to run on.

`_old_rp` counterexample (7 states): r1 holds an open connection; `reset()` takes the
lock; r2 and r3 block both IO threads in `lock_shared()`; `closeAll()`
dispatches r1's socket close to its strand and waits for it, but no IO
thread is free. Reader-preferring locks do not avoid this one.

## State counts

TLC 2.19, `-workers auto`, Apple Silicon Mac. Runs that stop at a
violation report the states explored so far. Those counts, and the depth
reported with several workers, vary a little between runs.

| Configuration | Generated | Distinct | Depth | Time |
|---------------|-----------|----------|-------|------|
| `DaemonReadiness_new` | 313,583 | 103,388 | 22-23 | 6 s |
| `DaemonReadiness_daemonfix` | 313,583 | 103,388 | 22-23 | 6 s |
| `DaemonReadiness_clientfix` | 313,583 | 103,388 | 22-23 | 6 s |
| `DaemonReadiness_old` | 1,742 | 859 | 8 | 1 s |
| `DaemonReadiness_witness` | 942 | 531 | 8 | 1 s |
| `DaemonReadiness_ipclate` | 33,830 | 11,959 | 12 | 1 s |
| `ClientIoReset_new_wp` | 9,914,976 | 3,861,707 | 32 | 3 min 25 s |
| `ClientIoReset_new_rp` | 9,917,142 | 3,861,707 | 32 | 3 min 47 s |
| `ClientIoReset_new_wp_load` | 77,725 | 28,932 | 27 | 3 s |
| `ClientIoReset_announce_rp_load` | 77,725 | 28,932 | 27 | 3 s |
| `ClientIoReset_new_rp_load` | 77,809 | 28,932 | - | 3 s |
| `ClientIoReset_old_wp` | 16,038 | 8,002 | 10 | 1 s |
| `ClientIoReset_old_rp` | 14,405 | 6,806 | 11 | 1 s |

The full script takes about 9 minutes.

## Findings

`_new_rp_load`: with a reader-preferring `shared_mutex` (libstdc++ on glibc)
`reset()` can wait indefinitely while creations keep overlapping, for
example clients that keep reconnecting to a stopped daemon.
`try_acquire_operation_guard()` reads `resets_in_progress_` only after
`try_lock_shared()` fails, and on glibc that call succeeds while a writer
waits. Each creation holds the shared lock across its connect and backoff,
so the lock never drains. This is a liveness gap, not a deadlock: it ends
when the creations stop. `AnnounceFirst` (check the counter before
`try_lock_shared`) passes the same configuration.

## Limits

- One reset, two pools, one client connection per request; no daemon-side
  connection drops beyond reset, no request timeouts (the activity timeout
  in `asio_transport.cpp` bounds the real waits).
- `shared_mutex` is modeled as writer- or reader-preferring with no
  spurious `try_lock_shared` success; spurious failure is modeled.
- Readiness flags are read once in `RequestDispatcher::dispatch`, as the
  code does; the handler reads the repository later.
- GetStats, Shutdown and the remaining request kinds are not modeled.
