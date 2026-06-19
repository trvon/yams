# Daemon Download Jobs

## Goal

Expose download tracking and resume to daemon clients without turning the daemon into a general
purpose network fetch proxy.

The intended users are agents and automation flows that archive external artifacts into YAMS CAS
and need:

- durable job identity
- progress/status visibility
- resumable downloads across daemon restarts
- optional post-indexing into the existing content pipeline

The design explicitly does not start with arbitrary remote download capability.

## Current State

### Implemented Now

- The downloader subsystem already supports:
  - probe + range GET
  - ETag-based resume
  - staging file reopen and partial range persistence
  - CAS finalization
  - optional export
- The current resume store is local-manager scoped and keyed by URL in
  [src/downloader/download_manager.cpp](../../src/downloader/download_manager.cpp).
- The daemon exposes a guarded download path in
  [src/daemon/components/dispatcher/request_dispatcher_documents.cpp](../../src/daemon/components/dispatcher/request_dispatcher_documents.cpp)
  behind `YAMS_ENABLE_DAEMON_DOWNLOAD`.
- The daemon now enforces the narrow policy boundary before network activity:
  - feature gate enabled
  - allowed scheme
  - allowed host
  - checksum requirement
  - store-only enforcement
  - checksum format validation
- Accepted daemon downloads are backgrounded as tracked jobs with:
  - `jobId`
  - persisted job metadata
  - `queued/running/completed/failed/canceled` state transitions
  - status polling and list APIs
  - cooperative cancel signaling
- The CLI exposes daemon job controls in
  [src/cli/commands/download_command.cpp](../../src/cli/commands/download_command.cpp):
  - `yams download <url>`
  - `yams download --status <job-id>`
  - `yams download --list-jobs`
  - `yams download --cancel <job-id>`
- MCP exposes the same read/control surface in
  [src/mcp/mcp_server.cpp](../../src/mcp/mcp_server.cpp):
  - `download_jobs` with `action=status|list|cancel`

### Still Missing

- true job-oriented start/resume request types distinct from the compatibility `DownloadRequest`
- `maxFileBytes` enforcement
- `rateLimitRps` enforcement
- sandbox/process isolation for daemon downloads
- resume-by-`jobId` semantics stronger than the downloader's URL-based resume store
- richer ownership/quota controls

## Problem

If the daemon directly exposes the current downloader request shape, the daemon becomes a network
boundary with new risk:

- SSRF and internal host reachability
- credential or custom-header abuse
- disk exhaustion via large or repeated downloads
- long-lived network tasks consuming daemon capacity
- cross-agent state collision when resume state is keyed only by URL
- arbitrary filesystem writes if export behavior leaks through

That is too much surface for a first daemonized version.

## Design Principles

1. Keep the daemon surface narrower than the local CLI/MCP surface.
2. Treat downloads as managed jobs, not one-shot unary fetches.
3. Enforce policy before any network activity.
4. Keep first version store-only and CAS-rooted.
5. Use durable daemon job IDs, not URL identity, for tracking and resume.
6. Reuse the downloader manager internally rather than reimplement transport.

## Target Model

### Public Daemon API

The code currently uses a compatibility `DownloadRequest` plus separate status/list/cancel calls.
The intended end state is a fully job-oriented API instead of expanding the unary request forever.

- `StartDownloadJobRequest`
- `GetDownloadJobStatusRequest`
- `CancelDownloadJobRequest`
- `ResumeDownloadJobRequest`
- `ListDownloadJobsRequest`

Matching responses/events:

- `StartDownloadJobResponse`
- `DownloadJobStatusResponse`
- `CancelDownloadJobResponse`
- `ResumeDownloadJobResponse`
- `ListDownloadJobsResponse`
- optional `DownloadJobEvent`

### Job States

- `queued`
- `probing`
- `downloading`
- `verifying`
- `finalizing`
- `indexing`
- `completed`
- `failed`
- `cancelled`
- `blocked_policy`

### Job Record

Persist a daemon-owned record for each job:

- `download_id`
- `owner` or caller identity
- canonical URL
- request fingerprint
- policy decision snapshot
- created/updated timestamps
- bytes downloaded / total bytes
- attempt count
- ETag / Last-Modified
- CAS hash when completed
- last error
- indexing outcome

The downloader resume store can remain an implementation detail, but daemon logic should key work by
`download_id`.

## Request Boundary

### First Version Allowed Inputs

- `url`
- optional tags / metadata
- optional collection / snapshot indexing metadata

### First Version Rejected Inputs

- custom headers
- proxy
- TLS insecure / custom CA
- export path / export dir
- overwrite controls
- arbitrary retry and transport tuning

Those can remain local CLI or MCP-only until the daemon has a stronger policy and audit model.

## Policy Enforcement

Use the existing `DaemonConfig::DownloadPolicy` in
[include/yams/daemon/daemon.h](../../include/yams/daemon/daemon.h), but
enforce it in the handler before invoking the download service.

Implemented checks today:

- feature gate must be enabled
- allowed scheme check
- allowed host check
- checksum requirement check
- store-only hard enforcement

Not implemented yet:

- max file bytes enforcement
- rate limit enforcement
- sandbox strategy selection

Recommended defaults:

- `enable=false`
- `allowedSchemes=["https"]`
- `requireChecksum=true`
- `storeOnly=true`
- no export

## Resume Identity

The current downloader resume identity is effectively URL + ETag. That is not sufficient for
multi-agent daemon use.

Daemon job fingerprint should include:

- canonical URL
- effective caller identity
- checksum requirement / checksum value
- headers/auth fingerprint if those are ever supported
- store-only/export mode

Resume should only continue if:

- job fingerprint matches
- server validators are still compatible
- policy still allows the request

## Execution Model

### Implemented Baseline

- daemon-owned background job runner
- progress/status polling via status requests
- durable job metadata persisted by daemon
- cooperative cancellation support

### Remaining Phases

- stronger bounded concurrency / quotas
- resume across daemon restart by job identity, not only downloader-local URL resume
- optional event streaming
- richer policy
- controlled auth/header support
- agent/job quotas

## Security Boundaries

The daemon download path should be treated as its own trust boundary.

Threats to design against:

- SSRF to loopback, link-local, RFC1918, metadata endpoints
- open redirect escaping host allowlists
- oversized or unbounded bodies
- validator confusion on resumed jobs
- multi-tenant job collision
- content-type deception and indexing abuse
- progress/event amplification

Out of scope for first version:

- arbitrary authenticated internet fetches
- user-specified export to filesystem paths
- browser-equivalent cookie/session behavior

## Integration with Existing Services

Use the existing downloader stack:

- `IDownloadManager`
- `IResumeStore`
- `IDiskWriter`
- `IHttpAdapter`

Use existing indexing flow after successful CAS finalize:

- add document metadata
- apply tags and metadata
- optional collection/snapshot placement

## Rollout Plan

Completed:

1. Enforce current download policy in the existing daemon handler.
2. Add job record type and persistence.
3. Add `start/status/list` job API.
4. Add `cancel`.

Remaining:

1. Move the current handler to a compatibility wrapper or deprecate it.
2. Add resume-by-job.
3. Enforce `maxFileBytes`, rate limits, and sandboxing.
4. Only then consider broadening request shape.

## Open Questions

- What daemon caller identity is available for job ownership and quotas?
- Should checksum be mandatory for all daemon downloads or only for non-allowlisted hosts?
- Should MCP use the daemon job path later, or remain a separate local download path?
- Where should durable download-job state live: metadata DB, dedicated table, or sidecar state?
