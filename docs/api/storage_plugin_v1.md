---
title: Object Storage Plugin v1 — API and Capability Matrix
---

# Object Storage Plugin v1

!!! note "Draft"
    This API is under development and may change before stabilization.

## Overview

Provider-agnostic plugin API for object storage used by DR and hosting. Backends expose capabilities; core negotiates behavior. The current production path is S3-compatible.

## Key Interfaces

| Interface | Header | Description |
|-----------|--------|-------------|
| C ABI | `include/yams/plugins/object_storage_v1.h` | Stable loader surface |
| C++ | `include/yams/plugins/object_storage_iface.hpp` | Typed options/hooks |

## Core Operations

- **CRUD**: `put`, `get`, `head`, `delete`, `list`, `compose` (optional)
- **Checksums**: `crc32c`, `sha256`
- **Server-side encryption**: SSE-KMS, SSE-C (optional)
- **Idempotency**: `idempotencyKey` for safe retries/resume
- **Hooks**: `verifyObject()`, `health()`; optional DR: `drObjectPresent()`, `replicationLagSeconds()`

## Capability Matrix

| Feature | S3 | R2 |
|---------|----|----|
| Multipart | Yes | Yes |
| Resume | Yes | Yes |
| crc32c | Yes (`x-amz-checksum-crc32c`) | No (use sha256) |
| sha256 | Yes (`x-amz-checksum-sha256`) | Yes (ETag semantics differ) |
| SSE-KMS | Yes (`kmsKeyId`) | No (provider encryption at rest only) |
| SSE-C | Optional (supported) | No |
| Storage class | Yes (e.g., `STANDARD_IA`) | N/A |
| Compose | Limited (multipart complete) | Yes (multipart complete) |
| Versioning | Yes (bucket setting) | Yes (bucket setting) |
| Presign upload | Yes | Yes |
| Presign download | Yes | Yes |
| DR presence | Yes (HEAD in DR bucket) | Yes (presence only) |
| DR replication lag | Yes (RTC metrics when enabled) | Unknown (fallback to presence) |

## Provider Validation Status

- **Cloudflare R2**: Tested in this repository.
- **AWS S3**: Supported via the same S3-compatible API path, but not currently part of automated validation in this repository.

## Behavior Notes

- End-to-end verification prefers crc32c; falls back to sha256 where crc32c is not available.
- **DR readiness**:
    - S3 path: "ready" when all objects present in DR bucket; `replicationLagSeconds` available with RTC.
    - R2 path: "ready" determined by presence only; no replication lag metric.
- Listing-free reconciliation is achieved via manifest pointer scans; `list()` is bounded and optional.

## Error Model

- Typed errors for retry vs terminal, HTTP code, provider code, errno, optional `retryAfterMs`.
- Workers/agents implement exponential backoff with jitter, max attempt caps, and idempotency keys.

## Security

- Provider credentials are scoped per project via environment/bindings. No secrets persisted in catalogs or logs.
- SSE-KMS is enforced via options on S3; R2 uses provider encryption at rest.

## Tests

- **Unit**: Checksums, header formation, error typing, idempotency.
- **Integration**: MinIO (S3-compatible) for multipart, resume, versioning; AWS S3 for SSE-KMS and RTC.
- **Negative**: Induced 5xx/429 with retries, checksum mismatch aborts.
