---
title: Object Storage Plugin v1 — API and Capability Matrix
status: draft
---

Overview
- Provider‑agnostic plugin API for object storage used by DR and hosting. Backends expose capabilities; core negotiates behavior.

Key Interfaces
- C ABI: include/yams/plugins/object_storage_v1.h (stable loader surface)
- C++ interface: include/yams/plugins/object_storage_iface.hpp (typed options/hooks)

Core Operations
- put/get/head/delete/list/compose (optional)
- Checksums: crc32c, sha256
- SSE: SSE‑KMS, SSE‑C (optional)
- Idempotency: idempotencyKey for safe retries/resume
- Hooks: verifyObject(), health(); optional DR: drObjectPresent(), replicationLagSeconds()

Capability Matrix (initial)
- Feature: multipart — S3: yes, R2: yes
- Feature: resume — S3: yes, R2: yes
- Feature: crc32c — S3: yes (x‑amz‑checksum‑crc32c), R2: no (use sha256)
- Feature: sha256 — S3: yes (x‑amz‑checksum‑sha256), R2: yes (content SHA‑256 / ETag semantics differ)
- Feature: sse‑kms — S3: yes (kmsKeyId), R2: no (provider encryption at rest only)
- Feature: sse‑c — S3: optional (supported), R2: no
- Feature: storage‑class — S3: yes (e.g., STANDARD_IA), R2: n/a
- Feature: compose — S3: limited (multipart complete), R2: yes (multipart complete)
- Feature: versioning — S3: yes (bucket setting), R2: yes (bucket setting)
- Feature: presign‑upload — S3: yes, R2: yes
- Feature: presign‑download — S3: yes, R2: yes
- Feature: dr‑presence — S3: yes (HEAD in DR bucket), R2: yes (presence only)
- Feature: dr‑replication‑lag — S3: yes (RTC metrics when enabled), R2: unknown (fallback to presence)

Behavior Notes
- End‑to‑end verification prefers crc32c; fall back to sha256 where crc32c is not available.
- DR readiness:
  - S3 path: “ready” when all objects present in DR bucket; replicationLagSeconds available with RTC.
  - R2 path: “ready” determined by presence only; no replication lag metric.
- Listing‑free reconciliation is achieved via manifest pointer scans; list() is bounded and optional.

Error Model
- Typed errors for retry vs terminal, HTTP code, provider code, errno, optional retryAfterMs.
- Workers/agent implement exponential backoff with jitter, max attempt caps, and idempotency keys.

Security
- Provider credentials are scoped per project via environment/bindings. No secrets persisted in catalogs or logs.
- SSE‑KMS is enforced via options on S3; R2 uses provider encryption at rest.

Tests
- Unit: checksums, header formation, error typing, idempotency.
- Integration: MinIO (S3‑compatible) for multipart, resume, versioning; AWS S3 for SSE‑KMS and RTC.
- Negative: induced 5xx/429 with retries, checksum mismatch aborts.

