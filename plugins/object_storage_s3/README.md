S3 Object Storage Plugin (Experimental)

Overview
- Provides an S3/R2-compatible object storage backend via the YAMS plugin system.
- Implements basic object operations (PUT/GET/HEAD/DELETE) with AWS Signature V4.
- Adds list() using S3 ListObjectsV2.
- Includes a first-cut multipart upload (initiate/upload-part/complete/abort).

Status
- Experimental: APIs and behavior may change. Not all S3 features are covered.
- Multipart: first-cut implementation with optional checksum and SSE‑KMS headers.
- list(): implements ListObjectsV2 with prefix and delimiter parsing; bounded pagination.

Configuration
- BackendConfig.type: "s3"
- BackendConfig.url: "s3://<bucket>/<prefix>"
- BackendConfig.region: AWS region (e.g., "us-east-1"), or "auto" for R2.
- BackendConfig.usePathStyle: true to force path-style addressing.
- Credentials (config.credentials map or environment):
  - access_key / secret_key / session_token
  - endpoint: custom endpoint host (e.g., "<accountid>.r2.cloudflarestorage.com")

Environment Variables
- AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
- AWS_REGION (used by the smoke test; region is otherwise taken from BackendConfig)
- S3_TEST_BUCKET, S3_TEST_ENDPOINT, S3_TEST_USE_PATH_STYLE=1 (smoke test)

Build Notes
- Requires libcurl and OpenSSL development libraries present at build time.
- Target name: yams_object_storage_s3

Testing
- Optional smoke test: enable CMake option `YAMS_TEST_S3_PLUGIN_INTEGRATION` and run `ctest`.
- The smoke test requires the environment variables above to be set and performs a PUT/HEAD/DELETE round‑trip.

Security
- Credentials are taken from config and/or standard AWS environment variables.
- Signing uses AWS SigV4; verify environment and endpoint correctness for non-AWS providers.

Enhancements in this slice
- Stronger URI encoding for keys and canonicalization.
- Prefix-aware listing with delimiter support using XML parsing of ListObjectsV2.
- Optional checksum/SSE headers for PUT and multipart flows (signed):
  - `checksumAlgorithm = "sha256"` adds `x-amz-checksum-sha256`.
  - `sseKmsKeyId` adds `x-amz-server-side-encryption: aws:kms` and key id.
  - `storageClass` sets `x-amz-storage-class`.

Prefix-aware listing and prefixes
- Core API (`IStorageBackend::list`): returns a flat list of keys for a given prefix.
- Plugin API (`object_storage_v1` via adapter): accepts `delimiter`, `pageToken`, `maxKeys` in `opts_json` and returns JSON:
  - `{ "items": ["key1", ...], "nextPageToken": "...", "prefixes": ["dir1/", ...] }`.
  - Not all backends populate `prefixes`; clients can infer prefixes from keys when unset.

Signer header coverage
- The SigV4 signer includes optional headers in the signed set when provided (SSE‑KMS, storage class, checksum).

Limitations / Future Work
- Canonical query/signing consolidation across signer and callers.
- SSE‑C, CRC32C, compose, versioning, presign URLs, and robust pagination APIs.
- DR presence and replication hooks aligned with `object_storage_v1` interface.
