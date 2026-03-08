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

Provider validation status
- Cloudflare R2: tested in this repository (smoke + storage benchmark path).
- AWS S3: supported by the same S3-compatible plugin/configuration path, but not currently
  validated by automated tests in this repository.

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
- Unit coverage: `meson test -C builddir storage_submodule --print-errorlogs` (includes signer and storage submodule tests).
- Optional network smoke test: `meson test -C builddir s3_plugin_smoke --print-errorlogs`.
- The smoke test requires the environment variables above and performs a PUT/GET/HEAD/LIST/DELETE round-trip.
- Helper script: `scripts/dev/run_s3_plugin_smoke.sh [builddir]`.

Example (R2)
```bash
export AWS_ACCESS_KEY_ID="<r2-access-key>"
export AWS_SECRET_ACCESS_KEY="<r2-secret-key>"
export AWS_REGION="auto"
export S3_TEST_BUCKET="<bucket>"
export S3_TEST_ENDPOINT="<accountid>.r2.cloudflarestorage.com"
export S3_TEST_USE_PATH_STYLE=0
meson test -C builddir s3_plugin_smoke --print-errorlogs
```

Example (AWS S3, supported path)
```bash
export AWS_ACCESS_KEY_ID="<aws-access-key-id>"
export AWS_SECRET_ACCESS_KEY="<aws-secret-access-key>"
export AWS_REGION="us-east-1"
export S3_TEST_BUCKET="<bucket>"
export S3_TEST_ENDPOINT="s3.us-east-1.amazonaws.com"
export S3_TEST_USE_PATH_STYLE=0
meson test -C builddir s3_plugin_smoke --print-errorlogs
```

Note: set `S3_TEST_ENDPOINT` to host only. Do not include `https://` or `/<bucket>`.
Note: Cloudflare API bearer tokens are not S3 credentials; use an R2 S3 Access Key ID + Secret
Access Key pair from `R2 -> Manage R2 API tokens`.

If you only have a Cloudflare API token, create temporary S3 credentials via
`POST /accounts/{account_id}/r2/temp-access-credentials` and export returned
`accessKeyId`, `secretAccessKey`, and `sessionToken` as AWS env vars.

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
