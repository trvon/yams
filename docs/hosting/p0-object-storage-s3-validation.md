# P0 Validation: object_storage_s3

This checklist tracks the first P0 priority: validate `plugins/object_storage_s3` correctness and testability.

## Scope

- Plugin build and load path in local Meson workflow
- Core storage behavior (PUT/GET/HEAD/LIST/DELETE)
- Signer and storage submodule regression coverage
- R2-compatible smoke-test execution path

## Validation Performed

### 1) Build targets

```bash
meson setup builddir --reconfigure
meson compile -C builddir yams_object_storage_s3 yams_s3_plugin_smoke
```

Result: plugin and smoke-test executable build successfully.

### 2) Storage/signer regression suite

```bash
meson test -C builddir storage_submodule --print-errorlogs
```

Result: pass (`storage_submodule`).

### 3) Network smoke-test wiring

```bash
meson test -C builddir s3_plugin_smoke --print-errorlogs --verbose
```

Result: pass. When env is unset, smoke test exits with skip message.

## Changes Made

- Added Meson test registration for `s3_plugin_smoke` in `tests/plugins/meson.build`.
- Expanded smoke test to validate `PUT -> GET -> HEAD -> LIST -> DELETE` round-trip in
  `tests/plugins/s3/s3_plugin_smoke_test.cpp`.
- Hardened multipart query handling by percent-encoding `uploadId` in:
  - `uploadPart()`
  - `completeMultipartUpload()`
  - `abortMultipartUpload()`
  in `plugins/object_storage_s3/s3_plugin.cpp`.
- Updated plugin docs with Meson-first test commands and R2 example in
  `plugins/object_storage_s3/README.md`.

## Remaining P0 Validation Step (requires credentials)

Status (2026-03-06): root cause identified and fixed for signer mismatch in canonical request
formatting.

### Root cause fixed

- `S3Signer` canonical request was missing the required blank separator line between canonical
  headers and `SignedHeaders` for SigV4.
- This produced signatures accepted as syntactically valid but rejected by R2
  (`SignatureDoesNotMatch`).

### Fix applied

- Updated canonical request construction in `src/storage/s3_signer.cpp` to include the required
  separator newline.
- Added deterministic signer regression test (`tests/unit/storage/s3_signer_catch2_test.cpp`) using
  fixed signing timestamp (`YAMS_S3_SIGNER_FIXED_AMZ_DATE`) so this formatting issue cannot regress.
- Added signer input hardening by trimming credential/region whitespace before signing.

Additional mitigation applied:

- `S3Signer` now trims credential and region whitespace before signing to avoid copy/paste
  formatting issues causing false signature mismatches.

Observed variants (all failed with HTTP 403):

- endpoint host style + virtual host addressing
- endpoint host style + path style addressing
- endpoint with `https://` prefix + path style addressing

### Live endpoint validation

- `meson test -C builddir s3_plugin_smoke --print-errorlogs --verbose --no-rebuild`
  with valid temporary R2 S3 credentials now passes (`S3 plugin smoke: success`).

### Token note

- Generic Cloudflare bearer tokens are not used directly as S3 key/secret in this plugin.
- For token-only workflows, call
  `POST /accounts/{account_id}/r2/temp-access-credentials` and use returned
  `accessKeyId`, `secretAccessKey`, and `sessionToken` for the smoke run.

Run smoke test against a real endpoint (R2 or S3-compatible target):

### Cloudflare setup (Wrangler)

For `plugins/object_storage_s3`, a custom domain is optional. The plugin only needs:

- `S3_TEST_BUCKET`
- `S3_TEST_ENDPOINT` (`<accountid>.r2.cloudflarestorage.com`)
- R2 S3 API credentials (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`)

Provision a test bucket with Wrangler:

```bash
wrangler whoami
wrangler r2 bucket create yams-s3-smoke
```

If you want a bucket custom domain under `yamsmemory.ai`, add it separately:

```bash
wrangler r2 bucket domain add yams-s3-smoke \
  --domain <subdomain>.yamsmemory.ai \
  --zone-id <zone-id>
```

Then create an R2 API token/access key pair in Cloudflare dashboard (R2 S3 API) scoped to the
test bucket and export the values below.

Important: a generic Cloudflare API bearer token (`Authorization: Bearer ...`) is not directly
usable as S3 credentials for this plugin.

```bash
export AWS_ACCESS_KEY_ID="<key>"
export AWS_SECRET_ACCESS_KEY="<secret>"
export AWS_REGION="auto"
export S3_TEST_BUCKET="<bucket>"
export S3_TEST_ENDPOINT="<accountid>.r2.cloudflarestorage.com"
export S3_TEST_USE_PATH_STYLE=0

meson test -C builddir s3_plugin_smoke --print-errorlogs --verbose
```

Expected success message:

```text
S3 plugin smoke: success
```
