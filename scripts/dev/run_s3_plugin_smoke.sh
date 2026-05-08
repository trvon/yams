#!/usr/bin/env bash

set -euo pipefail

builddir="${1:-builddir}"
smoke_exe="${builddir}/tests/plugins/yams_s3_plugin_smoke"

required=(
  "YAMS_RUN_LIVE_S3_SMOKE"
  "S3_TEST_BUCKET"
  "AWS_ACCESS_KEY_ID"
  "AWS_SECRET_ACCESS_KEY"
)

missing=()
for var in "${required[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    missing+=("${var}")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Missing required environment variables: ${missing[*]}" >&2
  echo "Required: YAMS_RUN_LIVE_S3_SMOKE=1, S3_TEST_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY" >&2
  echo "Recommended for R2: AWS_REGION=auto, S3_TEST_ENDPOINT=<accountid>.r2.cloudflarestorage.com" >&2
  exit 2
fi

if [[ "${YAMS_RUN_LIVE_S3_SMOKE}" != "1" ]]; then
  echo "YAMS_RUN_LIVE_S3_SMOKE must be set to 1 to run the live S3/R2 smoke test." >&2
  exit 2
fi

if [[ ${#AWS_ACCESS_KEY_ID} -ne 32 || "${AWS_ACCESS_KEY_ID}" == *-* ]]; then
  echo "AWS_ACCESS_KEY_ID does not look like an R2 S3 access key (expected 32 chars)." >&2
  echo "Tip: use R2 S3 API credentials, not a Cloudflare API bearer token." >&2
  exit 2
fi

if [[ ${#AWS_SECRET_ACCESS_KEY} -lt 40 ]]; then
  echo "AWS_SECRET_ACCESS_KEY looks too short for S3-compatible credentials." >&2
  echo "Tip: use the paired R2 S3 Secret Access Key from 'Manage R2 API tokens'." >&2
  exit 2
fi

export AWS_REGION="${AWS_REGION:-auto}"
export S3_TEST_USE_PATH_STYLE="${S3_TEST_USE_PATH_STYLE:-0}"

if [[ -x "${smoke_exe}" && -f "${builddir}/build.ninja" ]]; then
  meson test -C "${builddir}" s3_plugin_smoke --print-errorlogs --verbose --no-rebuild
else
  if [[ ! -d "${builddir}" ]]; then
    meson setup "${builddir}"
  fi
  meson setup "${builddir}" --reconfigure
  meson compile -C "${builddir}" yams_object_storage_s3 yams_s3_plugin_smoke
  meson test -C "${builddir}" s3_plugin_smoke --print-errorlogs --verbose
fi
