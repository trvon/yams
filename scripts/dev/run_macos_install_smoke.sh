#!/usr/bin/env bash

set -euo pipefail

if [[ "${OSTYPE:-}" != darwin* ]]; then
  echo "This smoke test is intended for macOS." >&2
  exit 1
fi

BUILD_DIR="${1:-build/release}"
KEEP_STAGE="${KEEP_STAGE:-0}"

if [[ ! -d "$BUILD_DIR" ]]; then
  echo "Build directory not found: $BUILD_DIR" >&2
  exit 1
fi

prefix="$(
  python3 - "$BUILD_DIR" <<'PY'
import json
import pathlib
import sys

build_dir = pathlib.Path(sys.argv[1])
info_path = build_dir / 'meson-info' / 'intro-buildoptions.json'
if not info_path.exists():
    raise SystemExit(f'missing build options: {info_path}')

options = json.loads(info_path.read_text())
for entry in options:
    if entry.get('name') == 'prefix':
        value = entry.get('value')
        if not value:
            raise SystemExit('prefix option was empty')
        print(value)
        break
else:
    raise SystemExit('prefix option not found')
PY
)"

stage_dir="$(mktemp -d "${TMPDIR:-/tmp}/yams-install-smoke.XXXXXX")"
cleanup() {
  if [[ "$KEEP_STAGE" != "1" ]]; then
    rm -rf "$stage_dir"
  else
    echo "Kept staged install at: $stage_dir"
  fi
}
trap cleanup EXIT

echo "Staging install from $BUILD_DIR"
echo "Install prefix: $prefix"
echo "Stage dir: $stage_dir"

meson install -C "$BUILD_DIR" --destdir "$stage_dir"

install_root="$stage_dir$prefix"
bin_dir="$install_root/bin"
lib_dir="$install_root/lib"
plugin_dir="$lib_dir/yams/plugins"
cli_bin="$bin_dir/yams"
plugin_path="$plugin_dir/libyams_onnx_plugin.dylib"

if [[ ! -x "$cli_bin" ]]; then
  echo "Missing CLI binary: $cli_bin" >&2
  exit 1
fi

if [[ ! -f "$plugin_path" ]]; then
  echo "Missing ONNX plugin: $plugin_path" >&2
  exit 1
fi

if [[ ! -f "$lib_dir/libyams_onnx_resource.dylib" && ! -f "$lib_dir/libyams_onnx_resource.0.dylib" ]]; then
  echo "Missing ONNX resource library in $lib_dir" >&2
  exit 1
fi

if ! compgen -G "$lib_dir/libonnxruntime*.dylib" >/dev/null; then
  echo "Missing ONNX Runtime dylib in $lib_dir" >&2
  exit 1
fi

echo "Installed ONNX artifacts:"
echo "  CLI: $cli_bin"
echo "  Plugin: $plugin_path"
echo "  Resource: $(ls "$lib_dir"/libyams_onnx_resource*.dylib | tr '\n' ' ')"
echo "  Runtime: $(ls "$lib_dir"/libonnxruntime*.dylib | tr '\n' ' ')"

echo "Checking installed plugin linkage"
otool -L "$plugin_path"
if otool -L "$plugin_path" | grep -q 'libonnxruntime'; then
  echo "Installed ONNX plugin still links libonnxruntime directly" >&2
  exit 1
fi

export PATH="$bin_dir:$PATH"
export DYLD_LIBRARY_PATH="$lib_dir${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
export YAMS_PLUGIN_DIR="$plugin_dir"
export YAMS_SKIP_MODEL_LOADING=1

echo "Running staged CLI smoke"
"$cli_bin" --version

doctor_output="$({ "$cli_bin" doctor plugin onnx --no-daemon; } 2>&1)"
printf '%s\n' "$doctor_output"

if [[ "$doctor_output" != *"Interface: model_provider_v1 v1 -> AVAILABLE"* ]]; then
  echo "Installed plugin doctor smoke did not report the model provider interface" >&2
  exit 1
fi

echo "macOS install smoke passed"
