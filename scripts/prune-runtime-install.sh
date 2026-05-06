#!/usr/bin/env bash

set -euo pipefail

usage() {
  echo "Usage: $0 <install-root>" >&2
}

if [ "$#" -ne 1 ]; then
  usage
  exit 2
fi

install_root="${1%/}"

case "$install_root" in
  ""|"/"|".")
    echo "Refusing to prune unsafe install root: '$install_root'" >&2
    exit 1
    ;;
esac

if [ ! -d "$install_root" ]; then
  echo "Install root not found: $install_root" >&2
  exit 1
fi

strip_file() {
  local path="$1"

  case "$(uname -s)" in
    Darwin)
      strip -x "$path" 2>/dev/null || true
      ;;
    *)
      strip --strip-unneeded "$path" 2>/dev/null || true
      ;;
  esac
}

rm -rf "$install_root/include"

for libdir in "$install_root/lib" "$install_root/lib64"; do
  [ -d "$libdir" ] || continue
  rm -rf "$libdir/pkgconfig" "$libdir/cmake"
done

rm -rf "$install_root/share/pkgconfig" "$install_root/share/cmake"

find "$install_root" -type f \( -name '*.a' -o -name '*.la' \) -delete

if [ -d "$install_root/bin" ]; then
  while IFS= read -r -d '' path; do
    strip_file "$path"
  done < <(find "$install_root/bin" -type f -print0)
fi

while IFS= read -r -d '' path; do
  strip_file "$path"
done < <(
  find "$install_root" -type f \( -name '*.so' -o -name '*.so.*' -o -name '*.dylib' \) -print0
)

echo "Pruned runtime install tree: $install_root" >&2
du -sh "$install_root" 2>/dev/null || true
