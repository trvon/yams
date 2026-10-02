#!/usr/bin/env bash
# Split debug info out of a staged runtime install and strip the shipped binaries.
#
#   scripts/split-debug-symbols.sh <install-root> [<debug-dir>]
#
# <install-root> is the staged prefix (e.g. "$DESTDIR/usr"). Every ELF / Mach-O
# file under it is processed in place:
#   ELF:    objcopy --only-keep-debug -> <debug-dir>/<path>.debug (+ .build-id/xx/yyyy.debug),
#           strip --strip-unneeded, objcopy --add-gnu-debuglink
#   Mach-O: dsymutil -> <debug-dir>/<path>.dSYM, strip -S -x
# Without <debug-dir> the binaries are only stripped.
#
# The install rules decide WHAT ships (runtime/devel install tags); this script
# only decides how it ships: stripped, with symbols kept as a separate artifact.
set -euo pipefail

usage() {
  echo "Usage: $0 <install-root> [<debug-dir>]" >&2
}

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  usage
  exit 2
fi

install_root="${1%/}"
debug_dir="${2:-}"

case "$install_root" in
  "" | "/" | ".")
    echo "Refusing to process unsafe install root: '$install_root'" >&2
    exit 1
    ;;
esac
if [ ! -d "$install_root" ]; then
  echo "Install root not found: $install_root" >&2
  exit 1
fi
install_root="$(cd "$install_root" && pwd)"
if [ -n "$debug_dir" ]; then
  mkdir -p "$debug_dir"
  debug_dir="$(cd "$debug_dir" && pwd)"
fi

first_tool() {
  local t
  for t in "$@"; do
    if command -v "$t" >/dev/null 2>&1; then
      command -v "$t"
      return 0
    fi
  done
  return 1
}

magic_of() {
  # Prints elf / macho / none for the file's magic bytes.
  local hex
  hex="$(od -An -tx1 -N4 "$1" 2>/dev/null | tr -d ' \n')"
  case "$hex" in
    7f454c46) echo elf ;;
    feedface | cefaedfe | feedfacf | cffaedfe | cafebabe) echo macho ;;
    *) echo none ;;
  esac
}

processed=0
split=0

process_elf() {
  local path="$1" rel="$2"
  local sections
  sections="$("$READELF" -W -S "$path" 2>/dev/null || true)"
  if ! grep -Eq '\] \.(z?debug_|symtab)' <<<"$sections"; then
    return 0
  fi
  if [ -n "$debug_dir" ] && grep -Eq '\] \.z?debug_' <<<"$sections"; then
    local out="$debug_dir/$rel.debug"
    mkdir -p "$(dirname "$out")"
    "$OBJCOPY" --only-keep-debug --compress-debug-sections "$path" "$out"
    chmod 0644 "$out"
    local build_id
    build_id="$("$READELF" -n "$path" 2>/dev/null | sed -n 's/^.*Build ID: \([0-9a-f]*\).*$/\1/p' | head -1)"
    if [ -n "$build_id" ] && [ "${#build_id}" -gt 2 ]; then
      mkdir -p "$debug_dir/.build-id/${build_id:0:2}"
      ln -sf "../../$rel.debug" "$debug_dir/.build-id/${build_id:0:2}/${build_id:2}.debug"
    fi
    "$STRIP" --strip-unneeded "$path"
    (cd "$(dirname "$out")" && "$OBJCOPY" --add-gnu-debuglink="$(basename "$out")" "$path")
    split=$((split + 1))
  else
    "$STRIP" --strip-unneeded "$path"
  fi
}

process_macho() {
  local path="$1" rel="$2"
  if [ -n "$debug_dir" ] && command -v dsymutil >/dev/null 2>&1; then
    local out="$debug_dir/$rel.dSYM"
    mkdir -p "$(dirname "$out")"
    if dsymutil "$path" -o "$out" >/dev/null 2>&1; then
      split=$((split + 1))
    else
      rm -rf "$out"
    fi
  fi
  strip -S -x "$path" 2>/dev/null || true
}

if [ "$(uname -s)" != "Darwin" ]; then
  READELF="$(first_tool readelf llvm-readelf)" || {
    echo "readelf not found" >&2
    exit 1
  }
  OBJCOPY="$(first_tool objcopy llvm-objcopy)" || {
    echo "objcopy not found" >&2
    exit 1
  }
  STRIP="$(first_tool strip llvm-strip)" || {
    echo "strip not found" >&2
    exit 1
  }
fi

while IFS= read -r -d '' path; do
  rel="${path#"$install_root"/}"
  case "$rel" in
    # The bundled ONNX Runtime is an upstream release build: leave it byte-identical
    # (it ships stripped, and re-stripping would void a macOS code signature).
    */yams/onnxruntime/*) continue ;;
  esac
  case "$(magic_of "$path")" in
    elf)
      process_elf "$path" "$rel"
      processed=$((processed + 1))
      ;;
    macho)
      process_macho "$path" "$rel"
      processed=$((processed + 1))
      ;;
  esac
done < <(find "$install_root" -type f -print0)

echo "split-debug-symbols: ${processed} binaries stripped, ${split} debug files written${debug_dir:+ to $debug_dir}" >&2
du -sh "$install_root" 2>/dev/null || true
