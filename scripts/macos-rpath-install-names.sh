#!/usr/bin/env bash
# Meson install script (macOS): keep YAMS's private dylibs @rpath-relative.
#
#   macos-rpath-install-names.sh <libdir>
#
# Meson rewrites the install name of every installed shared library to its
# absolute install path (e.g. /opt/homebrew/lib/yams/libyams_onnx_resource.dylib)
# and points consumers at that path, so a staged or unzipped tree only runs from
# its final prefix. The executables already carry @executable_path/../lib/yams
# and the plugins @loader_path/.. RPATHs; restore the @rpath install name so
# those RPATHs are what resolves the library.
set -euo pipefail

libdir="${1:?usage: $0 <libdir>}"
root="${MESON_INSTALL_DESTDIR_PREFIX:?run from meson install}"
lib="$root/$libdir/yams/libyams_onnx_resource.dylib"
[ -f "$lib" ] || exit 0

new_id="@rpath/libyams_onnx_resource.dylib"
old_id="$(otool -D "$lib" | tail -n 1)"
if [ "$old_id" != "$new_id" ]; then
  install_name_tool -id "$new_id" "$lib"
fi

for f in "$root"/bin/* "$root/$libdir"/yams/plugins/*.dylib "$root/$libdir"/yams/plugins/*.so; do
  [ -f "$f" ] && [ ! -L "$f" ] || continue
  if otool -L "$f" 2>/dev/null | grep -qF "$old_id "; then
    install_name_tool -change "$old_id" "$new_id" "$f"
  fi
done
