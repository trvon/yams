#!/usr/bin/env bash
# Keep the ONNX Runtime Conan package in its own Actions cache entry.
#
#   onnxruntime-conan-cache.sh restore <archive>
#   onnxruntime-conan-cache.sh save <archive> <key-prefix>
#
# The shared Conan cache entry (conan2-...) is written by the warm job, which
# runs without ONNX, and the build job gets an exact-key hit, so actions/cache
# never saves what the build added. ONNX Runtime was therefore fetched on every
# release build. This script moves it through `conan cache save/restore`:
#
#   restore  unpack <archive> (if the cache step restored one) into the Conan
#            cache and record which onnxruntime packages are present.
#   save     archive only the onnxruntime packages this run created (none when
#            the restored ones were used) and print save=true and
#            key=<key-prefix><digest of the saved package ids> to
#            $GITHUB_OUTPUT. The digest keys each entry by the exact packages
#            it holds; a compiler or option change gives a new entry instead of
#            growing an old one.
set -euo pipefail

mode="${1:?usage: $0 restore|save <archive> [key-prefix]}"
archive="${2:?archive path required}"
state="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/onnxruntime-conan-before.json"
out="${GITHUB_OUTPUT:-/dev/stdout}"

PY="$(command -v python || command -v python3)"

list_packages() {
  conan list "onnxruntime/*#*:*#*" --format=json 2>/dev/null
}

case "$mode" in
  restore)
    if [ -f "$archive" ]; then
      if conan cache restore "$archive"; then
        echo "Restored ONNX Runtime Conan package from $archive"
      else
        echo "::warning::conan cache restore failed for $archive; ONNX Runtime will be fetched"
      fi
    else
      echo "No cached ONNX Runtime Conan package"
    fi
    list_packages >"$state" || echo '{"Local Cache": {}}' >"$state"
    ;;
  save)
    prefix="${3:?key prefix required}"
    after="$(mktemp)"
    selected="$(mktemp)"
    list_packages >"$after" || echo '{"Local Cache": {}}' >"$after"
    [ -f "$state" ] || echo '{"Local Cache": {}}' >"$state"
    # Keep the packages present now but not after restore. Prints a digest of
    # their ids, or nothing when there is nothing new.
    digest="$("$PY" -I - "$state" "$after" "$selected" <<'EOF'
import hashlib
import json
import sys

before_path, after_path, out_path = sys.argv[1:4]


def load(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle).get("Local Cache", {})


def ids(cache):
    for ref, info in cache.items():
        for rrev, rinfo in info.get("revisions", {}).items():
            for pkg_id in rinfo.get("packages", {}):
                yield f"{ref}#{rrev}:{pkg_id}"


before = set(ids(load(before_path)))
after = load(after_path)
selected = {}
new_ids = []
for ref, info in after.items():
    for rrev, rinfo in info.get("revisions", {}).items():
        for pkg_id, pinfo in rinfo.get("packages", {}).items():
            key = f"{ref}#{rrev}:{pkg_id}"
            if key in before or not pinfo.get("revisions"):
                continue
            new_ids.append(key)
            rsel = selected.setdefault(ref, {"revisions": {}})["revisions"].setdefault(
                rrev, {k: v for k, v in rinfo.items() if k != "packages"} | {"packages": {}}
            )
            rsel["packages"][pkg_id] = pinfo
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump({"Local Cache": selected}, handle)
if new_ids:
    print(hashlib.sha256("\n".join(sorted(new_ids)).encode()).hexdigest()[:16])
EOF
)"
    if [ -z "$digest" ]; then
      echo "No new ONNX Runtime Conan package to cache"
      echo "save=false" >>"$out"
      exit 0
    fi
    rm -f "$archive"
    conan cache save --list "$selected" --no-source --file "$archive"
    ls -l "$archive"
    {
      echo "save=true"
      echo "key=${prefix}${digest}"
    } >>"$out"
    ;;
  *)
    echo "unknown mode: $mode" >&2
    exit 1
    ;;
esac
