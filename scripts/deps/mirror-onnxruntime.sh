#!/usr/bin/env bash
# Stage the YAMS mirror of an ONNX Runtime release.
#
#   mirror-onnxruntime.sh <version> [outdir]
#
# Downloads Microsoft.ML.OnnxRuntime.<version>.nupkg from nuget.org into
# outdir (default: a new temporary directory), checks it against
#   - NuGet's catalog SHA-512 for the package, and
#   - the SHA-256 pinned in conan/onnxruntime/conanfile.py,
# then prints the gh commands that publish it as the prerelease
# deps-onnxruntime-<version> on trvon/yams. It publishes nothing itself.
#
# The recipe tries this mirror after the upstream GitHub archive and before
# nuget.org (see _MIRROR_BASE_URL in the recipe). Upstream GitHub archives for
# 1.23.0 return 404, so the mirror carries the NuGet package only.
#
# For a version the recipe does not pin yet, the script prints the SHA-256 to
# add to _NUGET_CPU_SHA256 and _MIRROR_SHA256 and exits 2 without printing
# publish commands.
set -euo pipefail

version="${1:?usage: $0 <version> [outdir]}"
outdir="${2:-}"
repo="${YAMS_MIRROR_REPO:-trvon/yams}"
tag="deps-onnxruntime-${version}"

case "$version" in
  *[!0-9.]* | "" | .* | *.) echo "bad version: $version" >&2; exit 1 ;;
esac

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
recipe="$here/../../conan/onnxruntime/conanfile.py"

if [ -z "$outdir" ]; then
  outdir="$(mktemp -d "${TMPDIR:-/tmp}/onnxruntime-mirror-${version}.XXXXXX")"
fi
mkdir -p "$outdir"
outdir="$(cd "$outdir" && pwd)"

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

sha512_b64_of() {
  openssl dgst -sha512 -binary "$1" | openssl base64 -A
}

id="microsoft.ml.onnxruntime"
nupkg="${id}.${version}.nupkg"
nupkg_url="https://api.nuget.org/v3-flatcontainer/${id}/${version}/${nupkg}"
reg_url="https://api.nuget.org/v3/registration5-gz-semver2/${id}/${version}.json"

echo "Downloading $nupkg_url"
curl -fsSL --retry 3 -o "$outdir/$nupkg" "$nupkg_url"

# NuGet's catalog entry records the SHA-512 nuget.org computed at upload time.
# The JSON is parsed with python3 -I from stdin; nothing in it is executed.
json_field() {
  python3 -I -c 'import json, sys; print(json.load(sys.stdin).get(sys.argv[1], ""))' "$1"
}
catalog_url="$(curl -fsSL --compressed "$reg_url" | json_field catalogEntry)"
case "$catalog_url" in
  https://api.nuget.org/v3/catalog0/*) ;;
  *) echo "unexpected catalog URL: ${catalog_url:-none}" >&2; exit 1 ;;
esac
catalog_json="$(curl -fsSL --compressed "$catalog_url")"
want_sha512="$(printf '%s' "$catalog_json" | json_field packageHash)"
algo="$(printf '%s' "$catalog_json" | json_field packageHashAlgorithm)"
have_sha512="$(sha512_b64_of "$outdir/$nupkg")"
if [ "$algo" != "SHA512" ] || [ "$have_sha512" != "$want_sha512" ]; then
  echo "SHA-512 mismatch against the NuGet catalog for $nupkg" >&2
  echo "  catalog: $algo $want_sha512" >&2
  echo "  file:    $have_sha512" >&2
  exit 1
fi
echo "NuGet catalog SHA-512 matches"

have_sha256="$(sha256_of "$outdir/$nupkg")"
pinned="$(sed -n -E "s/^ *\"${version//./\\.}\": \"([0-9a-f]{64})\",?$/\1/p" "$recipe" | head -n 1)"
if [ -z "$pinned" ]; then
  echo
  echo "$version is not pinned in $recipe. After review, add:"
  echo "  _NUGET_CPU_SHA256:  \"$version\": \"$have_sha256\","
  echo "  _MIRROR_SHA256:     \"$version\": {\"$nupkg\": _NUGET_CPU_SHA256[\"$version\"]},"
  exit 2
fi
if [ "$pinned" != "$have_sha256" ]; then
  echo "SHA-256 mismatch for $nupkg" >&2
  echo "  recipe: $pinned" >&2
  echo "  file:   $have_sha256" >&2
  exit 1
fi
echo "Recipe SHA-256 matches: $have_sha256"

printf '%s  %s\n' "$have_sha256" "$nupkg" >"$outdir/SHA256SUMS"

cat <<EOF

Staged in $outdir:
  $nupkg
  SHA256SUMS

To publish the mirror (not run by this script):

  gh release create "$tag" --repo "$repo" --prerelease \\
    --title "ONNX Runtime $version (dependency mirror)" \\
    --notes "Unmodified copy of $nupkg from nuget.org, used by conan/onnxruntime when upstream downloads fail. SHA-256 $have_sha256."
  gh release upload "$tag" --repo "$repo" \\
    "$outdir/$nupkg" "$outdir/SHA256SUMS"

Check it afterwards:

  gh release download "$tag" --repo "$repo" -p "$nupkg" -D "\$(mktemp -d)"
EOF
