#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Unified YAMS lint dispatcher.
# See: scripts/lint.sh --help

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Color helpers
if [[ -t 2 ]]; then
  C_RED='\033[0;31m' C_GREEN='\033[0;32m' C_YELLOW='\033[1;33m'
  C_BLUE='\033[0;34m' C_CYAN='\033[0;36m' C_DIM='\033[2m' C_OFF='\033[0m'
else
  C_RED='' C_GREEN='' C_YELLOW='' C_BLUE='' C_CYAN='' C_DIM='' C_OFF=''
fi
say()    { printf '%b%s%b\n' "$C_BLUE"   "→ $*" "$C_OFF" >&2; }
ok()     { printf '%b%s%b\n' "$C_GREEN"  "✓ $*" "$C_OFF" >&2; }
warn()   { printf '%b%s%b\n' "$C_YELLOW" "⚠ $*" "$C_OFF" >&2; }
err()    { printf '%b%s%b\n' "$C_RED"    "✗ $*" "$C_OFF" >&2; }
section(){ printf '\n%b== %s ==%b\n' "$C_CYAN" "$*" "$C_OFF" >&2; }

usage() {
  cat <<'EOF'
Usage: scripts/lint.sh [check ...] [options]

Unified YAMS lint dispatcher. Subsumes formatter, static analyzers,
metadata checks, and dependency audit.

CHECKS (positional; default = format cppcheck opengrep when none given)
  format          clang-format            (delegates scripts/format-code.sh)
  cppcheck        cppcheck                (delegates scripts/check-quality.sh)
  tidy            clang-tidy
  opengrep        opengrep audit profile  (delegates scripts/dev/run_opengrep.sh)
  windows-hdr     Windows.h include order + WIN32_LEAN_AND_MEAN/NOMINMAX
  license         GPL-3.0-or-later header enforcement
  commitlint      commit-message lint     (npx commitlint, requires npm)
  dco             DCO Signed-off-by check
  actionlint      GitHub workflow YAML lint
  deps-conan      Conan dep version drift
  deps-cve        Conan dep CVE scan      (osv-scanner)
  deps-subs       git submodule freshness
  deps-licenses   Conan + submodule license allowlist
  deps            shorthand: deps-conan deps-cve deps-subs deps-licenses
  all             every check above

MODE
  --check         read-only (default for all but format)
  --fix           auto-fix where supported (format)

SCOPE
  --git           only files staged in git index
  --diff REF      only files changed vs REF (e.g. origin/main)
  --paths PATH... explicit path list (consumes the rest of the line)
  --all-files     full repo (default)

BEHAVIOR
  --baseline REF  for opengrep + cppcheck: report only findings new vs REF
  --strict        exit nonzero on any finding (incl. warn-only)
  --hook          shorthand for the pre-commit set
  --ci            shorthand for the CI quality set
  --help          this message

EXAMPLES
  scripts/lint.sh                          # default: format check + opengrep + cppcheck
  scripts/lint.sh --hook                   # what .githooks/pre-commit calls
  scripts/lint.sh --ci                     # what CI runs
  scripts/lint.sh deps                     # full deps audit
  scripts/lint.sh opengrep --baseline HEAD~1   # delta-only opengrep run
  scripts/lint.sh format --fix --git       # fix formatting on staged files
EOF
}

# ----- Argument parser ---------------------------------------------------

CHECKS=()
SCOPE="all-files"
DIFF_REF=""
PATHS=()
MODE_FIX=0
STRICT=0
BASELINE=""
HOOK=0
CI=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --check)   MODE_FIX=0; shift ;;
    --fix)     MODE_FIX=1; shift ;;
    --git)     SCOPE="git"; shift ;;
    --diff)    SCOPE="diff"; DIFF_REF="${2:?--diff requires REF}"; shift 2 ;;
    --paths)   SCOPE="paths"; shift; while [[ $# -gt 0 && "$1" != --* ]]; do PATHS+=("$1"); shift; done ;;
    --all-files) SCOPE="all-files"; shift ;;
    --baseline) BASELINE="${2:?--baseline requires REF}"; shift 2 ;;
    --strict)  STRICT=1; shift ;;
    --hook)    HOOK=1; shift ;;
    --ci)      CI=1; shift ;;
    --) shift; while [[ $# -gt 0 ]]; do PATHS+=("$1"); shift; done; SCOPE="paths" ;;
    --*) err "unknown flag: $1"; usage; exit 2 ;;
    *) CHECKS+=("$1"); shift ;;
  esac
done

# Default check sets
if (( HOOK )); then
  SCOPE="git"; STRICT=1; MODE_FIX=1
  CHECKS=(format windows-hdr license opengrep)
elif (( CI )); then
  SCOPE="all-files"; STRICT=1
  CHECKS=(opengrep cppcheck tidy actionlint dco)
elif [[ ${#CHECKS[@]} -eq 0 ]]; then
  CHECKS=(format cppcheck opengrep)
fi

# Expand "all" / "deps" shorthands
NEW=()
for c in "${CHECKS[@]}"; do
  case "$c" in
    all)  NEW+=(format cppcheck tidy opengrep windows-hdr license commitlint dco actionlint deps-conan deps-cve deps-subs deps-licenses) ;;
    deps) NEW+=(deps-conan deps-cve deps-subs deps-licenses) ;;
    *)    NEW+=("$c") ;;
  esac
done
CHECKS=("${NEW[@]}")

# ----- Path resolver -----------------------------------------------------

# Emits NUL-delimited file list to stdout, filtered by extension regex in $1.
# Caller iterates with: while IFS= read -r -d '' f; do ...; done < <(resolve_paths '...')
resolve_paths() {
  local ext_re="${1:-.*}"
  case "$SCOPE" in
    git)
      git diff --cached --name-only --diff-filter=ACM -z 2>/dev/null \
        | tr -d '\0' | tr '\n' '\0' \
        | while IFS= read -r -d '' f; do
            [[ -f "$f" && "$f" =~ $ext_re ]] && printf '%s\0' "$f"
          done
      ;;
    diff)
      git diff --name-only --diff-filter=ACMR "$DIFF_REF" \
        | while IFS= read -r f; do
            [[ -f "$f" && "$f" =~ $ext_re ]] && printf '%s\0' "$f"
          done
      ;;
    paths)
      for p in "${PATHS[@]}"; do
        if [[ -d "$p" ]]; then
          find "$p" -type f -print0 | while IFS= read -r -d '' f; do
            [[ "$f" =~ $ext_re ]] && printf '%s\0' "$f"
          done
        elif [[ -f "$p" && "$p" =~ $ext_re ]]; then
          printf '%s\0' "$p"
        fi
      done
      ;;
    all-files)
      git ls-files -z 2>/dev/null \
        | while IFS= read -r -d '' f; do
            [[ -f "$f" && "$f" =~ $ext_re ]] && printf '%s\0' "$f"
          done
      ;;
  esac
}

# ----- Helpers: delegating wrappers --------------------------------------

CPP_RE='\.(c|cc|cpp|cxx|h|hh|hpp)$'

lint_format() {
  section "format (clang-format)"
  local files=()
  while IFS= read -r -d '' f; do files+=("$f"); done < <(resolve_paths "$CPP_RE")
  if [[ ${#files[@]} -eq 0 ]]; then ok "format: no files in scope"; return 0; fi
  # Windows path: invoke format-code.ps1 if available; otherwise fall back to .sh.
  local fmt_sh="$REPO_ROOT/scripts/format-code.sh"
  local fmt_ps1="$REPO_ROOT/scripts/format-code.ps1"
  local on_windows=0
  case "${OSTYPE:-}" in msys*|cygwin*|win32*|mingw*) on_windows=1 ;; esac
  run_fmt_check() {
    if (( on_windows )) && command -v pwsh >/dev/null 2>&1; then
      printf '%s\n' "${files[@]}" | pwsh -NoProfile -ExecutionPolicy Bypass -File "$fmt_ps1" -Check -Serial -StdinFiles
    elif (( on_windows )) && command -v powershell >/dev/null 2>&1; then
      printf '%s\n' "${files[@]}" | powershell -NoProfile -ExecutionPolicy Bypass -File "$fmt_ps1" -Check -Serial -StdinFiles
    else
      printf '%s\n' "${files[@]}" | "$fmt_sh" --check --serial --stdin-files
    fi
  }
  run_fmt_fix() {
    if (( on_windows )) && command -v pwsh >/dev/null 2>&1; then
      printf '%s\n' "${files[@]}" | pwsh -NoProfile -ExecutionPolicy Bypass -File "$fmt_ps1" -Serial -StdinFiles
    elif (( on_windows )) && command -v powershell >/dev/null 2>&1; then
      printf '%s\n' "${files[@]}" | powershell -NoProfile -ExecutionPolicy Bypass -File "$fmt_ps1" -Serial -StdinFiles
    else
      printf '%s\n' "${files[@]}" | "$fmt_sh" --serial --stdin-files
    fi
  }
  if (( MODE_FIX )); then
    run_fmt_fix
    # When operating on staged files, restage post-format.
    if [[ "$SCOPE" == "git" ]]; then
      git add -- "${files[@]}"
    fi
  fi
  if run_fmt_check; then
    ok "format: clean"; return 0
  else
    err "format: drift detected (run 'scripts/lint.sh format --fix --git' to repair)"
    return 1
  fi
}

lint_cppcheck() {
  section "cppcheck"
  local args=(--profile ci --format human --parallel)
  case "$SCOPE" in
    diff) args+=(--diff-base "$DIFF_REF") ;;
    git)  args+=(--git) ;;
  esac
  [[ -n "$BASELINE" ]] && args+=(--baseline "$BASELINE")
  if "$REPO_ROOT/scripts/check-quality.sh" "${args[@]}"; then
    ok "cppcheck: clean"; return 0
  else
    local rc=$?
    err "cppcheck: $rc finding(s)"
    return $rc
  fi
}

lint_opengrep() {
  section "opengrep (audit profile)"
  local env_pfx=()
  env_pfx+=("OPENGREP_PROFILE=audit")
  env_pfx+=("OPENGREP_OUT_DIR=.artifacts/opengrep")
  (( STRICT )) && env_pfx+=("OPENGREP_STRICT=1")
  [[ -n "$BASELINE" ]] && env_pfx+=("OPENGREP_BASELINE_COMMIT=$BASELINE")
  local target=()
  case "$SCOPE" in
    paths) target=("${PATHS[@]}") ;;
    git)   target=(); while IFS= read -r -d '' f; do target+=("$f"); done < <(resolve_paths "$CPP_RE") ;;
    diff)  target=(); while IFS= read -r -d '' f; do target+=("$f"); done < <(resolve_paths "$CPP_RE") ;;
    all-files) target=(src include tests) ;;
  esac
  if [[ "$SCOPE" =~ ^(git|diff)$ && ${#target[@]} -eq 0 ]]; then
    ok "opengrep: no files in scope"; return 0
  fi
  local json_out=".artifacts/opengrep/opengrep-yams-audit.json"
  if env "${env_pfx[@]}" "$REPO_ROOT/scripts/dev/run_opengrep.sh" "${target[@]}"; then
    local findings=0
    if [[ -f "$json_out" ]]; then
      findings=$(python3 - "$json_out" <<'PYCOUNT'
import json, sys
try:
    with open(sys.argv[1], 'r', encoding='utf-8') as fh:
        print(len(json.load(fh).get('results', [])))
except Exception:
    print(0)
PYCOUNT
)
    fi
    if (( findings > 0 )); then
      if (( STRICT )); then
        err "opengrep: ${findings} finding(s) reported (see $json_out)"
        return 1
      fi
      warn "opengrep: ${findings} finding(s) reported (non-strict; see $json_out)"
      return 0
    fi
    ok "opengrep: clean"; return 0
  else
    err "opengrep: findings reported"; return 1
  fi
}

lint_tidy() {
  section "clang-tidy"
  if ! command -v clang-tidy >/dev/null 2>&1; then
    warn "clang-tidy not installed; skipping"; return 0
  fi
  local files=() rc=0
  while IFS= read -r -d '' f; do files+=("$f"); done < <(resolve_paths "$CPP_RE")
  if [[ ${#files[@]} -eq 0 ]]; then ok "tidy: no files in scope"; return 0; fi
  for f in "${files[@]:-}"; do
    [[ -z "$f" ]] && continue
    clang-tidy "$f" --quiet 2>/dev/null || rc=1
  done
  if (( rc == 0 )); then ok "tidy: clean"; else err "tidy: findings reported"; fi
  return $rc
}

# ----- Helpers: factored out from .githooks/pre-commit ------------------

lint_windows_hdr() {
  section "windows-hdr (Windows.h include order + WIN32_LEAN_AND_MEAN/NOMINMAX)"
  local files=() errors=()
  while IFS= read -r -d '' f; do files+=("$f"); done < <(resolve_paths "$CPP_RE")
  if [[ ${#files[@]} -eq 0 ]]; then ok "windows-hdr: no files in scope"; return 0; fi
  for f in "${files[@]}"; do
    [[ -f "$f" ]] || continue
    if grep -qE '#include\s*<[^>]*[Pp]sapi\.h[^>]*>' "$f"; then
      local psapi_line win_line lean_mean_line nominmax_line
      psapi_line=$(grep -nE '#include\s*<[^>]*[Pp]sapi\.h[^>]*>' "$f" | head -1 | cut -d: -f1)
      win_line=$(grep -nE '#include\s*<[^>]*[Ww]indows\.h[^>]*>' "$f" | head -1 | cut -d: -f1 || echo "")
      if [[ -z "$win_line" ]]; then
        errors+=("$f: Missing #include <Windows.h> before Psapi.h (Psapi.h depends on Windows types)")
      elif (( psapi_line < win_line )); then
        errors+=("$f: Psapi.h included at line $psapi_line, but Windows.h must come first (line $win_line)")
      fi
      lean_mean_line=$(grep -nE '#define\s+WIN32_LEAN_AND_MEAN' "$f" | head -1 | cut -d: -f1 || echo "")
      if [[ -z "$lean_mean_line" ]]; then
        errors+=("$f: Missing #define WIN32_LEAN_AND_MEAN before Windows headers")
      elif [[ -n "$win_line" && "$lean_mean_line" -gt "$win_line" ]]; then
        errors+=("$f: WIN32_LEAN_AND_MEAN must be defined before Windows.h (currently at line $lean_mean_line, Windows.h at $win_line)")
      fi
      nominmax_line=$(grep -nE '#define\s+NOMINMAX' "$f" | head -1 | cut -d: -f1 || echo "")
      if [[ -z "$nominmax_line" ]]; then
        errors+=("$f: Missing #define NOMINMAX before Windows headers")
      elif [[ -n "$win_line" && "$nominmax_line" -gt "$win_line" ]]; then
        errors+=("$f: NOMINMAX must be defined before Windows.h (currently at line $nominmax_line, Windows.h at $win_line)")
      fi
    fi
  done
  if (( ${#errors[@]} > 0 )); then
    err "windows-hdr: ${#errors[@]} violation(s)"
    for e in "${errors[@]}"; do printf '  - %s\n' "$e" >&2; done
    cat >&2 <<'EOF'
Fix: Include Windows.h BEFORE Psapi.h, and define WIN32_LEAN_AND_MEAN and NOMINMAX first.
Example:
  #ifdef _WIN32
  #ifndef WIN32_LEAN_AND_MEAN
  #define WIN32_LEAN_AND_MEAN 1
  #endif
  #ifndef NOMINMAX
  #define NOMINMAX 1
  #endif
  #include <Windows.h>
  #include <Psapi.h>
  #endif
EOF
    return 1
  fi
  ok "windows-hdr: clean"
}

lint_license() {
  section "license (GPL-3.0-or-later only)"
  local files=() errors=()
  while IFS= read -r -d '' f; do files+=("$f"); done < <(resolve_paths "$CPP_RE")
  if [[ ${#files[@]} -eq 0 ]]; then ok "license: no files in scope"; return 0; fi
  for f in "${files[@]}"; do
    [[ -f "$f" ]] || continue
    [[ "$f" == *"third_party/"* || "$f" == *"subprojects/"* || "$f" == *"external/"* ]] && continue
    if grep -qE "SPDX-License-Identifier:\s*Apache-2\.0" "$f"; then
      errors+=("$f: SPDX-License-Identifier: Apache-2.0 (must be GPL-3.0-or-later)")
    fi
    if grep -qE "SPDX-License-Identifier:\s*MIT" "$f"; then
      errors+=("$f: SPDX-License-Identifier: MIT (must be GPL-3.0-or-later)")
    fi
    if grep -qE "SPDX-License-Identifier:\s*BSD" "$f"; then
      errors+=("$f: SPDX-License-Identifier: BSD-XX (must be GPL-3.0-or-later)")
    fi
    if grep -qE "Licensed under the Apache License, Version 2\.0" "$f"; then
      errors+=("$f: Apache License text body (must be GPL-3.0-or-later)")
    fi
    if grep -qE "Permission is hereby granted, free of charge" "$f"; then
      errors+=("$f: MIT License text body (must be GPL-3.0-or-later)")
    fi
  done
  if (( ${#errors[@]} > 0 )); then
    err "license: ${#errors[@]} non-GPL header(s)"
    for e in "${errors[@]}"; do printf '  - %s\n' "$e" >&2; done
    cat >&2 <<'EOF'
Fix: Update header to GPL-3.0-or-later:
  // SPDX-License-Identifier: GPL-3.0-or-later

Or run: python3 scripts/update-licenses.py
Note: third_party/, subprojects/, external/ are excluded from this check.
EOF
    return 1
  fi
  ok "license: clean"
}

# ----- Helpers: metadata checks -----------------------------------------

lint_commitlint() {
  section "commitlint"
  if ! command -v npx >/dev/null 2>&1; then
    warn "npx not installed; skipping"; return 0
  fi
  local from="${COMMITLINT_FROM:-${GITHUB_BASE_SHA:-}}"
  local to="${COMMITLINT_TO:-${GITHUB_HEAD_SHA:-HEAD}}"
  if [[ -z "$from" ]]; then
    from=$(git merge-base HEAD origin/main 2>/dev/null || git rev-list --max-parents=0 HEAD | head -1)
  fi
  if npx --no-install commitlint --from "$from" --to "$to" --verbose 2>&1; then
    ok "commitlint: $(git rev-list --count "$from..$to") commit(s) clean"
  else
    err "commitlint: violations found"; return 1
  fi
}

lint_dco() {
  section "dco (Signed-off-by)"
  local from="${DCO_BASE_SHA:-${GITHUB_BASE_SHA:-}}"
  local to="${DCO_HEAD_SHA:-${GITHUB_HEAD_SHA:-HEAD}}"
  if [[ -z "$from" ]]; then
    from=$(git merge-base HEAD origin/main 2>/dev/null || echo "HEAD~1")
  fi
  local missing=0
  while read -r commit; do
    if ! git log -1 --format=%B "$commit" | grep -qE '^Signed-off-by: .+ <.+@.+>$'; then
      err "Missing Signed-off-by in commit $commit"
      git log -1 --format=%B "$commit" | sed 's/^/    /' >&2
      missing=1
    fi
  done < <(git rev-list "$from..$to" 2>/dev/null)
  if (( missing )); then
    err "dco: one or more commits missing sign-off"; return 1
  fi
  ok "dco: all commits in $from..$to signed"
}

lint_actionlint() {
  section "actionlint (workflow YAML)"
  if ! command -v actionlint >/dev/null 2>&1; then
    warn "actionlint not installed (https://github.com/rhysd/actionlint); skipping"; return 0
  fi
  if actionlint -color; then
    ok "actionlint: workflows clean"
  else
    err "actionlint: violations found"; return 1
  fi
}

# ----- Helpers: deps audit ----------------------------------------------

lint_deps_conan() {
  section "deps-conan (pinned package version drift)"
  if ! command -v conan >/dev/null 2>&1; then
    warn "conan not installed; skipping"; return 0
  fi
  local pinned drift=0 line name version
  pinned=$(grep -nE "self\.requires\(\"[^\"]+/[^\"]+\"" conanfile.py | sed -E 's/.*self\.requires\("([^"]+)\/([^"]+)".*/\1\t\2/')
  printf "%-24s %-18s %-18s %s\n" "package" "pinned" "latest" "status" >&2
  while IFS=$'\t' read -r name version; do
    [[ -z "$name" ]] && continue
    local latest
    latest=$(conan search "$name" --remote=conancenter --format=json 2>/dev/null \
      | jq -r '.conancenter[][] | .reference // empty' 2>/dev/null \
      | sed -E "s|^${name}/||" | sort -V | tail -1)
    [[ -z "$latest" ]] && latest="?"
    if [[ "$latest" == "?" ]] || [[ "$latest" == "$version" ]]; then
      printf "  %-22s %-18s %-18s %s\n" "$name" "$version" "$latest" "ok" >&2
    else
      printf "  %-22s %-18s %-18s %s\n" "$name" "$version" "$latest" "DRIFT" >&2
      drift=1
    fi
  done <<< "$pinned"
  if (( drift && STRICT )); then
    err "deps-conan: drift detected (--strict)"; return 1
  fi
  ok "deps-conan: report complete"
}

lint_deps_cve() {
  section "deps-cve (OSV scan)"
  if ! command -v osv-scanner >/dev/null 2>&1; then
    warn "osv-scanner not installed (https://google.github.io/osv-scanner/); skipping"
    warn "  install: brew install osv-scanner   |   go install github.com/google/osv-scanner/cmd/osv-scanner@latest"
    return 0
  fi
  if [[ ! -f conan.lock ]]; then
    if command -v conan >/dev/null 2>&1; then
      say "generating conan.lock for OSV scan"
      conan lock create . --lockfile-out=conan.lock 2>/dev/null || warn "conan lock create failed; continuing"
    fi
  fi
  if [[ ! -f conan.lock ]]; then
    warn "no conan.lock available; skipping CVE scan"; return 0
  fi
  if osv-scanner --lockfile=conan.lock --format=table; then
    ok "deps-cve: no known vulnerabilities"
  else
    err "deps-cve: CVE(s) found"; return 1
  fi
}

lint_deps_subs() {
  section "deps-subs (submodule freshness)"
  local lag=0
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    if [[ ! -d "$path/.git" && ! -f "$path/.git" ]]; then
      warn "submodule $path not initialized; skipping"; continue
    fi
    git -C "$path" fetch --quiet 2>/dev/null || true
    local upstream behind
    upstream=$(git -C "$path" rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null || echo "")
    if [[ -z "$upstream" ]]; then
      warn "$path: no upstream tracked; skipping"; continue
    fi
    behind=$(git -C "$path" rev-list --count "HEAD..$upstream" 2>/dev/null || echo "?")
    if [[ "$behind" == "0" ]]; then
      printf "  %-40s up to date\n" "$path" >&2
    else
      printf "  %-40s %s commit(s) behind %s\n" "$path" "$behind" "$upstream" >&2
      (( behind > 0 )) && lag=1
    fi
  done < <(git config --file .gitmodules --get-regexp 'submodule\..*\.path' | awk '{print $2}')
  if (( lag && STRICT )); then
    err "deps-subs: submodule(s) behind upstream (--strict)"; return 1
  fi
  ok "deps-subs: report complete"
}

lint_deps_licenses() {
  section "deps-licenses (Conan + submodule license allowlist)"
  # Allowlist of licenses compatible with GPL-3.0-or-later.
  local allow_re='^(GPL-3\.0|LGPL-3\.0|MPL-2\.0|Apache-2\.0|BSD|MIT|Zlib|ISC|Boost|BSL-1\.0|Unlicense|Public-Domain)'
  local violations=0
  if command -v conan >/dev/null 2>&1; then
    local graph
    graph=$(conan graph info . --format=json 2>/dev/null || echo '{}')
    if [[ "$graph" != "{}" ]]; then
      while IFS=$'\t' read -r ref license; do
        [[ -z "$ref" || -z "$license" || "$license" == "null" ]] && continue
        if ! [[ "$license" =~ $allow_re ]]; then
          err "license: $ref → $license (not on GPL-3.0-or-later allowlist)"
          violations=1
        fi
      done < <(echo "$graph" | jq -r '.graph.nodes // {} | to_entries[] | select(.value.ref != null and .value.ref != "conanfile") | "\(.value.ref)\t\(.value.license // "")"' 2>/dev/null)
    else
      warn "conan graph info empty; skipping Conan license check"
    fi
  else
    warn "conan not installed; skipping Conan license check"
  fi
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    [[ ! -d "$path" ]] && continue
    local lf
    lf=$(find "$path" -maxdepth 2 -iname 'LICENSE*' -o -iname 'COPYING*' 2>/dev/null | head -1)
    if [[ -z "$lf" ]]; then
      warn "submodule $path: no LICENSE/COPYING file detected"; continue
    fi
  done < <(git config --file .gitmodules --get-regexp 'submodule\..*\.path' | awk '{print $2}')
  if (( violations )); then return 1; fi
  ok "deps-licenses: all on GPL-3.0-or-later allowlist"
}

# ----- Dispatcher -------------------------------------------------------

resolve_fn() {
  case "$1" in
    format)        echo lint_format ;;
    cppcheck)      echo lint_cppcheck ;;
    tidy)          echo lint_tidy ;;
    opengrep)      echo lint_opengrep ;;
    windows-hdr)   echo lint_windows_hdr ;;
    license)       echo lint_license ;;
    commitlint)    echo lint_commitlint ;;
    dco)           echo lint_dco ;;
    actionlint)    echo lint_actionlint ;;
    deps-conan)    echo lint_deps_conan ;;
    deps-cve)      echo lint_deps_cve ;;
    deps-subs)     echo lint_deps_subs ;;
    deps-licenses) echo lint_deps_licenses ;;
    *)             echo "" ;;
  esac
}

EXIT_CODE=0
for c in "${CHECKS[@]}"; do
  fn=$(resolve_fn "$c")
  if [[ -z "$fn" ]]; then
    err "unknown check: $c"; EXIT_CODE=2; continue
  fi
  if ! "$fn"; then
    EXIT_CODE=1
  fi
done

if (( EXIT_CODE == 0 )); then
  ok "lint: all checks passed (${#CHECKS[@]} run)"
else
  err "lint: failed (exit $EXIT_CODE)"
fi
exit $EXIT_CODE
