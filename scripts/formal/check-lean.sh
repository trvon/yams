#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
#
# Build the Lean formalization and gate it so that only the Lean kernel is trusted.
#
#   scripts/formal/check-lean.sh
#
# Fails when:
#   - `lake build` fails in formal/topology;
#   - any formal/**/*.lean uses sorry, admit, native_decide, sorryAx or
#     debug.skipKernelTC outside comments and strings;
#   - any formal/**/*.lean declares an axiom or opaque constant that is not
#     listed in formal/topology/axiom-allowlist.txt (fully qualified names);
#   - any theorem in formal/topology/main-theorems.txt depends on an axiom
#     outside {propext, Classical.choice, Quot.sound} plus the allowlist.
#
# Portable to macOS bash 3.2. Needs lake (elan) and perl on PATH.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../.." && pwd)
formal_dir="$repo_root/formal"
project_dir="$formal_dir/topology"
main_list="$project_dir/main-theorems.txt"
allowlist="$project_dir/axiom-allowlist.txt"
root_module="Yams.Topology"
standard_axioms="propext Classical.choice Quot.sound"

if ! command -v lake >/dev/null 2>&1 && [ -x "$HOME/.elan/bin/lake" ]; then
  PATH="$HOME/.elan/bin:$PATH"
fi
for tool in lake perl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "check-lean: $tool not found on PATH" >&2
    exit 2
  fi
done

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/check-lean.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}

# Print non-comment, non-blank lines of a list file.
read_list() {
  [ -f "$1" ] || return 0
  sed -e 's/#.*//' -e 's/[[:space:]]*$//' -e 's/^[[:space:]]*//' "$1" | grep -v '^$' || true
}

allow_extra=" $(read_list "$allowlist" | tr "\n" " ")"
allowed_axioms="$standard_axioms$allow_extra"

# --- 1. Source scan --------------------------------------------------------
# The perl filter blanks comments (nested /- -/ and --) and string literals,
# keeping line numbers, then reports forbidden tokens and axiom/opaque
# declarations with their namespace-qualified names.
# shellcheck disable=SC2016 # perl source, not shell
scan_lean='
use strict; use warnings;
my $file = shift @ARGV;
local $/; my $s = <STDIN>; $s = "" unless defined $s;
my ($o, $d, $i, $n) = ("", 0, 0, length $s);
while ($i < $n) {
  my $c = substr($s, $i, 1); my $t = substr($s, $i, 2);
  if ($d > 0) {
    if ($t eq "/-") { $d++; $i += 2; $o .= "  "; next; }
    if ($t eq "-/") { $d--; $i += 2; $o .= "  "; next; }
    $o .= ($c eq "\n") ? "\n" : " "; $i++; next;
  }
  if ($t eq "/-") { $d = 1; $i += 2; $o .= "  "; next; }
  if ($t eq "--") { $i++ while ($i < $n && substr($s, $i, 1) ne "\n"); next; }
  if ($c eq "\"") {
    $o .= " "; $i++;
    while ($i < $n) {
      my $e = substr($s, $i, 1);
      if ($e eq "\\") { $i += 2; $o .= "  "; next; }
      $i++;
      if ($e eq "\"") { $o .= " "; last; }
      $o .= ($e eq "\n") ? "\n" : " ";
    }
    next;
  }
  $o .= $c; $i++;
}
my $id = qr/[A-Za-z0-9_\x{27}!?]/;
my @scope; my $ln = 0;
for my $line (split /\n/, $o, -1) {
  $ln++;
  while ($line =~ /(?<![A-Za-z0-9_\x{27}!?.])(sorry|admit|native_decide|sorryAx|debug\.skipKernelTC)(?!$id)/g) {
    print "FORBID\t$file\t$ln\t$1\n";
  }
  if ($line =~ /^\s*namespace\s+(\S+)/) { push @scope, ["n", $1]; }
  elsif ($line =~ /^\s*section\b\s*(\S*)/) { push @scope, ["s", $1]; }
  elsif ($line =~ /^\s*end\b/) { pop @scope; }
  while ($line =~ /(?<![A-Za-z0-9_\x{27}!?.])(axiom|opaque)\s+([^\s:({\[]+)/g) {
    my ($kind, $name) = ($1, $2);
    if ($name =~ s/^_root_\.//) { }
    else {
      my @ns = map { $_->[1] } grep { $_->[0] eq "n" } @scope;
      $name = join(".", @ns, $name) if @ns;
    }
    print "DECL\t$file\t$ln\t$kind\t$name\n";
  }
}
'

echo "== source scan (formal/**/*.lean)"
lean_files="$tmp_dir/files.txt"
find "$formal_dir" -name '*.lean' -not -path '*/.lake/*' | LC_ALL=C sort >"$lean_files"
if [ ! -s "$lean_files" ]; then
  fail "no .lean files under $formal_dir"
fi
scan_out="$tmp_dir/scan.txt"
: >"$scan_out"
while IFS= read -r f; do
  rel=${f#"$repo_root"/}
  perl -CSD -e "$scan_lean" "$rel" <"$f" >>"$scan_out"
done <"$lean_files"

while IFS="$(printf '\t')" read -r tag file line a b; do
  case "$tag" in
    FORBID)
      fail "$file:$line: forbidden token '$a'"
      ;;
    DECL)
      case "$allow_extra " in
        *" $b "*) echo "  allowlisted $a $b ($file:$line)" ;;
        *) fail "$file:$line: $a declaration '$b' not in ${allowlist#"$repo_root"/}" ;;
      esac
      ;;
  esac
done <"$scan_out"
echo "  scanned $(wc -l <"$lean_files" | tr -d ' ') files"

# --- 2. Build --------------------------------------------------------------
echo "== lake build ($(cat "$project_dir/lean-toolchain"))"
build_ok=1
start=$(date +%s)
if ! (cd "$project_dir" && lake build); then
  fail "lake build failed"
  build_ok=0
fi
echo "  lake build took $(($(date +%s) - start))s"

# --- 3. Axiom dependencies of the main theorems ---------------------------
echo "== #print axioms (${main_list#"$repo_root"/})"
theorems="$tmp_dir/theorems.txt"
read_list "$main_list" >"$theorems"
if [ ! -s "$theorems" ]; then
  fail "no theorems listed in $main_list"
elif [ "$build_ok" -eq 1 ]; then
  probe="$tmp_dir/PrintAxioms.lean"
  {
    echo "import $root_module"
    while IFS= read -r t; do echo "#print axioms $t"; done <"$theorems"
  } >"$probe"
  axioms_out="$tmp_dir/axioms.txt"
  if ! (cd "$project_dir" && lake env lean "$probe") >"$axioms_out" 2>&1; then
    sed 's/^/  /' "$axioms_out" >&2
    fail "lean could not check the main theorems (unknown name or error above)"
  else
    # One line per theorem: name<TAB>comma-separated axioms ("" if none).
    perl -0777 -ne '
      while (/\x27(\S+)\x27 (?:does not depend on any axioms|depends on axioms: \[([^\]]*)\])/g) {
        my ($t, $ax) = ($1, defined $2 ? $2 : "");
        $ax =~ s/\s+//g;
        print "$t\t$ax\n";
      }' "$axioms_out" >"$tmp_dir/parsed.txt"
    checked=0
    while IFS= read -r t; do
      row=$(awk -F '\t' -v t="$t" '$1 == t { print; exit }' "$tmp_dir/parsed.txt")
      if [ -z "$row" ]; then
        fail "no #print axioms result for $t"
        continue
      fi
      checked=$((checked + 1))
      ax=$(printf '%s\n' "$row" | cut -f2)
      bad=""
      for a in $(printf '%s' "$ax" | tr ',' ' '); do
        case " $allowed_axioms sorryAx " in
          *" $a "*) ;;
          *) bad="$bad $a" ;;
        esac
      done
      case ",$ax," in
        *,sorryAx,*) fail "$t depends on sorryAx (a sorry reached the kernel)" ;;
      esac
      if [ -n "$bad" ]; then
        fail "$t depends on non-standard axioms:$bad"
      fi
    done <"$theorems"
    printf '  checked %s theorems; axiom usage:\n' "$checked"
    cut -f2 "$tmp_dir/parsed.txt" | sed 's/^$/(none)/' | LC_ALL=C sort | uniq -c | sed 's/^/  /'
  fi
fi

if [ "$failures" -gt 0 ]; then
  echo "check-lean: $failures failure(s)" >&2
  exit 1
fi
echo "check-lean: ok"
