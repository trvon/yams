#!/bin/bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Wrapper script for AFL++ fuzzing with Docker
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
IMAGE_NAME="yams-fuzz"

usage() {
    cat <<EOF
Usage: $0 <command> [args...]

Commands:
    build                 Build Docker image with AFL++ fuzzers
    fuzz <target>         Run AFL++ fuzzer in Docker (e.g., p2p_protocol, p2p_delta, ipc_protocol)
    local <target> [libFuzzer args...]
                          Run a local libFuzzer build over data/fuzz/corpus/<target>
    exec <command>        Run a command inside the fuzzing container
    shell                 Open interactive shell in container
    clean                 Remove Docker image

Local libFuzzer build (clang with the libFuzzer runtime, e.g. Homebrew or apt LLVM; Apple clang
does not ship it). Every project object gets -fsanitize=fuzzer-no-link plus ASan/UBSan:
  CC=clang CXX=clang++ meson setup build/fuzz -Dbuild-fuzzers=true \\
      -Dbuild-tests=false -Dbuild-plugins=false -Dbuild-cli=false
  meson compile -C build/fuzz
  ./tools/fuzzing/generate_corpus.sh build/fuzz/tools/fuzzing/seedgen
  $0 local p2p_handshake -max_total_time=120
On macOS with Homebrew LLVM, also export BOOST_ROOT=\$(brew --prefix) before meson setup.
FUZZ_BUILD_DIR selects another build directory (default: build/fuzz). New corpus units and
crash-* artifacts land in <build-dir>/fuzz-runs/<target>/. A dictionary is passed automatically
when tools/fuzzing/dicts/<target>.dict exists (p2p_* targets fall back to dicts/p2p.dict).

Examples:
  $0 build
  AFL_FUZZ_SECONDS=60 $0 fuzz p2p_protocol
  $0 local p2p_json -max_total_time=60
  $0 shell
EOF
    exit 1
}

# Print the dictionary for a target (relative to tools/fuzzing), or nothing when none exists.
dict_for_target() {
    local target="$1"
    if [[ -f "${SCRIPT_DIR}/dicts/${target}.dict" ]]; then
        echo "dicts/${target}.dict"
    elif [[ "$target" == p2p_* && -f "${SCRIPT_DIR}/dicts/p2p.dict" ]]; then
        echo "dicts/p2p.dict"
    fi
}

cmd_build() {
    echo "Building fuzzer Docker image..."
    docker build \
        -f "${SCRIPT_DIR}/Dockerfile" \
        -t "${IMAGE_NAME}" \
        "${PROJECT_ROOT}"
}

cmd_fuzz() {
    local target="${1:-}"
    if [[ -z "$target" ]]; then
        echo "Error: target required (example: ipc_protocol, ipc_roundtrip, proto_serializer)"
        exit 1
    fi

    # AFL++ uses a per-fuzzer subdirectory inside -o; without -M/-S it defaults to "default".
    # If you have multiple terminals, "default" will collide. Use a unique ID by default.
    local fuzzer_id="${AFL_FUZZER_ID:-$(hostname)-$$}"

    local fuzzer_bin="/src/build/fuzzing/tools/fuzzing/fuzz_${target}"
    local corpus_dir="/fuzz/corpus/${target}"
    local findings_dir="/fuzz/findings/${target}"
    local duration_args=()
    if [[ -n "${AFL_FUZZ_SECONDS:-}" ]]; then
        if [[ ! "${AFL_FUZZ_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
            echo "Error: AFL_FUZZ_SECONDS must be a positive integer"
            exit 1
        fi
        duration_args=(-V "${AFL_FUZZ_SECONDS}")
    fi
    local tty_arg=""
    if [[ -t 0 && -t 1 ]]; then
        tty_arg="-it"
    fi

    # Ensure mounted host directories exist for AFL input/output.
    mkdir -p "${PROJECT_ROOT}/data/fuzz/corpus/${target}"
    mkdir -p "${PROJECT_ROOT}/data/fuzz/findings/${target}"

    local dict_args=()
    local dict
    dict="$(dict_for_target "$target")"
    if [[ -n "$dict" ]]; then
        dict_args=(-x "/src/tools/fuzzing/${dict}")
    fi

    echo "Running AFL++ fuzzer for target: ${target}"
    docker run --rm ${tty_arg:+"${tty_arg}"} \
        -v "${PROJECT_ROOT}/data/fuzz:/fuzz" \
        -e AFL_AUTORESUME=1 \
        "${IMAGE_NAME}" \
        afl-fuzz -S "${fuzzer_id}" -i "${corpus_dir}" -o "${findings_dir}" -m none \
        "${dict_args[@]}" "${duration_args[@]}" "${fuzzer_bin}"
}

cmd_local() {
    local target="${1:-}"
    if [[ -z "$target" ]]; then
        echo "Error: target required (example: p2p_json, p2p_handshake, topology_codec)"
        exit 1
    fi
    shift

    local build_dir="${FUZZ_BUILD_DIR:-${PROJECT_ROOT}/build/fuzz}"
    local fuzzer_bin="${build_dir}/tools/fuzzing/fuzz_${target}"
    if [[ ! -x "$fuzzer_bin" ]]; then
        echo "Error: ${fuzzer_bin} is not built; see '$0' usage for the libFuzzer build"
        exit 1
    fi
    local seeds="${PROJECT_ROOT}/data/fuzz/corpus/${target}"
    local run_dir="${build_dir}/fuzz-runs/${target}"
    mkdir -p "${run_dir}/corpus" "${run_dir}/artifacts" "${seeds}"

    local dict_args=()
    local dict
    dict="$(dict_for_target "$target")"
    if [[ -n "$dict" ]]; then
        dict_args=("-dict=${SCRIPT_DIR}/${dict}")
    fi

    echo "Running libFuzzer target ${target} (seeds: ${seeds})"
    "$fuzzer_bin" "${dict_args[@]}" "-artifact_prefix=${run_dir}/artifacts/" "$@" \
        "${run_dir}/corpus" "${seeds}"
}

cmd_exec() {
    if [[ $# -eq 0 ]]; then
        echo "Error: command required"
        exit 1
    fi

    docker run --rm -ti \
        -v "${PROJECT_ROOT}/data/fuzz:/fuzz" \
        "${IMAGE_NAME}" "$@"
}

cmd_shell() {
    docker run --rm -ti "${IMAGE_NAME}" /bin/bash
}

cmd_clean() {
    docker rmi "${IMAGE_NAME}"
}

[[ $# -eq 0 ]] && usage

case "$1" in
    build) cmd_build ;;
    fuzz) shift; cmd_fuzz "$@" ;;
    local) shift; cmd_local "$@" ;;
    exec) shift; cmd_exec "$@" ;;
    shell) cmd_shell ;;
    clean) cmd_clean ;;
    *) usage ;;
esac
