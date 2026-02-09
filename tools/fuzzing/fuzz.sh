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
    build                 Build Docker image with fuzzers
    fuzz <target>         Run AFL++ fuzzer for target (e.g., ipc_protocol, ipc_roundtrip, proto_serializer)
    exec <command>        Run a command inside the fuzzing container
    shell                 Open interactive shell in container
    clean                 Remove Docker image

Examples:
  $0 build
  $0 fuzz ipc_protocol
  $0 shell
EOF
    exit 1
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

    echo "Running AFL++ fuzzer for target: ${target}"
    docker run --rm -ti \
        -v "${PROJECT_ROOT}/data/fuzz:/fuzz" \
        -e AFL_AUTORESUME=1 \
        "${IMAGE_NAME}" \
        afl-fuzz -S "${fuzzer_id}" -i "${corpus_dir}" -o "${findings_dir}" -m none "${fuzzer_bin}"
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
    exec) shift; cmd_exec "$@" ;;
    shell) cmd_shell ;;
    clean) cmd_clean ;;
    *) usage ;;
esac
