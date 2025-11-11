#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
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
  fuzz <target>         Run AFL++ fuzzer for target (ipc_protocol, add_document)
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
    docker build --platform linux/amd64 \
        -f "${SCRIPT_DIR}/Dockerfile" \
        -t "${IMAGE_NAME}" \
        "${PROJECT_ROOT}"
}

cmd_fuzz() {
    local target="${1:-}"
    if [[ -z "$target" ]]; then
        echo "Error: target required (ipc_protocol or add_document)"
        exit 1
    fi

    local fuzzer_bin="/src/build/fuzzing/tools/fuzzing/fuzz_${target}"
    local corpus_dir="/src/data/fuzz/corpus/${target}"
    local findings_dir="/src/data/fuzz/findings/${target}"

    echo "Running AFL++ fuzzer for target: ${target}"
    docker run --rm -ti --platform linux/amd64 \
        -v "${PROJECT_ROOT}/data/fuzz:/src/data/fuzz" \
        "${IMAGE_NAME}" \
        afl-fuzz -i "${corpus_dir}" -o "${findings_dir}" -m none "${fuzzer_bin}"
}

cmd_shell() {
    docker run --rm -ti --platform linux/amd64 "${IMAGE_NAME}" /bin/bash
}

cmd_clean() {
    docker rmi "${IMAGE_NAME}"
}

[[ $# -eq 0 ]] && usage

case "$1" in
    build) cmd_build ;;
    fuzz) shift; cmd_fuzz "$@" ;;
    shell) cmd_shell ;;
    clean) cmd_clean ;;
    *) usage ;;
esac
