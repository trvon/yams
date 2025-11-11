#!/bin/bash
# Generate seed corpus from existing protocol tests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CORPUS_DIR="${PROJECT_ROOT}/data/fuzz/corpus"

echo "Generating seed corpus from protocol tests..."

# Create corpus directories
mkdir -p "${CORPUS_DIR}/ipc" "${CORPUS_DIR}/add_document"

# Generate minimal valid frames
# TODO: Extract from actual test runs
echo "Creating minimal seed inputs..."

# Minimal IPC frame header (20 bytes)
printf '\x59\x41\x4D\x53'  > "${CORPUS_DIR}/ipc/01_magic.bin"      # YAMS magic
printf '\x00\x00\x00\x01' >> "${CORPUS_DIR}/ipc/01_magic.bin"      # version=1
printf '\x00\x00\x00\x00' >> "${CORPUS_DIR}/ipc/01_magic.bin"      # payload_size=0
printf '\x00\x00\x00\x00' >> "${CORPUS_DIR}/ipc/01_magic.bin"      # checksum=0
printf '\x00\x00\x00\x00' >> "${CORPUS_DIR}/ipc/01_magic.bin"      # flags=0

echo "Seed corpus generated in ${CORPUS_DIR}"
echo "Add more seeds by running integration tests and capturing traffic"
