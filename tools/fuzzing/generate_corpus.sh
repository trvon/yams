#!/bin/bash
# Generate seed corpus from existing protocol tests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CORPUS_DIR="${PROJECT_ROOT}/data/fuzz/corpus"

echo "Generating seed corpus from protocol tests..."

# Create corpus directories
mkdir -p \
	"${CORPUS_DIR}/ipc_protocol" \
	"${CORPUS_DIR}/ipc_roundtrip" \
	"${CORPUS_DIR}/add_document" \
	"${CORPUS_DIR}/proto_serializer" \
	"${CORPUS_DIR}/request_handler" \
	"${CORPUS_DIR}/streaming_processor" \
	"${CORPUS_DIR}/query_parser" \
	"${CORPUS_DIR}/plugin_trust" \
	"${CORPUS_DIR}/plugin_abi_mount" \
	"${CORPUS_DIR}/plugin_abi_negotiation"

echo "Creating minimal seed inputs..."

# Minimal MessageFramer frame header (20 bytes)
min_frame="${CORPUS_DIR}/ipc_protocol/01_min_frame.bin"
printf '\x59\x41\x4D\x53'  >  "$min_frame"  # MAGIC: "YAMS"
printf '\x00\x00\x00\x01' >> "$min_frame"  # version=1
printf '\x00\x00\x00\x00' >> "$min_frame"  # payload_size=0
printf '\x00\x00\x00\x00' >> "$min_frame"  # checksum=0
printf '\x00\x00\x00\x00' >> "$min_frame"  # flags=0

cp "$min_frame" "${CORPUS_DIR}/ipc_roundtrip/01_min_frame.bin"

# Some non-empty byte seeds for payload-focused fuzzers
printf '\x00' > "${CORPUS_DIR}/add_document/00_zero.bin"
head -c 64 /dev/urandom > "${CORPUS_DIR}/add_document/01_random.bin"

printf '\x00' > "${CORPUS_DIR}/proto_serializer/00_zero.bin"
head -c 64 /dev/urandom > "${CORPUS_DIR}/proto_serializer/01_random.bin"

printf '\x00' > "${CORPUS_DIR}/request_handler/00_zero.bin"
head -c 64 /dev/urandom > "${CORPUS_DIR}/request_handler/01_random.bin"

printf '\x00' > "${CORPUS_DIR}/streaming_processor/00_zero.bin"
head -c 64 /dev/urandom > "${CORPUS_DIR}/streaming_processor/01_random.bin"

# User-facing query parser seeds (plain text)
cat > "${CORPUS_DIR}/query_parser/01_simple.txt" <<'EOF'
hello world
EOF
cat > "${CORPUS_DIR}/query_parser/02_boolean.txt" <<'EOF'
(title:hello OR title:world) AND content:test
EOF
cat > "${CORPUS_DIR}/query_parser/03_phrase.txt" <<'EOF'
"quoted phrase" author:bob
EOF
cat > "${CORPUS_DIR}/query_parser/04_wildcards.txt" <<'EOF'
tag:dev* file:readme?
EOF
cat > "${CORPUS_DIR}/query_parser/05_fuzzy.txt" <<'EOF'
helo~2 world~1
EOF
cat > "${CORPUS_DIR}/query_parser/06_ranges.txt" <<'EOF'
date:[2020 TO 2026] size:{10 TO 100}
EOF

# Plugin trust/path seeds.
# Format: base\0candidate\0trustfile-body
printf '/trusted\0/trusted_evil/plugin.so\0/trusted\n' > "${CORPUS_DIR}/plugin_trust/01_prefix_bypass.bin"
printf '/trusted\0/trusted/plugin.so\0/trusted\n#comment\n' > "${CORPUS_DIR}/plugin_trust/02_trusted_ok.bin"

# ABI plugin mount seeds (treated as config bytes and interface selectors)
cat > "${CORPUS_DIR}/plugin_abi_mount/01_empty_json.txt" <<'EOF'
{}
EOF
cat > "${CORPUS_DIR}/plugin_abi_mount/02_simple.txt" <<'EOF'
{"mode":"smoke","feature":"abi"}
EOF
head -c 128 /dev/urandom > "${CORPUS_DIR}/plugin_abi_mount/03_random.bin"

# ABI negotiation seeds (manifest/interface oriented)
cat > "${CORPUS_DIR}/plugin_abi_negotiation/01_ifaces.txt" <<'EOF'
fuzz_iface_v1 content_extractor_v1 dr_provider_v1
EOF
cat > "${CORPUS_DIR}/plugin_abi_negotiation/02_versions.txt" <<'EOF'
v=1 v=2 v=999
EOF
head -c 128 /dev/urandom > "${CORPUS_DIR}/plugin_abi_negotiation/03_random.bin"

echo "Generating structured IPC seeds (Search/Grep/Delete) via seedgen (if available)..."
if docker image inspect yams-fuzz >/dev/null 2>&1; then
	"${SCRIPT_DIR}/fuzz.sh" exec /src/build/fuzzing/tools/fuzzing/seedgen --out /fuzz/corpus --max-seeds 40 || true
else
	echo "Note: Docker image yams-fuzz not found; run ./tools/fuzzing/fuzz.sh build to enable structured seeds."
fi

echo "Seed corpus generated in ${CORPUS_DIR}"
echo "Add more seeds by running integration tests and capturing traffic"
