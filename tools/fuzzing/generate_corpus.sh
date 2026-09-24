#!/bin/bash
# Generate seed corpus from existing protocol tests.
# Usage: generate_corpus.sh [path/to/local/seedgen]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CORPUS_DIR="${PROJECT_ROOT}/data/fuzz/corpus"

echo "Generating seed corpus from protocol tests..."

# Create corpus directories
mkdir -p \
	"${CORPUS_DIR}/ipc_protocol" \
	"${CORPUS_DIR}/ipc_roundtrip" \
	"${CORPUS_DIR}/p2p_protocol" \
	"${CORPUS_DIR}/p2p_delta" \
	"${CORPUS_DIR}/add_document" \
	"${CORPUS_DIR}/proto_serializer" \
	"${CORPUS_DIR}/request_handler" \
	"${CORPUS_DIR}/streaming_processor" \
	"${CORPUS_DIR}/plugin_trust" \
	"${CORPUS_DIR}/plugin_abi_mount" \
	"${CORPUS_DIR}/plugin_abi_negotiation" \
	"${CORPUS_DIR}/download_jobs" \
	"${CORPUS_DIR}/fts5_query" \
	"${CORPUS_DIR}/utf8" \
	"${CORPUS_DIR}/text_extractors" \
	"${CORPUS_DIR}/compressed_block"

echo "Creating minimal seed inputs..."

# FTS5 query seeds: first byte selects prefix-wildcard handling.
printf '\x01dendritic cells (DCs)' >"${CORPUS_DIR}/fts5_query/01_plain.txt"
printf '\x01(foo OR bar) NOT baz' >"${CORPUS_DIR}/fts5_query/02_operators.txt"
printf '\x00title:report "exact phrase"' >"${CORPUS_DIR}/fts5_query/03_column_phrase.txt"
printf '\x01NEAR(hello world, 2) IL-6*' >"${CORPUS_DIR}/fts5_query/04_near_prefix.txt"

# UTF-8 / encoding seeds: valid multibyte, surrogate, overlong, UTF-16LE with BOM.
printf 'caf\xc3\xa9 \xf0\x9f\x98\x80' >"${CORPUS_DIR}/utf8/01_valid.bin"
printf 'a\xed\xa0\x80b\xe0\x80\x80c\xf4\x90\x80\x80' >"${CORPUS_DIR}/utf8/02_invalid.bin"
printf '\xff\xfeH\x00i\x00\x00\xd8A\x00' >"${CORPUS_DIR}/utf8/03_utf16le.bin"

# Text extractor seeds: HTML with scripts, comments and block tags.
printf '<html><head><title>T</title><script>x</script><style>y</style></head><body><!-- c --><p>a</p><div>b &amp; c</div></body></html>' \
	>"${CORPUS_DIR}/text_extractors/01_page.html"
printf '<script<script<!--<<<' >"${CORPUS_DIR}/text_extractors/02_unclosed.html"

# Compressed block seeds: mode 1 (round-trip) inputs; mode 0 headers are best found by the fuzzer.
printf '\x01hello hello hello hello hello' >"${CORPUS_DIR}/compressed_block/01_roundtrip.bin"
{
	printf '\x01'
	head -c 4096 /dev/zero | tr '\0' 'a'
} >"${CORPUS_DIR}/compressed_block/02_repetitive.bin"

# Minimal MessageFramer frame header (20 bytes)
min_frame="${CORPUS_DIR}/ipc_protocol/01_min_frame.bin"
printf '\x59\x41\x4D\x53'  >  "$min_frame"  # MAGIC: "YAMS"
printf '\x00\x00\x00\x01' >> "$min_frame"  # version=1
printf '\x00\x00\x00\x00' >> "$min_frame"  # payload_size=0
printf '\x00\x00\x00\x00' >> "$min_frame"  # checksum=0
printf '\x00\x00\x00\x00' >> "$min_frame"  # flags=0

cp "$min_frame" "${CORPUS_DIR}/ipc_roundtrip/01_min_frame.bin"

# Direct-P2P control seeds. Keep a protocol-v3 hello so exact-version rejection remains covered
# after the authenticated bounded-window protocol-v4 upgrade.
cat > "${CORPUS_DIR}/p2p_protocol/01_hello_v4.json" <<'EOF'
{"type":"hello","protocol":4,"schema_version":4,"node_id":"node-a","corpus_id":"corpus","corpus_epoch":1,"max_writer_advance":128,"max_writer_window_bytes":1048576}
EOF
cat > "${CORPUS_DIR}/p2p_protocol/02_legacy_hello_v3.json" <<'EOF'
{"type":"hello","protocol":3,"schema_version":4,"node_id":"node-a","corpus_id":"corpus","corpus_epoch":1,"max_writer_advance":128,"max_writer_window_bytes":1048576}
EOF
cat > "${CORPUS_DIR}/p2p_protocol/03_empty_state.json" <<'EOF'
{"type":"state","vv":{"counters_":{}},"seen":{},"commitments":[],"quarantined_writers":[]}
EOF
cat > "${CORPUS_DIR}/p2p_delta/01_delta_batch.json" <<'EOF'
{"type":"delta_batch","count":1,"has_more":false}
EOF
cat > "${CORPUS_DIR}/p2p_delta/02_replication_mode.json" <<'EOF'
{"type":"replication_mode","mode":"delta"}
EOF
cat > "${CORPUS_DIR}/p2p_delta/03_invalid_snapshot.json" <<'EOF'
{"type":"snapshot_begin","witness":"node-a","frontier":{"counters_":{}},"commitments":[],"record_count":18446744073709551615,"payload_bytes":0,"root_digest":"0000000000000000000000000000000000000000000000000000000000000000","witness_key_id":"node-a-v1","witness_algorithm":"Ed25519","witness_signature":"AA=="}
EOF

# Some non-empty byte seeds for payload-focused fuzzers
printf '\x00' > "${CORPUS_DIR}/add_document/00_zero.bin"
head -c 64 /dev/urandom > "${CORPUS_DIR}/add_document/01_random.bin"

printf '\x00' > "${CORPUS_DIR}/proto_serializer/00_zero.bin"
head -c 64 /dev/urandom > "${CORPUS_DIR}/proto_serializer/01_random.bin"

printf '\x00' > "${CORPUS_DIR}/request_handler/00_zero.bin"
head -c 64 /dev/urandom > "${CORPUS_DIR}/request_handler/01_random.bin"

printf '\x00' > "${CORPUS_DIR}/streaming_processor/00_zero.bin"
head -c 64 /dev/urandom > "${CORPUS_DIR}/streaming_processor/01_random.bin"

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

# Daemon download job IPC seeds
printf '\x00' > "${CORPUS_DIR}/download_jobs/00_zero.bin"
cat > "${CORPUS_DIR}/download_jobs/01_start_url.txt" <<'EOF'
https://example.com/archive.tar.gz
EOF
cat > "${CORPUS_DIR}/download_jobs/02_job_id.txt" <<'EOF'
job-download-123
EOF
head -c 96 /dev/urandom > "${CORPUS_DIR}/download_jobs/03_random.bin"

echo "Generating framed direct-P2P session and topology snapshot seeds..."
python3 "${SCRIPT_DIR}/p2p_seeds.py" "${CORPUS_DIR}"
cp "${CORPUS_DIR}/p2p_protocol/"*.json "${CORPUS_DIR}/p2p_json/" 2>/dev/null || true
cp "${CORPUS_DIR}/p2p_delta/"*.json "${CORPUS_DIR}/p2p_json/" 2>/dev/null || true

echo "Generating structured IPC seeds (Search/Grep/Delete) via seedgen (if available)..."
LOCAL_SEEDGEN="${1:-}"
if [[ -n "$LOCAL_SEEDGEN" && -x "$LOCAL_SEEDGEN" ]]; then
	"$LOCAL_SEEDGEN" --out "${CORPUS_DIR}" --max-seeds 40 || true
elif docker image inspect yams-fuzz >/dev/null 2>&1; then
	"${SCRIPT_DIR}/fuzz.sh" exec /src/build/fuzzing/tools/fuzzing/seedgen --out /fuzz/corpus --max-seeds 40 || true
else
	echo "Note: Docker image yams-fuzz not found; run ./tools/fuzzing/fuzz.sh build to enable structured seeds."
fi

echo "Seed corpus generated in ${CORPUS_DIR}"
echo "Add more seeds by running integration tests and capturing traffic"
