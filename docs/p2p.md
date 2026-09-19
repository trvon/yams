# P2P corpus sync

YAMS can reconcile one corpus between running daemons. Direct transport is the recommended mode: it uses TCP with TLS 1.3 mutual identity and does not require a shared filesystem, object store, VPN, or relay.

## Configure each daemon

Add a `[memory_sync]` section to each daemon's active `config.toml`:

```toml
[memory_sync]
enabled = true
transport = "direct"
listen = "0.0.0.0:9721"
node_id = "123e4567-e89b-42d3-a456-426614174000"
corpus_id = "team-memory"
corpus_epoch = 1
mode = "persistent"
allow_first_contact = false
writer_auth_required = true
writer_auth_manifest = "/absolute/path/to/writers.json"
```

Requirements:

- Give every daemon a different stable UUID in `node_id`.
- Use the same `corpus_id` and positive `corpus_epoch` on intended peers.
- Keep the node ID and data directory across restarts.
- Expose the listen port only on networks where the peer should reach it. YAMS encrypts and authenticates the session; the network itself need not be trusted.
- Direct transport requires a usable writer-authentication manifest bound to the same node, corpus, and epoch. The manifest must contain the local Ed25519 private key and every trusted writer public key needed to verify retained history and cold-bootstrap snapshots.
- The manifest and trusted public-key files must be regular files owned by the daemon user and protected against group/other writes; Windows DACLs must be protected from inheritance and grant write authority only to that user. Public readability is allowed.
- When `identity_key` is omitted, direct transport reuses the manifest's local private key for TLS identity. Otherwise the daemon creates or loads an owner-only persistent key; Unix group/other permissions and Windows ACL entries outside the file owner fail closed.

Generate a node ID and owner-only Ed25519 writer key when needed:

```bash
uuidgen | tr '[:upper:]' '[:lower:]'
openssl genpkey -algorithm ED25519 -out writer-private.pem
openssl pkey -in writer-private.pem -pubout -out writer-public.pem
chmod 600 writer-private.pem # Unix; use an owner-only ACL on Windows
chmod go-w writer-public.pem # protect trust membership from writes
```

Each node's manifest binds the local key and all operator-approved writer public keys to one corpus epoch:

```json
{
  "schema_version": 1,
  "corpus_id": "team-memory",
  "corpus_epoch": 1,
  "local_key": {
    "writer_id": "123e4567-e89b-42d3-a456-426614174000",
    "key_id": "node-a-v1",
    "private_key_path": "writer-private.pem"
  },
  "trusted_writers": [
    {
      "writer_id": "123e4567-e89b-42d3-a456-426614174000",
      "key_id": "node-a-v1",
      "public_key_path": "writer-public.pem"
    },
    {
      "writer_id": "223e4567-e89b-42d3-a456-426614174000",
      "key_id": "node-b-v1",
      "public_key_path": "peer-writer-public.pem"
    }
  ]
}
```

Relative key paths resolve from the manifest directory. Distribute public keys and certificate pins through a separate trusted channel; never distribute the private key or rely on TOFU for cold bootstrap authority.

### Upgrade from unsigned direct history

Direct stores created before authenticated envelopes have unknown signing provenance and are not migrated silently. Startup fails closed if `data_dir/p2p/op-store` contains history without the matching `authenticated-store-v1.json` marker. Preserve that store for audit, configure and distribute new writer manifests, advance `corpus_epoch`, move the old `op-store` out of the active data directory, and bootstrap a fresh authenticated store. Do not copy or locally re-sign old envelopes into the new epoch.

The trust-schema upgrade also invalidates every legacy `pinned_by_operator` bit because older `connect ?pin=` calls could set it without enrollment. Re-run `yams p2p enroll` for each verified peer; ordinary TOFU and connection pins remain non-authoritative.

Restart both daemons after changing configuration, then confirm the compact status:

```bash
yams daemon status
```

Use `yams daemon status -d` for corpus, identity, peer, counter, and failure details. Status is
local activity and health telemetry, not a global convergence claim: `peer_count` includes the
local peer registry, `successful_cycles` counts completed local reconciliation cycles, and a recent
success does not prove that every enrolled peer is reachable or has the same frontier. Confirm
convergence with explicit per-node corpus/frontier checks after the required full-mesh sessions.

## Enroll and connect peers

Unknown inbound peers are rejected by default before application state or deltas are exchanged. On each node, obtain the local public identity:

```bash
yams p2p identity --json
```

Exchange each node's `node_id` and `spki_pin` through a separate trusted channel. Enroll node B on node A, and node A on node B:

```bash
yams p2p enroll <peer-node-id> <peer-sha256-spki-pin>
```

After both operators enroll the other identity, either node can initiate synchronization:

```bash
yams p2p connect "host.example:9721?pin=<peer-sha256-spki-pin>&remember=true"
yams p2p peers
yams p2p peers --json
```

`connect` remembers the endpoint by default and retries it with bounded backoff after a disconnect or restart. Use `remember=false` for a one-shot connection:

```bash
yams p2p connect "host.example:9721?pin=<peer-sha256-spki-pin>&remember=false"
```

Setting `allow_first_contact = true` explicitly restores legacy TOFU recognition and emits a startup warning. It permits unsolicited peers to persist a certificate pin, but that pin is not operator enrollment and can never authorize cold bootstrap. Promote the exact existing node/pin only with `yams p2p enroll` after verification through a separate trusted channel.

Disconnect and disable automatic reconnect:

```bash
yams p2p disconnect <peer-node-id>
```

Direct protocol v4 is full mesh: with three nodes, connect every pair. Protocol versions are exact-match; upgrade all enrolled direct peers together because v3 and v4 do not interoperate. A node sends only its own incremental operations; it does not relay a third node's delta history. The authenticated handshake exchanges bounded per-writer full-history commitments and proves the receiver's exact nonzero prefix before application deltas move. Each connection stages one resource-bounded writer window whose exact endpoint is negotiated and authenticated during the handshake. The window must match that handshake-frozen prefix before any operation is applied; the next connection proves and resumes from the resulting durable prefix. Writer-counter entries make steady-state export proportional to the returned window instead of the complete retained history. A mismatch durably quarantines the authenticated writer, removes its visible winners through adapter invalidations, and stops the session; operators must investigate or re-enroll a replacement corpus epoch rather than clearing protocol history manually.

Inbound TCP handshakes use a bounded pre-authentication pool that is separate from the bounded mutually authenticated session pool, so a stalled unauthenticated TLS client cannot reserve an application-session slot. Both pools have hard implementation ceilings independent of caller configuration. TLS handshakes retain their own short deadline, while every accepted or initiated connection also receives one absolute transport I/O deadline shared by all later protocol frames; repeated small frames cannot extend it. Local handler work between I/O calls remains cooperative and must preserve its own operation bounds. Listener, session, and reconnect thread boundaries contain and report exceptions instead of terminating the daemon. Shutdown schedules socket cancellation on each connection's own I/O context before joining workers, avoiding concurrent TLS socket teardown.

A durably empty enrolled node can cold-bootstrap only when both peers were explicitly operator-enrolled and usable writer authentication is configured. The peer sends a bounded snapshot of current winners at its exact handshake-frozen frontier, including original schema-v4 writer signatures and a detached witness signature over the complete snapshot root. The receiver requires exact corpus, epoch, frontier, commitment, key, payload-hash, aggregate-byte, and resident-state bounds before staging anything. A durable journal promotes the snapshot idempotently across restart; the journal signature is reverified before recovery. The installed commitments preserve causal continuation, so later traffic still requires the next contiguous writer counter and valid dependencies. Same-session TOFU, non-empty targets, quarantined source state, malformed signatures, and oversized snapshots fail closed.

Local document deletion first writes a node/corpus-scoped durable outbox intent. If the daemon stops after local removal but before tombstone publication, restart probes the recorded metadata/content absence condition, prepares one exact signed tombstone envelope, and retries it idempotently. The outbox entry is removed only after the tombstone index and full-history commitment are durable; the tombstone itself remains retained until the configured replica acknowledgement policy permits collection.

## Verify corpus access

Add a unique file on each node, then verify it from the other:

```bash
yams add ./unique-note.txt
yams search "unique phrase"
yams grep -F "unique phrase" --no-live
yams list --format json
yams cat <sha256>
yams get <sha256> --extract -o ./retrieved.txt
```

Test recovery by stopping one daemon, adding a file on the running node, restarting the stopped daemon, and checking all five retrieval surfaces again. `list` should contain one row per hash on both nodes.

## Optional shared-store S3/R2 transport

`shared-store` remains available for controlled deployments that prefer a durable S3-compatible exchange namespace. It is not direct P2P. Backend ACLs are the default trust boundary, so use a dedicated bucket/prefix and credentials with only required object permissions.

```toml
[memory_sync]
enabled = true
transport = "shared-store"
backend = "s3"
path = "s3://my-memory-bucket/yams-memory"
node_id = "123e4567-e89b-42d3-a456-426614174000"
corpus_id = "team-memory"
corpus_epoch = 1
mode = "persistent"

[storage.s3]
url = "s3://my-memory-bucket/yams-memory"
endpoint = "<account-id>.r2.cloudflarestorage.com"
region = "auto"
use_path_style = false
fallback_policy = "strict"
```

Use a different `node_id` on each daemon. Keep `path`, corpus ID, and epoch identical. Supply S3 credentials outside git:

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
# Required only for temporary credentials:
export AWS_SESSION_TOKEN="..."
```

For AWS, set the regional endpoint and region instead. For local MinIO, use an `http://host:port` endpoint and `use_path_style = true`.

## Validate S3 compatibility

Run the local persistent MinIO convergence lane:

```bash
python3 tests/benchmarks/xplan/runner.py run p2p_memory_sync_local --arm s3-persistent
```

For an authorized Cloudflare R2 smoke test, create a unique disposable bucket, obtain R2 S3 access-key credentials, and run the plugin round trip:

```bash
BUCKET="yams-memory-smoke-$(date +%Y%m%d%H%M%S)"
wrangler r2 bucket create "$BUCKET"

export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION="auto"
export S3_TEST_BUCKET="$BUCKET"
export S3_TEST_ENDPOINT="<account-id>.r2.cloudflarestorage.com"
export S3_TEST_USE_PATH_STYLE=0
scripts/dev/run_s3_plugin_smoke.sh build/debug

wrangler r2 bucket delete "$BUCKET"
```

The smoke performs PUT, GET, HEAD, LIST, and DELETE. Do not put credentials or generated test configuration in git.

## Fuzz direct protocol controls

The direct-P2P fuzz targets exercise the production protocol-v4 handshake/state/history control
parsers and bounded delta/bootstrap control parsers. Their deterministic seed corpus retains a protocol-v3 hello specifically
to verify fail-closed exact-version rejection after the v4 upgrade:

```bash
tools/fuzzing/generate_corpus.sh
tools/fuzzing/fuzz.sh build
AFL_FUZZ_SECONDS=60 tools/fuzzing/fuzz.sh fuzz p2p_protocol
AFL_FUZZ_SECONDS=60 tools/fuzzing/fuzz.sh fuzz p2p_delta
```

These parser harnesses do not replace deterministic TLS/session, payload-accounting, authenticated
application, or restart tests. Bounded local fuzz runs are regression evidence, not
release-readiness evidence. Promotion still requires exact protocol-v4 binaries on every endpoint; at least three isolated cold, warm,
reconnect, and restart repeats; exact pre/post corpus equality; and captured latency, CPU, RSS,
logical/wire-byte, and disk-I/O measurements. Existing protocol-v3 or host-contended profiles must
not be reused for that gate.

## Troubleshooting

```bash
yams daemon status -d
yams p2p status --json
yams p2p peers --json
yams daemon log
```

Common causes of rejection: corpus or epoch mismatch, duplicate node IDs, an unexpected SPKI pin, unreachable listen address, or S3 credentials scoped to the wrong bucket.
