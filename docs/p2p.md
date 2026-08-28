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
```

Requirements:

- Give every daemon a different stable UUID in `node_id`.
- Use the same `corpus_id` and positive `corpus_epoch` on intended peers.
- Keep the node ID and data directory across restarts.
- Expose the listen port only on networks where the peer should reach it. YAMS encrypts and authenticates the session; the network itself need not be trusted.
- The daemon creates an owner-only persistent Ed25519 identity under its data directory unless `identity_key` points to an existing key.

Generate a node ID when needed:

```bash
uuidgen | tr '[:upper:]' '[:lower:]'
```

Restart both daemons after changing configuration, then confirm the compact status:

```bash
yams daemon status
```

Use `yams daemon status -d` for corpus, identity, peer, counter, and failure details.

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

Setting `allow_first_contact = true` explicitly restores legacy TOFU enrollment and emits a startup warning. It permits unsolicited peers to request enrollment and is not recommended for normal operation.

Disconnect and disable automatic reconnect:

```bash
yams p2p disconnect <peer-node-id>
```

Direct protocol v2 is full mesh: with three nodes, connect every pair. A node sends its own operations; it does not relay a third node's history. The authenticated handshake exchanges bounded per-writer full-history commitments and proves the receiver's exact nonzero prefix before application deltas move. Incoming batches remain staged until their canonical envelope chain matches the handshake-frozen frontier. A mismatch durably quarantines the authenticated writer, removes its visible winners through adapter invalidations, and stops the session; operators must investigate or re-enroll a replacement corpus epoch rather than clearing protocol history manually.

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

## Troubleshooting

```bash
yams daemon status -d
yams p2p status --json
yams p2p peers --json
yams daemon log
```

Common causes of rejection: corpus or epoch mismatch, duplicate node IDs, an unexpected SPKI pin, unreachable listen address, or S3 credentials scoped to the wrong bucket.
