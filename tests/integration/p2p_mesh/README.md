# p2p mesh harness

Runs an N-node YAMS direct-transport memory-sync mesh in Docker so convergence and
failure-recovery bugs can be reproduced end to end and fixes verified against the same
scenario. It is local/manual tooling: nothing here runs in GitHub or Forgejo CI.

Each node is a container with its own volume (`/node`: data dir, `HOME`, XDG dirs, socket,
writer keys, logs). Nothing is mounted from the host; `down` deletes the volumes. Trust is set
up the way an operator does it: per-node Ed25519 writer key and manifest listing every writer
(`writer_auth_required = true`), `corpus_scope = "shared"`, `allow_first_contact = false`, then
`yams p2p enroll` of every peer's node id and SPKI pin on every node and `yams p2p connect
"<host>:9721?pin=...&remember=true"` between every pair. Status then reports
`trust=mutual-tls-operator-pinned`. Each node starts from the config `yams init --auto` writes
(simeon embeddings, vector DB, topology), so seeded documents produce vector and topology
records.

## Requirements

- Docker with the compose plugin, Python 3 (stdlib only), `strip`/`ldd` on the host.
- Linux `yams-cli` and `yams-daemon` from a build dir (`build/release`), or a released `.deb`.
- The base image's glibc/libstdc++ must be at least as new as the build host's. The default
  base is `debian:testing-slim`; use `--base ubuntu:24.04` for a `.deb`.
- If `docker pull` fails with a credential-helper (`gpg`/`pass`) error over ssh, point
  `DOCKER_CONFIG` at an empty config and put a stub `docker-credential-pass` that prints
  `credentials not found in native keychain` first in `PATH`.

State and artifacts live under `build/p2p_mesh/` (override with `--work DIR` or `MESH_WORK`).
Every `up`/`run` uses its own compose project (`yamsmesh-<random>`, or `--project NAME`), so
containers, network, volumes and image tag never collide with another mesh on the same host;
give concurrent meshes different `--work` directories.

## Commands

```bash
M=tests/integration/p2p_mesh/mesh.py

python3 $M up --nodes 3 --build-dir build/release     # or: --deb ./yams_0.20.3_amd64.deb
python3 $M seed --docs-per-node 40 --topics 8 --settle 30
python3 $M converge --timeout 300                     # exit 0 converged, 2 not converged
python3 $M status                                     # artifacts/<stamp>/: status, peers, logs
python3 $M chaos restart n2                           # also kill|start|pause|unpause
python3 $M chaos partition n3; python3 $M chaos heal n3
python3 $M chaos netem n1 --delay-ms 200 --loss 5     # needs NET_ADMIN (granted); netem-clear
python3 $M down                                       # containers, network, volumes, image (--keep-image)
```

`run` chains up, seed, converge, status, a `REPORT.md` and `down`:

```bash
python3 $M run --nodes 5 --build-dir build/release --docs-per-node 16 --timeout 600 --label exp-5node
```

`converge` polls `yams list` on every node until every seeded hash is present, then proves it
with `yams get --hash` on every node. It records records, peers and failed cycles per node, and,
on builds whose `yams p2p status --json` has an `apply` block, the deferred and failure counters.
After convergence it requires `apply.deferred.total` to drain to 0 (exit 3 otherwise) and
`quarantined_writers` to be 0 on every node (exit 4 otherwise): an honest mesh never durably
quarantines a writer.
`status` also counts the memory_sync apply-failure log lines per node.

## Scenarios

- Baseline: `seed --docs-per-node 8 --topics 4`. Small corpora converge even on builds with the
  inbound-apply deadlock, because the vector prerequisites usually arrive in time.
- Convergence deadlock (production 0.20.3 symptom): three or more nodes each add a few dozen
  topic-sharing documents with embeddings at once (`seed --docs-per-node 40 --topics 8`).
  Peers receive vector records whose document blob is not yet local, the vector apply stage
  fails, and a build that skips its own publish after an apply failure stops publishing.
  Every node plateaus at the same `records=` while `failed_cycles=0` and `peers=N-1`, and
  the log fills with `memory_sync vector apply failed: replicated embedding content
  prerequisite is missing`.
- Writer quarantine under load: with 3+ concurrent writers, sessions can also fail with
  `local writer-window resolver violated negotiated bounds` and a node can durably quarantine a
  peer writer (`quarantined=` in status), after which it never receives that writer's documents.
  This is separate from the apply/publish deadlock; `converge` will report it as non-convergence.
  Smaller corpora (`--docs-per-node 24`) avoid it more often.
- Recovery: after `converge` succeeds, `chaos restart nK`, `chaos partition nK`, add documents on
  the others, `chaos heal nK`, `seed` again and `converge` again.
- Released build: `up --deb yams.deb --base ubuntu:24.04` tests the exact packaged version.
