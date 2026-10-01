#!/usr/bin/env python3
"""N-node YAMS direct-p2p memory-sync mesh in Docker (local/manual use only, not CI).

Commands: up, seed, converge, status, chaos, run, down.  See README.md.
Python stdlib only; shells out to the `docker` CLI (compose v2 plugin).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
DEFAULT_WORK = REPO / "build" / "p2p_mesh"
PROJECT = "yamsmesh"
CORPUS_ID = "mesh-memory"
LISTEN_PORT = 9721

FAILURE_PATTERNS = [
    "memory_sync content apply failed",
    "memory_sync metadata apply failed",
    "memory_sync vector apply failed",
    "memory_sync topology apply failed",
    "replicated embedding content prerequisite is missing",
    "replicated metadata content prerequisite is missing",
    "topology edge references a missing replicated node",
]

TOPICS = [
    ("compaction", "log-structured merge tree compaction schedules write amplification and sstable levels"),
    ("tls", "mutual tls certificate pinning handshake authenticates peers without a certificate authority"),
    ("vectors", "approximate nearest neighbor search over dense embeddings using hierarchical small world graphs"),
    ("backup", "incremental backup snapshots deduplicate content addressed chunks across retention windows"),
    ("tracing", "distributed tracing spans propagate context across services to explain tail latency"),
    ("scheduler", "work stealing scheduler balances coroutine tasks across worker threads under contention"),
    ("crdt", "conflict free replicated data types merge concurrent edits with commutative operations"),
    ("parsing", "incremental parsers reuse syntax trees so editors can highlight code while typing"),
]


def log(msg: str) -> None:
    print(f"[mesh] {msg}", flush=True)


def die(msg: str) -> "None":
    print(f"mesh: error: {msg}", file=sys.stderr)
    sys.exit(1)


def run(cmd, *, check=True, capture=True, timeout=None, input_text=None, env=None):
    proc = subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=capture,
        timeout=timeout,
        input=input_text,
        env=env,
    )
    if check and proc.returncode != 0:
        out = (proc.stdout or "") + (proc.stderr or "")
        die(f"command failed ({proc.returncode}): {' '.join(map(str, cmd))}\n{out.strip()}")
    return proc


class Mesh:
    def __init__(self, work: Path, project: str = PROJECT):
        self.work = work
        self.project = project
        self.state_path = work / "state.json"
        self.compose_path = work / "docker-compose.yml"
        self.state: dict = {}
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())
            self.project = self.state.get("project", project)

    # -- state ---------------------------------------------------------
    def save(self) -> None:
        self.work.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self.state, indent=2))

    def need_state(self) -> None:
        if not self.state.get("nodes"):
            die(f"no mesh state in {self.work}; run `mesh.py up` first")

    @property
    def nodes(self) -> list[dict]:
        return self.state["nodes"]

    def ctr(self, node: dict) -> str:
        return f"{self.project}-{node['name']}"

    # -- docker helpers -------------------------------------------------
    def compose(self, *args, **kw):
        return run(
            ["docker", "compose", "-p", self.project, "-f", str(self.compose_path), *args], **kw
        )

    def exec(self, node: dict, *argv, user=None, check=True, timeout=120, input_text=None):
        cmd = ["docker", "exec"]
        if input_text is not None:
            cmd.append("-i")
        if user:
            cmd += ["-u", user]
        cmd += [self.ctr(node), *argv]
        return run(cmd, check=check, timeout=timeout, input_text=input_text)

    def put(self, node: dict, path: str, content: str, mode: str = "644") -> None:
        self.exec(
            node,
            "sh",
            "-c",
            f"umask 077 && cat > '{path}' && chmod {mode} '{path}'",
            input_text=content,
        )

    def yams(self, node: dict, *args, check=False, timeout=60):
        return self.exec(node, "yams", *args, check=check, timeout=timeout)

    def p2p_json(self, node: dict, sub: str, timeout=30):
        proc = self.yams(node, "p2p", sub, "--json", timeout=timeout)
        if proc.returncode != 0:
            return None
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError:
            return None

    # -- up -------------------------------------------------------------
    def stage_payload(self, build_dir: Path | None, deb: Path | None) -> Path:
        payload = self.work / "context" / "payload"
        if payload.exists():
            shutil.rmtree(payload)
        payload.mkdir(parents=True)
        if deb:
            if not deb.is_file():
                die(f"--deb {deb} not found")
            shutil.copy2(deb, payload / deb.name)
            return payload.parent
        assert build_dir is not None
        found = {}
        for name, aliases in (("yams-cli", ("yams-cli", "yams")), ("yams-daemon", ("yams-daemon",))):
            for cand in build_dir.rglob("*"):
                if (
                    cand.name in aliases
                    and cand.is_file()
                    and os.access(cand, os.X_OK)
                    and "subprojects" not in cand.parts
                    and not cand.is_symlink()
                ):
                    found[name] = cand
                    break
            if name not in found:
                die(f"no {name} binary under {build_dir}")
        bindir = payload / "bin"
        libdir = payload / "lib"
        bindir.mkdir()
        libdir.mkdir()
        for name, src in found.items():
            shutil.copy2(src, bindir / name)
            if shutil.which("strip"):
                run(["strip", "--strip-unneeded", str(bindir / name)], check=False)
        (bindir / "yams").symlink_to("yams-cli")
        # Bundle shared libraries that resolve into the build tree or the conan cache.
        libs: set[Path] = set()
        for src in found.values():
            ldd = run(["ldd", str(src)], check=False).stdout
            for m in re.finditer(r"=>\s+(/\S+)", ldd):
                p = Path(m.group(1))
                if str(p).startswith(str(build_dir.resolve())) or ".conan" in str(p):
                    libs.add(p)
        for lib in sorted(libs):
            shutil.copy2(lib, libdir / lib.name)
        log(f"payload: {', '.join(p.name for p in found.values())} + {len(libs)} bundled libs")
        return payload.parent

    def write_compose(self, nodes: list[dict], image: str, log_level: str) -> None:
        env = {
            "HOME": "/node/home",
            "XDG_CONFIG_HOME": "/node/xdg/config",
            "XDG_DATA_HOME": "/node/xdg/data",
            "XDG_STATE_HOME": "/node/xdg/state",
            "XDG_RUNTIME_DIR": "/node/run",
            "YAMS_DATA_DIR": "/node/data",
            "YAMS_DAEMON_SOCKET": "/node/run/yams.sock",
            "YAMS_NON_INTERACTIVE": "1",
            "LD_LIBRARY_PATH": "/opt/yams/lib",
            "MESH_LOG_LEVEL": log_level,
        }
        lines = ["name: " + self.project, "services:"]
        for n in nodes:
            lines += [
                f"  {n['name']}:",
                f"    image: {image}",
                f"    container_name: {self.ctr(n)}",
                f"    hostname: {n['name']}",
                "    init: true",
                "    cap_add: [NET_ADMIN]",
                "    stop_grace_period: 20s",
                "    networks: [mesh]",
                "    environment:",
                *[f"      {k}: \"{v}\"" for k, v in env.items()],
                "    volumes:",
                f"      - {n['name']}-data:/node",
            ]
        lines += ["networks:", "  mesh:", "    driver: bridge", "volumes:"]
        lines += [f"  {n['name']}-data:" for n in nodes]
        self.compose_path.write_text("\n".join(lines) + "\n")

    def cmd_up(self, a) -> None:
        if self.state.get("nodes") and not a.force:
            die("mesh state exists; run `mesh.py down` first (or pass --force)")
        if bool(a.build_dir) == bool(a.deb):
            die("pass exactly one of --build-dir or --deb")
        if a.nodes < 2:
            die("--nodes must be >= 2")
        self.work.mkdir(parents=True, exist_ok=True)
        ctx = self.stage_payload(Path(a.build_dir).resolve() if a.build_dir else None,
                                 Path(a.deb).resolve() if a.deb else None)
        shutil.copy2(HERE / "Dockerfile", ctx / "Dockerfile")
        shutil.copy2(HERE / "node.sh", ctx / "node.sh")
        image = f"{self.project}:{int(time.time())}"
        base = a.base or ("ubuntu:24.04" if a.deb else "debian:testing-slim")
        log(f"building image {image} (base {base})")
        run(["docker", "build", "--build-arg", f"BASE={base}", "-t", image, str(ctx)],
            capture=False)

        nodes = [
            {"name": f"n{i + 1}", "node_id": str(uuid.uuid4()), "key_id": f"n{i + 1}-v1"}
            for i in range(a.nodes)
        ]
        self.state = {
            "project": self.project,
            "image": image,
            "corpus_id": CORPUS_ID,
            "corpus_epoch": 1,
            "nodes": nodes,
            "docs": [],
            "extra_config": a.config or [],
        }
        self.write_compose(nodes, image, a.log_level)
        self.save()
        self.compose("up", "-d", "--no-build", capture=False)

        log("waiting for containers")
        for n in nodes:
            self.wait_for(lambda n=n: self.exec(n, "test", "-d", "/node/home", check=False).returncode == 0,
                          30, f"{n['name']} container")
        for n in nodes:
            self.bootstrap_node(n)
        self.finish_bootstrap()
        for n in nodes:
            self.exec(n, "touch", "/node/ready")
        for n in nodes:
            self.wait_for(lambda n=n: self.p2p_json(n, "identity") is not None, 90,
                          f"{n['name']} daemon")
            ident = self.p2p_json(n, "identity")
            n["spki_pin"] = ident["spki_pin"]
            if ident["node_id"] != n["node_id"]:
                die(f"{n['name']} reports node_id {ident['node_id']} != configured {n['node_id']}")
        self.save()
        self.wire_mesh(a.topology, a.connect)
        log(f"mesh of {a.nodes} nodes is up")
        self.print_status_line()

    def bootstrap_node(self, n: dict) -> None:
        """Keys + config for one node: writer key, manifest listing every writer, config.toml."""
        self.exec(n, "sh", "-c", "mkdir -p /node/keys /node/xdg/config/yams && chmod 700 /node/keys")
        init = self.yams(n, "init", "--auto", "--no-keygen", timeout=120)
        if init.returncode != 0:
            die(f"yams init failed on {n['name']}: {init.stdout}{init.stderr}")
        self.exec(n, "openssl", "genpkey", "-algorithm", "ED25519", "-out", "/node/keys/writer-private.pem")
        self.exec(n, "chmod", "600", "/node/keys/writer-private.pem")
        pub = self.exec(n, "openssl", "pkey", "-in", "/node/keys/writer-private.pem", "-pubout").stdout
        n["writer_public"] = pub
        self.exec(n, "chmod", "go-w", "/node/keys")
        self.save()

    def finish_bootstrap(self) -> None:
        for n in self.nodes:
            trusted = []
            for peer in self.nodes:
                path = f"/node/keys/{peer['name']}-public.pem"
                self.put(n, path, peer["writer_public"], "644")
                trusted.append(
                    {"writer_id": peer["node_id"], "key_id": peer["key_id"], "public_key_path": path}
                )
            manifest = {
                "schema_version": 1,
                "corpus_id": self.state["corpus_id"],
                "corpus_epoch": self.state["corpus_epoch"],
                "local_key": {
                    "writer_id": n["node_id"],
                    "key_id": n["key_id"],
                    "private_key_path": "/node/keys/writer-private.pem",
                },
                "trusted_writers": trusted,
            }
            self.put(n, "/node/keys/writers.json", json.dumps(manifest, indent=2), "644")
            toml = [
                "[memory_sync]",
                "enabled = true",
                'transport = "direct"',
                f'listen = "0.0.0.0:{LISTEN_PORT}"',
                f'node_id = "{n["node_id"]}"',
                f'corpus_id = "{self.state["corpus_id"]}"',
                f'corpus_epoch = {self.state["corpus_epoch"]}',
                'corpus_scope = "shared"',
                'mode = "persistent"',
                "allow_first_contact = false",
                "writer_auth_required = true",
                'writer_auth_manifest = "/node/keys/writers.json"',
            ]
            toml += [f"{kv}" for kv in self.state.get("extra_config", [])]
            # Start from the same config `yams init` gives a fresh install (simeon embeddings,
            # vector DB, topology defaults), then append the sync section the way an operator does.
            self.put(n, "/node/memory-sync.toml", "\n".join(toml) + "\n")
            self.exec(n, "sh", "-c",
                      "printf '\\n' >> /node/xdg/config/yams/config.toml && "
                      "cat /node/memory-sync.toml >> /node/xdg/config/yams/config.toml")

    def wire_mesh(self, topology: str, connect: str) -> None:
        nodes = self.nodes
        for n in nodes:
            for peer in nodes:
                if peer is n:
                    continue
                proc = self.yams(n, "p2p", "enroll", peer["node_id"], peer["spki_pin"])
                if proc.returncode != 0:
                    die(f"enroll {peer['name']} on {n['name']} failed: {proc.stdout}{proc.stderr}")
        if topology == "none":
            return
        for i, n in enumerate(nodes):
            for j, peer in enumerate(nodes):
                if i == j or (connect == "once" and j < i):
                    continue
                conn = f"{peer['name']}:{LISTEN_PORT}?pin={peer['spki_pin']}&remember=true"
                proc = self.yams(n, "p2p", "connect", conn, timeout=90)
                tag = "ok" if proc.returncode == 0 else "FAILED"
                log(f"connect {n['name']} -> {peer['name']}: {tag}")
                if proc.returncode != 0:
                    log((proc.stdout + proc.stderr).strip())

    def wait_for(self, fn, timeout, what):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if fn():
                    return
            except subprocess.TimeoutExpired:
                pass
            time.sleep(1)
        die(f"timed out waiting for {what}")

    # -- status ---------------------------------------------------------
    def node_status(self, n: dict) -> dict:
        data = self.p2p_json(n, "status") or {}
        return data

    def print_status_line(self) -> None:
        for n in self.nodes:
            proc = self.yams(n, "p2p", "status")
            line = (proc.stdout or proc.stderr).strip().splitlines()
            print(f"  {n['name']}: {line[0] if line else '<no output>'}")

    def count_failures(self, n: dict) -> dict[str, int]:
        logs = self.exec(n, "sh", "-c", "cat /node/log/daemon.log 2>/dev/null", check=False).stdout
        return {p: logs.count(p) for p in FAILURE_PATTERNS if logs.count(p)}

    def cmd_status(self, a) -> None:
        self.need_state()
        out = Path(a.out) if a.out else self.work / "artifacts" / time.strftime("%Y%m%d-%H%M%S")
        self.collect(out)
        log(f"artifacts: {out}")

    def collect(self, out: Path) -> dict:
        out.mkdir(parents=True, exist_ok=True)
        summary = {}
        for n in self.nodes:
            nd = out / n["name"]
            nd.mkdir(exist_ok=True)
            entry = {"status": self.node_status(n), "peers": self.p2p_json(n, "peers")}
            (nd / "p2p-status.json").write_text(json.dumps(entry["status"], indent=2))
            (nd / "p2p-peers.json").write_text(json.dumps(entry["peers"], indent=2))
            text = self.yams(n, "p2p", "status").stdout
            (nd / "p2p-status.txt").write_text(text)
            logs = self.exec(n, "sh", "-c", "cat /node/log/daemon.log 2>/dev/null", check=False).stdout
            (nd / "daemon.log").write_text(logs)
            (nd / "container.log").write_text(
                run(["docker", "logs", self.ctr(n)], check=False).stdout or ""
            )
            entry["failures"] = {p: logs.count(p) for p in FAILURE_PATTERNS if logs.count(p)}
            entry["status_line"] = text.strip().splitlines()[0] if text.strip() else ""
            entry["apply"] = (entry["status"] or {}).get("apply")
            summary[n["name"]] = entry
        (out / "summary.json").write_text(json.dumps(summary, indent=2))
        for name, e in summary.items():
            print(f"  {name}: {e['status_line']}")
            for p, c in e["failures"].items():
                print(f"      {c:5d}  {p}")
            if e.get("apply"):
                ap = e["apply"]
                print(f"      apply: cycles={ap.get('cycles')} failed={ap.get('failed_cycles')} "
                      f"deferred={ap.get('deferred')} publish_skipped={ap.get('publish_skipped_cycles')}")
        return summary

    # -- seed -----------------------------------------------------------
    def cmd_seed(self, a) -> None:
        self.need_state()
        topics = TOPICS[: a.topics]
        added = []
        rounds = max(1, a.docs_per_node // len(topics))
        for r in range(rounds):
            for n in self.nodes:
                for t, (topic, blurb) in enumerate(topics):
                    # Same topic everywhere (so semantic-neighbour edges cross nodes), but every
                    # document body is unique to its author node and round.
                    body = (
                        f"Note {r} on {topic} by {n['name']}: {blurb}. "
                        f"This variant was written on {n['name']} (writer {n['node_id']}), "
                        f"round {r}, topic index {t}. "
                        + " ".join(f"{topic}-{n['name']}-{r}-{k}" for k in range(a.words))
                        + "\n"
                    )
                    fname = f"/node/seed/{topic}-{n['name']}-{r}.md"
                    self.exec(n, "mkdir", "-p", "/node/seed")
                    self.put(n, fname, body)
                    proc = self.yams(n, "add", fname, "--tags", f"mesh,{topic}", timeout=120)
                    if proc.returncode != 0:
                        die(f"add on {n['name']} failed: {proc.stdout}{proc.stderr}")
                    h = hashlib.sha256(body.encode()).hexdigest()
                    added.append({"node": n["name"], "hash": h, "topic": topic, "round": r})
                if a.stagger:
                    time.sleep(a.stagger)
        self.state["docs"] = self.state.get("docs", []) + added
        self.save()
        log(f"seeded {len(added)} documents ({rounds} rounds x {len(self.nodes)} nodes x {len(topics)} topics)")
        if a.settle:
            log(f"waiting {a.settle}s for local post-ingest (embeddings/topology)")
            time.sleep(a.settle)

    # -- converge -------------------------------------------------------
    def listed_hashes(self, n: dict) -> set[str] | None:
        """All document hashes the node lists (one CLI call)."""
        proc = self.yams(n, "list", "--format", "json", "--limit", "1000000", timeout=120)
        if proc.returncode != 0:
            return None
        try:
            docs = json.loads(proc.stdout).get("documents", [])
        except json.JSONDecodeError:
            return None
        return {d["hash"] for d in docs if d.get("hash")}

    def readable(self, n: dict, h: str) -> bool:
        proc = self.yams(n, "get", "--hash", h, "--raw", "--max-bytes", "1", timeout=30)
        return proc.returncode == 0

    def apply_summary(self, n: dict) -> dict:
        """Compact view of status JSON (the `apply` block exists only on builds that report it)."""
        st = self.node_status(n)
        ap = st.get("apply") or {}
        return {
            "records": st.get("records"),
            "peers": st.get("peer_count"),
            "failed_cycles": st.get("failed_cycles"),
            "quarantined": st.get("quarantined"),
            "apply": ap or None,
            "deferred": (ap.get("deferred") or {}).get("total") if ap else None,
        }

    def cmd_converge(self, a) -> int:
        self.need_state()
        docs = self.state.get("docs", [])
        if not docs:
            die("no seeded documents; run `mesh.py seed` first")
        want = {d["hash"] for d in docs}
        start = time.time()
        deadline = start + a.timeout
        trace = []
        it = 0
        converged = False
        while True:
            it += 1
            missing = {}
            summaries = {}
            for n in self.nodes:
                have = self.listed_hashes(n)
                missing[n["name"]] = sorted(want - have) if have is not None else sorted(want)
                summaries[n["name"]] = self.apply_summary(n)
            total = len(want)
            elapsed = round(time.time() - start, 1)
            line = " ".join(
                f"{k}:{total - len(v)}/{total}(rec={summaries[k]['records']}"
                + (f",def={summaries[k]['deferred']}" if summaries[k]["deferred"] is not None else "")
                + ")"
                for k, v in missing.items()
            )
            print(f"[converge #{it} t={elapsed}s] {line}", flush=True)
            trace.append({"t": elapsed, "have": {k: total - len(v) for k, v in missing.items()},
                          "nodes": summaries})
            if all(not v for v in missing.values()):
                # Final proof with the real read path: `yams get` by hash on every node.
                bad = {n["name"]: [h for h in sorted(want) if not self.readable(n, h)]
                       for n in self.nodes} if not a.fast else {}
                if all(not v for v in bad.values()):
                    converged = True
                    break
            if time.time() >= deadline:
                break
            time.sleep(a.interval)
        result = {
            "converged": converged,
            "seconds": round(time.time() - start, 1),
            "documents": len(want),
            "readable": {k: len(want) - len(v) for k, v in missing.items()},
            "final": summaries,
            "trace": trace,
        }
        if converged and any(s["deferred"] is not None for s in summaries.values()):
            # Deferred records must drain once every document is readable; give the apply
            # loop a few cycles to retire the tail.
            drain_deadline = time.time() + a.drain_timeout
            while True:
                summaries = {n["name"]: self.apply_summary(n) for n in self.nodes}
                if all((s["deferred"] or 0) == 0 for s in summaries.values()):
                    result["deferred_drained"] = True
                    break
                if time.time() >= drain_deadline:
                    result["deferred_drained"] = False
                    break
                time.sleep(3)
            result["final"] = summaries
        self.state["last_converge"] = result
        self.save()
        self.work.mkdir(parents=True, exist_ok=True)
        (self.work / "converge.json").write_text(json.dumps(result, indent=2))
        if converged:
            log(f"CONVERGED in {result['seconds']}s: every document hash is readable on every node")
            if result.get("deferred_drained") is False:
                log("WARNING: deferred records did not drain to 0 after convergence")
                return 3
            return 0
        log(f"NOT CONVERGED after {a.timeout}s: readable {result['readable']} of {len(want)}")
        return 2

    # -- chaos ----------------------------------------------------------
    def node_by_name(self, name: str) -> dict:
        for n in self.nodes:
            if n["name"] == name:
                return n
        die(f"unknown node {name}")

    def cmd_chaos(self, a) -> None:
        self.need_state()
        n = self.node_by_name(a.node)
        net = f"{self.project}_mesh"
        act = a.action
        if act == "restart":
            run(["docker", "restart", self.ctr(n)], capture=False)
        elif act == "kill":
            run(["docker", "kill", self.ctr(n)], capture=False)
        elif act == "start":
            run(["docker", "start", self.ctr(n)], capture=False)
        elif act == "pause":
            run(["docker", "pause", self.ctr(n)], capture=False)
        elif act == "unpause":
            run(["docker", "unpause", self.ctr(n)], capture=False)
        elif act == "partition":
            run(["docker", "network", "disconnect", net, self.ctr(n)], capture=False)
        elif act == "heal":
            run(["docker", "network", "connect", "--alias", n["name"], net, self.ctr(n)], capture=False)
        elif act == "netem":
            args = ["tc", "qdisc", "replace", "dev", "eth0", "root", "netem"]
            if a.delay_ms:
                args += ["delay", f"{a.delay_ms}ms"]
            if a.loss:
                args += ["loss", f"{a.loss}%"]
            proc = self.exec(n, *args, user="root", check=False)
            if proc.returncode != 0:
                die(f"tc failed (needs NET_ADMIN and iproute2): {proc.stderr.strip()}")
        elif act == "netem-clear":
            self.exec(n, "tc", "qdisc", "del", "dev", "eth0", "root", user="root", check=False)
        log(f"{act} {n['name']}: done")

    # -- run (scenario) -------------------------------------------------
    def cmd_run(self, a) -> int:
        """up + seed + converge + status + REPORT.md, then down unless --keep."""
        label = a.label or f"{a.nodes}node"
        out = Path(a.out) if a.out else self.work / "artifacts" / label
        out.mkdir(parents=True, exist_ok=True)
        self.cmd_up(a)
        self.cmd_seed(a)
        rc = self.cmd_converge(a)
        summary = self.collect(out)
        conv = json.loads((self.work / "converge.json").read_text())
        (out / "converge.json").write_text(json.dumps(conv, indent=2))
        lines = [
            f"# Scenario {label}",
            "",
            f"- build: {a.build_dir or a.deb}",
            f"- nodes: {a.nodes}, documents seeded: {conv['documents']}",
            f"- converged: {conv['converged']} in {conv['seconds']}s (timeout {a.timeout}s)",
            f"- deferred drained: {conv.get('deferred_drained', 'n/a (build has no apply status)')}",
            "",
            "| node | readable | records | failed_cycles | peers | quarantined | deferred | vector fails | topology fails |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for name, e in summary.items():
            fin = conv["final"][name]
            lines.append(
                f"| {name} | {conv['readable'][name]}/{conv['documents']} | {fin['records']} | "
                f"{fin['failed_cycles']} | {fin['peers']} | {fin['quarantined']} | "
                f"{fin['deferred'] if fin['deferred'] is not None else 'n/a'} | "
                f"{e['failures'].get('memory_sync vector apply failed', 0)} | "
                f"{e['failures'].get('memory_sync topology apply failed', 0)} |"
            )
        lines += ["", "Status lines:", ""]
        lines += [f"    {n}: {e['status_line']}" for n, e in summary.items()]
        (out / "REPORT.md").write_text("\n".join(lines) + "\n")
        log(f"report: {out / 'REPORT.md'}")
        if not a.keep:
            self.cmd_down(argparse.Namespace(keep_image=False, purge=False))
        return rc

    # -- down -----------------------------------------------------------
    def cmd_down(self, a) -> None:
        if self.compose_path.exists():
            self.compose("down", "-v", "--remove-orphans", check=False, capture=False)
        image = self.state.get("image")
        if image and not a.keep_image:
            run(["docker", "rmi", image], check=False)
        if a.purge and self.work.exists():
            shutil.rmtree(self.work / "context", ignore_errors=True)
        self.state = {}
        if self.state_path.exists():
            self.state_path.unlink()
        log("down")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--work", default=os.environ.get("MESH_WORK", str(DEFAULT_WORK)),
                   help="state/artifact directory (default: build/p2p_mesh)")
    sub = p.add_subparsers(dest="cmd", required=True)

    up = sub.add_parser("up", help="build image, start N nodes, enroll and connect a full mesh")
    up.add_argument("--nodes", type=int, default=3)
    up.add_argument("--build-dir", help="host build dir containing yams-cli and yams-daemon")
    up.add_argument("--deb", help="released .deb to install instead of a build dir")
    up.add_argument("--base", help="base image (default debian:testing-slim; ubuntu:24.04 for --deb)")
    up.add_argument("--topology", choices=["full", "none"], default="full",
                    help="none: enroll only, no connect")
    up.add_argument("--connect", choices=["once", "both"], default="both",
                    help="connect each pair from one side or from both sides")
    up.add_argument("--config", action="append",
                    help="extra raw line appended to [memory_sync] (repeatable)")
    up.add_argument("--log-level", default="info")
    up.add_argument("--force", action="store_true")

    seed = sub.add_parser("seed", help="add documents on every node")
    seed.add_argument("--docs-per-node", type=int, default=16)
    seed.add_argument("--topics", type=int, default=4, help="shared topics (max %d)" % len(TOPICS))
    seed.add_argument("--words", type=int, default=40, help="unique filler tokens per document")
    seed.add_argument("--stagger", type=float, default=0.0, help="seconds between node rounds")
    seed.add_argument("--settle", type=int, default=0, help="seconds to wait after adding")

    cv = sub.add_parser("converge", help="poll until every hash is readable on every node")
    cv.add_argument("--timeout", type=int, default=300)
    cv.add_argument("--interval", type=int, default=10)
    cv.add_argument("--drain-timeout", type=int, default=60,
                    help="after convergence, wait this long for apply.deferred to reach 0")
    cv.add_argument("--fast", action="store_true",
                    help="trust `yams list` instead of verifying every hash with `yams get`")

    st = sub.add_parser("status", help="collect p2p status and daemon logs into an artifacts dir")
    st.add_argument("--out")

    ch = sub.add_parser("chaos", help="restart/kill/pause/partition/netem a node")
    ch.add_argument("action", choices=["restart", "kill", "start", "pause", "unpause", "partition",
                                       "heal", "netem", "netem-clear"])
    ch.add_argument("node")
    ch.add_argument("--delay-ms", type=int, default=0)
    ch.add_argument("--loss", type=float, default=0.0)

    rn = sub.add_parser("run", help="scenario: up + seed + converge + status + report (+ down)")
    rn.add_argument("--nodes", type=int, default=3)
    rn.add_argument("--build-dir")
    rn.add_argument("--deb")
    rn.add_argument("--base")
    rn.add_argument("--topology", choices=["full", "none"], default="full")
    rn.add_argument("--connect", choices=["once", "both"], default="both")
    rn.add_argument("--config", action="append")
    rn.add_argument("--log-level", default="info")
    rn.add_argument("--force", action="store_true")
    rn.add_argument("--docs-per-node", type=int, default=40)
    rn.add_argument("--topics", type=int, default=8)
    rn.add_argument("--words", type=int, default=40)
    rn.add_argument("--stagger", type=float, default=0.0)
    rn.add_argument("--settle", type=int, default=30)
    rn.add_argument("--timeout", type=int, default=300)
    rn.add_argument("--interval", type=int, default=15)
    rn.add_argument("--drain-timeout", type=int, default=60)
    rn.add_argument("--fast", action="store_true")
    rn.add_argument("--label")
    rn.add_argument("--out")
    rn.add_argument("--keep", action="store_true", help="leave the mesh up afterwards")

    dn = sub.add_parser("down", help="remove containers, network, volumes")
    dn.add_argument("--keep-image", action="store_true", help="keep the built image")
    dn.add_argument("--purge", action="store_true", help="also delete the staged build context")
    return p


def main() -> int:
    args = build_parser().parse_args()
    mesh = Mesh(Path(args.work))
    if args.cmd == "up":
        mesh.cmd_up(args)
    elif args.cmd == "seed":
        mesh.cmd_seed(args)
    elif args.cmd == "converge":
        return mesh.cmd_converge(args)
    elif args.cmd == "status":
        mesh.cmd_status(args)
    elif args.cmd == "chaos":
        mesh.cmd_chaos(args)
    elif args.cmd == "run":
        return mesh.cmd_run(args)
    elif args.cmd == "down":
        mesh.cmd_down(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
