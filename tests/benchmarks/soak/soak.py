#!/usr/bin/env python3
"""Single-node soak harness for yams-daemon (Linux).

Starts one daemon with an isolated XDG/data tree, optionally seeds a BEIR-style corpus,
then runs phases such as idle -> load -> idle while sampling process state from /proc:
RSS/PSS (smaps_rollup), per-thread CPU and context switches, wait channels,
FD and thread counts, DB/WAL file sizes, and (less often) `yams status --json`.
Optional gdb stack snapshots map thread ids to code so wakeups can be attributed.

Usage (grim):

    python3 tests/benchmarks/soak/soak.py run \
        --build-dir /data/yams-lt-soak/release \
        --work /data/yams-lt-soak/soak-baseline \
        --corpus ~/.cache/yams/benchmarks/scifact/corpus.jsonl --docs 3000 \
        --phases idle:1800,load:1800,idle:1800 --interval 10 --status-every 60

    python3 tests/benchmarks/soak/soak.py summarize --work /data/yams-lt-soak/soak-baseline

Outputs under --work/out: samples.jsonl (one row per sample, per-thread detail),
status.jsonl, ops.jsonl (load-op latencies), stacks/*.txt, summary.json, summary.md.

Stock library only. Reading /proc does not touch the daemon; `yams status` does
(a client connection ends the daemon's idle state). Status calls and gdb snapshots are
disabled by default; opt-in probes are recorded in metadata.
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import platform
import random
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
PAGE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096


def log(msg: str) -> None:
    print(f"[soak {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def die(msg: str, code: int = 2) -> None:
    print(f"soak: {msg}", file=sys.stderr)
    sys.exit(code)


# ---------------------------------------------------------------- /proc readers
def read_text(p: str) -> str:
    try:
        with open(p, encoding="utf-8") as f:
            return f.read()
    except OSError:
        return ""


def redact_config(text: str) -> str:
    """Redact values of credential-like TOML keys before artifact capture."""
    sensitive = re.compile(r"(?i)(password|passwd|secret|token|api[_-]?key|private[_-]?key)")
    lines = []
    for line in text.splitlines():
        key, sep, _value = line.partition("=")
        if sep and sensitive.search(key.strip()):
            line = f'{key.rstrip()} = "<redacted>"'
        lines.append(line)
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def parse_kv_kb(text: str) -> dict[str, int]:
    """'Key:  123 kB' lines -> {Key: 123}."""
    out = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        v = v.strip().split()
        if v and v[0].lstrip("-").isdigit():
            out[k.strip()] = int(v[0])
    return out


def task_stat(path: str) -> dict | None:
    s = read_text(path)
    if not s:
        return None
    lp = s.rfind(")")
    comm = s[s.find("(") + 1:lp]
    f = s[lp + 2:].split()
    # f[0] = state (field 3); field k -> f[k-3]
    try:
        return {
            "comm": comm,
            "state": f[0],
            "utime": int(f[11]),
            "stime": int(f[12]),
            "num_threads": int(f[16]),
            "starttime": int(f[19]),
        }
    except (IndexError, ValueError):
        return None


def daemon_command(daemon_bin: str, config: str, sock: str, data_dir: str, pid_file: str,
                   log_file: str, log_level: str, no_plugins: bool, prefix: str | None) -> list[str]:
    """Daemon argv. `prefix` (shell-split) runs in front, e.g. `taskset -c 2-5` or
    `systemd-run --user --scope -p CPUQuota=400% taskset -c 2-5`; it must exec the daemon in
    place or write the daemon's pid file, which start_daemon prefers over the launched pid."""
    cmd = [daemon_bin, "--foreground", "--config", config, "--socket", sock, "--data-dir",
           data_dir, "--pid-file", pid_file, "--log-file", log_file, "--log-level", log_level]
    if no_plugins:
        cmd.append("--no-plugins")
    return shlex.split(prefix) + cmd if prefix else cmd


def placement_from(status_text: str, cgroup_text: str, read) -> dict:
    """CPU affinity and cgroup v2 limits of a process, from its /proc status and cgroup text.
    `read(path)` returns file text or '' (cpu.max is 'quota period' or 'max period')."""
    cpus = None
    for line in status_text.splitlines():
        if line.startswith("Cpus_allowed_list:"):
            cpus = line.split(":", 1)[1].strip()
    cgroup = None
    for line in cgroup_text.splitlines():
        if line.startswith("0::"):
            cgroup = line[3:].strip()
    out = {"cpus_allowed_list": cpus, "cgroup": cgroup, "cpu_max": None, "memory_max": None}
    if cgroup:
        out["cpu_max"] = read(f"/sys/fs/cgroup{cgroup}/cpu.max").strip() or None
        out["memory_max"] = read(f"/sys/fs/cgroup{cgroup}/memory.max").strip() or None
    return out


def placement(pid: int) -> dict:
    return placement_from(read_text(f"/proc/{pid}/status"), read_text(f"/proc/{pid}/cgroup"),
                          read_text)


def sample_threads(pid: int) -> dict[str, dict]:
    threads = {}
    for tdir in glob.glob(f"/proc/{pid}/task/*"):
        tid = os.path.basename(tdir)
        st = task_stat(f"{tdir}/stat")
        if not st:
            continue
        status = parse_kv_kb(read_text(f"{tdir}/status"))
        sched = read_text(f"{tdir}/schedstat").split()
        wchan = read_text(f"{tdir}/wchan").strip() or "-"
        threads[tid] = {
            "comm": st["comm"],
            "state": st["state"],
            "utime": st["utime"],
            "stime": st["stime"],
            "starttime": st["starttime"],
            "vcs": status.get("voluntary_ctxt_switches", 0),
            "nvcs": status.get("nonvoluntary_ctxt_switches", 0),
            "slices": int(sched[2]) if len(sched) >= 3 else 0,
            "wchan": wchan,
        }
    return threads


def file_sizes(data_dir: str) -> dict[str, int]:
    out: dict[str, int] = {}
    total = 0
    for root, _dirs, files in os.walk(data_dir):
        for fn in files:
            p = os.path.join(root, fn)
            try:
                sz = os.stat(p).st_size
            except OSError:
                continue
            total += sz
            if fn.endswith((".db", ".db-wal", ".db-shm", ".sqlite", ".sqlite-wal", ".sqlite-shm",
                            ".wal", ".index", ".hnsw", ".bin")) or "vector" in fn or "wal" in root:
                out[os.path.relpath(p, data_dir)] = sz
    out["__total__"] = total
    return out


def sample_process(pid: int, data_dir: str) -> dict | None:
    status = parse_kv_kb(read_text(f"/proc/{pid}/status"))
    if not status:
        return None
    rollup = parse_kv_kb(read_text(f"/proc/{pid}/smaps_rollup"))
    try:
        fds = len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        fds = -1
    pst = task_stat(f"/proc/{pid}/stat") or {}
    return {
        "t": time.time(),
        "mono": time.monotonic(),
        "rss_kb": status.get("VmRSS", 0),
        "hwm_kb": status.get("VmHWM", 0),
        "vsz_kb": status.get("VmSize", 0),
        "swap_kb": status.get("VmSwap", 0),
        "pss_kb": rollup.get("Pss", 0),
        "anon_kb": rollup.get("Anonymous", 0),
        "private_dirty_kb": rollup.get("Private_Dirty", 0),
        "shared_clean_kb": rollup.get("Shared_Clean", 0),
        "threads": status.get("Threads", pst.get("num_threads", 0)),
        "fds": fds,
        "proc_vcs": status.get("voluntary_ctxt_switches", 0),
        "proc_nvcs": status.get("nonvoluntary_ctxt_switches", 0),
        "utime": pst.get("utime", 0),
        "stime": pst.get("stime", 0),
        "process_starttime": pst.get("starttime"),
        "files": file_sizes(data_dir),
        "tasks": sample_threads(pid),
    }


# ---------------------------------------------------------------- gdb stacks
THREAD_HDR = re.compile(r'^Thread \d+ \(Thread 0x[0-9a-f]+ \(LWP (\d+)\)(?: "([^"]*)")?\)')
FRAME = re.compile(r"^#(\d+)\s+(?:0x[0-9a-f]+ in )?(.+?)(?: \(.*)?$")


def gdb_stacks(pid: int, out_path: Path, depth: int = 20, timeout: int = 120) -> dict[str, list[str]]:
    """Attach once, dump all thread backtraces, return {tid: [frames]}."""
    cmd = ["gdb", "-batch", "-q", "-p", str(pid), "-ex", f"thread apply all bt {depth}"]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        out_path.write_text(f"gdb failed: {e}\n", encoding="utf-8")
        return {}
    out_path.write_text(res.stdout + "\n--- stderr ---\n" + res.stderr, encoding="utf-8")
    stacks: dict[str, list[str]] = {}
    cur = None
    for line in res.stdout.splitlines():
        m = THREAD_HDR.match(line)
        if m:
            cur = m.group(1)
            stacks[cur] = []
            continue
        if cur is None:
            continue
        fm = FRAME.match(line.strip())
        if fm:
            frame = fm.group(2).strip()
            stacks[cur].append(frame)
    return stacks


def label_from_frames(frames: list[str]) -> str:
    """First application frame (yams::...) else the first non-libc frame."""
    skip = ("__GI_", "syscall", "epoll_wait", "futex", "pthread_cond", "nanosleep", "clock_nanosleep",
            "std::", "boost::asio::detail", "__pthread", "start_thread", "clone", "?? ")
    for f in frames:
        m = re.search(r"yams::[A-Za-z0-9_:]+", f)
        if m:
            return m.group(0)
    for f in frames:
        if not any(f.startswith(s) or s in f for s in skip):
            return f.split("(")[0][:160]
    return frames[0][:160] if frames else "?"


# ---------------------------------------------------------------- stats helpers
def ols(xs: list[float], ys: list[float]) -> tuple[float, float, float, int]:
    """Slope, intercept, 95% CI half-width of slope (OLS, iid residuals), n."""
    n = len(xs)
    if n < 3:
        return 0.0, ys[0] if ys else 0.0, float("nan"), n
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return 0.0, my, float("nan"), n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    slope = sxy / sxx
    icpt = my - slope * mx
    resid = [y - (icpt + slope * x) for x, y in zip(xs, ys)]
    s2 = sum(r * r for r in resid) / (n - 2)
    se = math.sqrt(s2 / sxx)
    return slope, icpt, 1.96 * se, n


def block_slopes(xs: list[float], ys: list[float], blocks: int = 4) -> list[float]:
    """Slopes over consecutive blocks; spread hints at non-linearity/autocorrelation."""
    n = len(xs)
    if n < blocks * 3:
        return []
    out = []
    step = n // blocks
    for b in range(blocks):
        lo, hi = b * step, (b + 1) * step if b < blocks - 1 else n
        s, _, _, _ = ols(xs[lo:hi], ys[lo:hi])
        out.append(s)
    return out


def lifetime_key(process_starttime, tid, thread_starttime) -> str:
    return f"{process_starttime}:{tid}:{thread_starttime}"


# ---------------------------------------------------------------- the harness
class Soak:
    def __init__(self, a):
        self.a = a
        self.work = Path(a.work).resolve()
        self.out = self.work / "out"
        self.bin = Path(a.build_dir).resolve() if a.build_dir else None
        self.yams = a.yams or (str(self.bin / "tools" / "yams-cli" / "yams-cli") if self.bin else "yams")
        self.daemon_bin = a.daemon or (str(self.bin / "src" / "daemon" / "yams-daemon") if self.bin else "yams-daemon")
        self.data_dir = self.work / "data"
        self.sock = self.work / "run" / "yams.sock"
        self.env = dict(os.environ)
        self.env.update({
            "HOME": str(self.work / "home"),
            "XDG_CONFIG_HOME": str(self.work / "xdg" / "config"),
            "XDG_DATA_HOME": str(self.work / "xdg" / "data"),
            "XDG_STATE_HOME": str(self.work / "xdg" / "state"),
            "XDG_RUNTIME_DIR": str(self.work / "run"),
            "YAMS_DATA_DIR": str(self.data_dir),
            "YAMS_DAEMON_SOCKET": str(self.sock),
            "YAMS_NON_INTERACTIVE": "1",
            "YAMS_CLI_DISABLE_DAEMON_AUTOSTART": "1",
        })
        for k in a.env or []:
            kk, _, vv = k.partition("=")
            self.env[kk] = vv
        self.daemon: subprocess.Popen | None = None
        self.pid = 0
        self.stop = threading.Event()
        self.phase = "setup"
        self.phase_lock = threading.Lock()
        self.status_calls = 0
        self.samples_f = None
        self.status_f = None
        self.ops_f = None
        self.stack_labels: dict[str, str] = {}
        self.queries: list[str] = []
        self.sampler_error: str | None = None
        self.workload_errors: list[str] = []
        self.settle_completed: bool | None = None
        self.seed_result = {
            "required": bool(a.corpus),
            "requested_docs": a.docs if a.corpus else 0,
            "loaded_docs": 0,
            "successful_docs": 0,
            "failed_batches": [],
            "errors": [],
        }

    # -- plumbing
    def cli(self, *args, timeout=120, check=False) -> subprocess.CompletedProcess:
        cmd = [self.yams, *args]
        res = subprocess.run(cmd, env=self.env, capture_output=True, text=True, timeout=timeout)
        if check and res.returncode != 0:
            die(f"{' '.join(cmd)} failed rc={res.returncode}\n{res.stdout}\n{res.stderr}")
        return res

    def status_json(self, timeout=30) -> dict | None:
        try:
            res = self.cli("status", "--json", timeout=timeout)
        except subprocess.TimeoutExpired:
            return None
        self.status_calls += 1
        if res.returncode != 0:
            return None
        txt = res.stdout.strip()
        # Tolerate log lines before the JSON object.
        i = txt.find("{")
        if i < 0:
            return None
        try:
            return json.loads(txt[i:])
        except json.JSONDecodeError:
            return None

    def prepare_dirs(self) -> None:
        if self.work.exists() and self.a.fresh:
            shutil.rmtree(self.work)
        for d in ("home", "xdg/config/yams", "xdg/data", "xdg/state", "run", "data", "log", "corpus", "out", "out/stacks"):
            (self.work / d).mkdir(parents=True, exist_ok=True)
        os.chmod(self.work / "run", 0o700)

    def init_config(self) -> None:
        cfg = self.work / "xdg" / "config" / "yams" / "config.toml"
        if cfg.exists() and not self.a.fresh:
            log(f"config exists: {cfg}")
        else:
            res = self.cli("init", "--auto", "--no-keygen", timeout=180)
            if res.returncode != 0:
                die(f"yams init failed: {res.stdout}{res.stderr}")
            log(f"wrote {cfg}")
        if self.a.config_append:
            with cfg.open("a", encoding="utf-8") as f:
                f.write("\n" + Path(self.a.config_append).read_text(encoding="utf-8") + "\n")
            log(f"appended {self.a.config_append}")
        (self.out / "config.redacted.toml").write_text(redact_config(cfg.read_text(encoding="utf-8")), encoding="utf-8")

    def start_daemon(self) -> None:
        if self.sock.exists():
            self.sock.unlink()
        logf = self.work / "log" / "daemon.log"
        pid_file = self.work / "run" / "yams-daemon.pid"
        cmd = daemon_command(self.daemon_bin, str(self.work / "xdg/config/yams/config.toml"),
                             str(self.sock), str(self.data_dir), str(pid_file), str(logf),
                             self.a.log_level, self.a.no_plugins,
                             getattr(self.a, "daemon_prefix", None))
        (self.out / "daemon_cmd.txt").write_text(" ".join(cmd) + "\n", encoding="utf-8")
        stdout = (self.work / "log" / "daemon.stdout").open("ab")
        self.daemon = subprocess.Popen(cmd, env=self.env, stdout=stdout, stderr=subprocess.STDOUT,
                                       start_new_session=True)
        self.pid = self.daemon.pid
        log(f"daemon pid {self.pid}")
        deadline = time.monotonic() + self.a.ready_timeout
        while time.monotonic() < deadline:
            if self.daemon.poll() is not None:
                die(f"daemon exited rc={self.daemon.returncode}; see {logf}")
            st = self.status_json(timeout=15)
            if st and (st.get("ready") or st.get("lifecycle_state") in ("ready", "Ready", "degraded")):
                log(f"daemon ready: lifecycle={st.get('lifecycle_state')} version={st.get('version')}")
                (self.out / "status_initial.json").write_text(json.dumps(st, indent=2), encoding="utf-8")
                # A launch prefix that forks (rather than execs) leaves the daemon in a child;
                # the daemon's own pid file names the process to sample.
                written = read_text(str(pid_file)).strip()
                if written.isdigit() and int(written) != self.pid:
                    log(f"daemon pid from pid file {written} (launched {self.pid})")
                    self.pid = int(written)
                return
            time.sleep(2)
        die("daemon not ready in time")

    def stop_daemon(self) -> None:
        if not self.daemon:
            return
        if self.daemon.poll() is None:
            try:
                self.cli("daemon", "stop", timeout=60)
            except Exception:
                pass
            try:
                self.daemon.wait(timeout=60)
            except subprocess.TimeoutExpired:
                log("daemon did not stop on request; SIGTERM")
                self.daemon.send_signal(signal.SIGTERM)
                try:
                    self.daemon.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    log("SIGKILL")
                    self.daemon.kill()
        log(f"daemon exited rc={self.daemon.returncode}")

    # -- corpus
    def load_corpus(self) -> list[tuple[str, str]]:
        """[(doc_id, text)] from a BEIR corpus.jsonl (title + text) limited to --docs."""
        docs = []
        with open(self.a.corpus, encoding="utf-8") as f:
            for line_number, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{self.a.corpus} line {line_number}: invalid JSON: {exc.msg}") from exc
                if not isinstance(d, dict):
                    raise ValueError(f"{self.a.corpus} line {line_number}: expected a JSON object")
                did = str(d.get("_id") or d.get("id") or len(docs))
                title = str(d.get("title") or "")
                text = str(d.get("text") or "")
                body = (f"# {title}\n\n" if title else "") + text + "\n"
                docs.append((re.sub(r"[^A-Za-z0-9_.-]", "_", did), body))
                if len(docs) >= self.a.docs:
                    break
        qpath = Path(self.a.corpus).with_name("queries.jsonl")
        if qpath.exists():
            with qpath.open() as f:
                for line_number, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        query = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(f"{qpath} line {line_number}: invalid JSON: {exc.msg}") from exc
                    if not isinstance(query, dict):
                        raise ValueError(f"{qpath} line {line_number}: expected a JSON object")
                    q = query.get("text", "")
                    if q:
                        self.queries.append(str(q))
        if not self.queries:
            self.queries = ["protein expression cells", "clinical trial outcome", "treatment effect patients",
                            "gene mutation cancer", "dietary intake risk"]
        return docs

    def seed(self) -> None:
        if not self.a.corpus:
            log("no corpus: skipping seed")
            return
        try:
            docs = self.load_corpus()
        except (OSError, ValueError) as exc:
            self.seed_result["errors"].append(str(exc))
            raise RuntimeError(f"seed corpus load failed: {exc}") from exc
        self.seed_result["loaded_docs"] = len(docs)
        if self.a.docs < 1 or len(docs) != self.a.docs:
            error = f"requested {self.a.docs} seed documents, loaded {len(docs)}"
            self.seed_result["errors"].append(error)
            raise RuntimeError(error)

        cdir = self.work / "corpus"
        paths = []
        for did, body in docs:
            p = cdir / f"{did}.md"
            p.write_text(body, encoding="utf-8")
            paths.append(str(p))
        log(f"seeding {len(paths)} docs from {self.a.corpus}")
        t0 = time.monotonic()
        tags = "soak,seed"
        for i in range(0, len(paths), self.a.add_batch):
            chunk = paths[i:i + self.a.add_batch]
            try:
                res = self.cli("add", *chunk, "--tags", tags, timeout=600)
                rc = res.returncode
                error = (res.stderr or res.stdout)[-300:] if rc != 0 else ""
            except Exception as exc:
                rc = None
                error = f"{type(exc).__name__}: {exc}"
            if rc != 0:
                self.seed_result["failed_batches"].append(
                    {"offset": i, "documents": len(chunk), "rc": rc, "error": error})
                log(f"add batch {i} rc={rc}: {error}")
            else:
                self.seed_result["successful_docs"] += len(chunk)
            if (i // self.a.add_batch) % 5 == 0:
                log(f"  processed {min(i + self.a.add_batch, len(paths))}/{len(paths)}")
        log(f"seed add done in {time.monotonic() - t0:.0f}s")
        if self.seed_result["successful_docs"] != self.a.docs:
            raise RuntimeError(
                f"seed incomplete: requested {self.a.docs}, successful "
                f"{self.seed_result['successful_docs']}")
        self.settle()

    def settle(self) -> None:
        """Wait until the worker queue is empty and CPU is low (post-ingest drained)."""
        deadline = time.monotonic() + self.a.settle_max
        quiet = 0
        prev = None
        while time.monotonic() < deadline:
            st = self.status_json()
            queued = (st or {}).get("worker", {}).get("queued", None)
            active = (st or {}).get("worker", {}).get("active", None)
            cur = task_stat(f"/proc/{self.pid}/stat")
            cpu = None
            if cur is not None and prev is not None and prev[0] is not None:
                dt = time.monotonic() - prev[1]
                cpu = ((cur["utime"] + cur["stime"]) -
                       (prev[0]["utime"] + prev[0]["stime"])) / CLK_TCK / dt * 100
            prev = (cur, time.monotonic())
            log(f"settle: queued={queued} active={active} cpu={cpu if cpu is None else round(cpu, 1)}% "
                f"docs={(st or {}).get('totalDocuments')} indexed={(st or {}).get('indexedDocuments')}")
            if queued == 0 and (active in (0, None)) and cpu is not None and cpu < self.a.settle_cpu:
                quiet += 1
                if quiet >= 3:
                    log("settled")
                    (self.out / "status_settled.json").write_text(json.dumps(st, indent=2), encoding="utf-8")
                    self.settle_completed = True
                    return
            else:
                quiet = 0
            time.sleep(self.a.settle_poll)
        self.settle_completed = False
        raise RuntimeError("seed did not settle before timeout")

    # -- sampling
    def set_phase(self, name: str) -> None:
        with self.phase_lock:
            self.phase = name

    def sampler(self) -> None:
        if self.samples_f is None or self.status_f is None:
            raise RuntimeError("sampler output streams are not open")
        samples_f = self.samples_f
        status_f = self.status_f
        iv = self.a.interval
        next_status = time.monotonic()
        while not self.stop.is_set():
            t0 = time.monotonic()
            s = sample_process(self.pid, str(self.data_dir))
            if s is None:
                if not self.stop.is_set():
                    self.sampler_error = "process disappeared during sampling"
                log("sampler: process gone")
                break
            with self.phase_lock:
                s["phase"] = self.phase
            s["status_calls"] = self.status_calls
            if self.a.status_every > 0 and t0 >= next_status:
                st = self.status_json()
                next_status = t0 + self.a.status_every
                if st:
                    status_f.write(json.dumps({"t": s["t"], "phase": s["phase"], "status": st}) + "\n")
                    status_f.flush()
                    s["status"] = {
                        "memoryUsageMb": st.get("memoryUsageMb"),
                        "cpuUsagePercent": st.get("cpuUsagePercent"),
                        "worker": st.get("worker"),
                        "totalDocuments": st.get("totalDocuments"),
                        "indexedDocuments": st.get("indexedDocuments"),
                        "wal": st.get("wal"),
                        "activeConnections": st.get("activeConnections"),
                        "lifecycle_state": st.get("lifecycle_state"),
                        "memory": st.get("memory"),
                    }
            samples_f.write(json.dumps(s) + "\n")
            samples_f.flush()
            self.stop.wait(max(0.0, iv - (time.monotonic() - t0)))

    def snapshot_stacks(self, tag: str, force: bool = False) -> None:
        if self.a.stacks_per_phase <= 0 and not force:
            return
        p = self.out / "stacks" / f"{tag}.txt"
        st = gdb_stacks(self.pid, p, depth=self.a.stack_depth)
        process = task_stat(f"/proc/{self.pid}/stat")
        for tid, frames in st.items():
            thread = task_stat(f"/proc/{self.pid}/task/{tid}/stat")
            if process is None or thread is None:
                continue
            identity = lifetime_key(process["starttime"], tid, thread["starttime"])
            self.stack_labels.setdefault(identity, label_from_frames(frames))
        (self.out / "stack_labels.json").write_text(json.dumps(self.stack_labels, indent=2), encoding="utf-8")
        log(f"stacks: {len(st)} threads -> {p.name}")

    # -- load
    def load_op(self, kind: str, fn):
        if self.ops_f is None:
            raise RuntimeError("operation output stream is not open")
        ops_f = self.ops_f
        t0 = time.monotonic()
        rc = None
        err = ""
        try:
            res = fn()
            rc = res.returncode
            if rc != 0:
                err = (res.stderr or res.stdout)[-200:]
        except subprocess.TimeoutExpired:
            rc = -1
            err = "timeout"
        ops_f.write(json.dumps({"t": time.time(), "phase": self.phase, "op": kind,
                                "ms": round((time.monotonic() - t0) * 1000, 1),
                                "rc": rc, "err": err}) + "\n")
        ops_f.flush()

    def run_load(self, seconds: float) -> None:
        rnd = random.Random(42)
        words = ["protein", "cell", "patients", "risk", "treatment", "gene", "expression", "trial",
                 "disease", "mice", "receptor", "cancer", "diet", "study", "signal"]
        deadline = time.monotonic() + seconds
        n = 0
        ldir = self.work / "corpus" / "load"
        ldir.mkdir(exist_ok=True)
        while time.monotonic() < deadline and not self.stop.is_set():
            t0 = time.monotonic()
            n += 1
            body = f"# soak load note {n}\n\n" + " ".join(rnd.choice(words) for _ in range(120)) + \
                   f"\n\nwritten at {time.time():.0f} seq {n}\n"
            p = ldir / f"load-{n:06d}.md"
            p.write_text(body, encoding="utf-8")
            self.load_op("add", lambda: self.cli("add", str(p), "--tags", "soak,load", timeout=120))
            q = rnd.choice(self.queries) if self.queries else " ".join(rnd.sample(words, 3))
            self.load_op("search", lambda: self.cli("search", q, "--limit", "10", timeout=120))
            pat = rnd.choice(words)
            self.load_op("grep", lambda: self.cli("grep", pat, "--include", "*.md", timeout=120))
            if n % 10 == 0:
                self.load_op("list", lambda: self.cli("list", "--limit", "20", timeout=120))
            self.stop.wait(max(0.0, self.a.load_period - (time.monotonic() - t0)))
        log(f"load: {n} rounds")

    # -- phases
    def parse_phases(self) -> list[tuple[str, int]]:
        out = []
        for item in self.a.phases.split(","):
            name, _, secs = item.strip().partition(":")
            if name not in ("idle", "load"):
                die(f"unknown phase {name!r} (idle|load)")
            out.append((name, int(secs or 600)))
        return out

    def run(self) -> int:
        phases = self.parse_phases()
        self.prepare_dirs()
        meta = {"schema_version": 2, "argv": sys.argv, "start": time.time(),
                "host": platform.node(), "phases": phases, "interval_s": self.a.interval,
                "yams": self.yams, "daemon": self.daemon_bin, "clk_tck": CLK_TCK,
                "daemon_prefix": getattr(self.a, "daemon_prefix", None),
                "observer_probes": {"status_every_s": self.a.status_every,
                                    "gdb_snapshots_per_phase": self.a.stacks_per_phase,
                                    "gdb_snapshot_at_end": bool(getattr(self.a, "stacks_at_end", False))},
                "run": {"completed": False, "sampler_error": None, "workload_errors": []}}
        th = None
        run_completed = False
        pending_error: BaseException | None = None
        try:
            self.init_config()
            self.samples_f = (self.out / "samples.jsonl").open("a", encoding="utf-8")
            self.status_f = (self.out / "status.jsonl").open("a", encoding="utf-8")
            self.ops_f = (self.out / "ops.jsonl").open("a", encoding="utf-8")
            self.start_daemon()
            meta["pid"] = self.pid
            pst = task_stat(f"/proc/{self.pid}/stat")
            meta["process_identity"] = {"pid": self.pid, "starttime_ticks": (pst or {}).get("starttime")}
            meta["launched_pid"] = self.daemon.pid if hasattr(self.daemon, "pid") else None
            meta["placement"] = placement(self.pid)
            try:
                meta["daemon_version"] = self.cli("--version", timeout=20).stdout.strip()
            except Exception:
                meta["daemon_version"] = None
            (self.out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

            def sampler_target():
                try:
                    self.sampler()
                except Exception as exc:
                    self.sampler_error = f"{type(exc).__name__}: {exc}"
                    self.stop.set()

            th = threading.Thread(target=sampler_target, daemon=True)
            th.start()
            self.set_phase("seed")
            self.seed()
            if self.a.warmup > 0:
                self.set_phase("warmup")
                log(f"warmup {self.a.warmup}s")
                self.stop.wait(self.a.warmup)
            for i, (name, secs) in enumerate(phases):
                pname = f"{i}-{name}"
                self.set_phase(pname)
                log(f"phase {pname}: {secs}s")
                marks = []
                if self.a.stacks_per_phase > 0:
                    k = self.a.stacks_per_phase
                    marks = [secs * (j + 1) / (k + 1) for j in range(k)]
                phase_start = time.monotonic()
                if name == "load":
                    def load_target(seconds=secs):
                        try:
                            self.run_load(seconds)
                        except Exception as exc:
                            self.workload_errors.append(f"{type(exc).__name__}: {exc}")
                            self.stop.set()
                    worker = threading.Thread(target=load_target, daemon=True)
                    worker.start()
                    for mark in marks:
                        self.stop.wait(max(0.0, phase_start + mark - time.monotonic()))
                        self.snapshot_stacks(f"{pname}-{int(mark)}s")
                    worker.join()
                else:
                    for mark in marks:
                        self.stop.wait(max(0.0, phase_start + mark - time.monotonic()))
                        self.snapshot_stacks(f"{pname}-{int(mark)}s")
                    self.stop.wait(max(0.0, phase_start + secs - time.monotonic()))
                if self.sampler_error or self.workload_errors:
                    break
            self.set_phase("end")
            if getattr(self.a, "stacks_at_end", False) and not (self.sampler_error or self.workload_errors):
                # One snapshot after the timed phases labels threads without touching the
                # measured window.
                self.snapshot_stacks("end", force=True)
            st = self.status_json() if self.a.status_every > 0 else None
            if st:
                (self.out / "status_final.json").write_text(json.dumps(st, indent=2), encoding="utf-8")
            run_completed = not self.sampler_error and not self.workload_errors
        except BaseException as exc:
            pending_error = exc
            self.workload_errors.append(f"harness {type(exc).__name__}: {exc}")
        finally:
            self.stop.set()
            if th is not None:
                th.join(timeout=30)
                if th.is_alive():
                    self.sampler_error = "sampler thread did not stop"
            for stream in (self.samples_f, self.status_f, self.ops_f):
                if stream is not None and not stream.closed:
                    stream.close()
            run_completed = (run_completed and not self.sampler_error and
                             not self.workload_errors and pending_error is None)
            meta["seed"] = self.seed_result
            meta["settle"] = {"required": bool(self.a.corpus), "completed": self.settle_completed}
            meta["run"] = {"completed": run_completed, "sampler_error": self.sampler_error,
                           "workload_errors": self.workload_errors}
            meta["end"] = time.time()
            (self.out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

        summary = None
        summary_error: BaseException | None = None
        try:
            summary = summarize(self.out)
        except BaseException as exc:
            summary_error = exc
            self.workload_errors.append(f"summary {type(exc).__name__}: {exc}")
            meta["run"] = {"completed": False, "sampler_error": self.sampler_error,
                           "workload_errors": self.workload_errors}
            meta["end"] = time.time()
            (self.out / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

        valid = summary is not None and summary["validation"]["valid"] is True
        if self.daemon is not None and not (self.a.keep_daemon and valid):
            self.stop_daemon()
        try:
            shutil.copy2(self.work / "log" / "daemon.log", self.out / "daemon.log")
        except OSError:
            pass
        if pending_error is not None:
            if isinstance(pending_error, SystemExit):
                raise pending_error
            return 3
        if summary_error is not None:
            raise summary_error
        return 0 if valid else 3


# ---------------------------------------------------------------- summary
def thread_deltas(rows: list[dict]) -> tuple[dict, float, int]:
    """Counters over comparable PID/TID lifetimes; return rejected reset/reuse intervals."""
    acc: dict[str, dict] = {}
    wall = 0.0
    rejected = 0
    for a, b in zip(rows, rows[1:], strict=False):
        dt = b["mono"] - a["mono"]
        process_starttime = a.get("process_starttime")
        if dt <= 0 or process_starttime is None or process_starttime != b.get("process_starttime"):
            rejected += 1
            continue
        wall += dt
        for tid, tb in b["tasks"].items():
            ta = a["tasks"].get(tid)
            if not ta or ta.get("starttime") != tb.get("starttime"):
                if ta:
                    rejected += 1
                continue
            counters = {
                "cpu": (tb["utime"] + tb["stime"]) - (ta["utime"] + ta["stime"]),
                "vcs": tb["vcs"] - ta["vcs"],
                "nvcs": tb["nvcs"] - ta["nvcs"],
                "slices": tb["slices"] - ta["slices"],
            }
            if any(value < 0 for value in counters.values()):
                rejected += 1
                continue
            identity = lifetime_key(process_starttime, tid, tb["starttime"])
            d = acc.setdefault(identity, {"tid": tid, "thread_starttime": tb["starttime"],
                                           "process_starttime": process_starttime,
                                           "comm": tb["comm"], "cpu": 0, "vcs": 0,
                                           "nvcs": 0, "slices": 0,
                                           "wchan": defaultdict(float), "secs": 0.0})
            for key, value in counters.items():
                d[key] += value
            d["wchan"][tb["wchan"]] += dt
            d["secs"] += dt
    return acc, wall, rejected


def validate_summary(meta: dict, samples: list[dict], ops: list[dict], phases: dict,
                     artifact_errors: list[str] | None = None) -> dict:
    """Classify current-schema runs; legacy artifacts remain explicitly unknown."""
    reasons = list(artifact_errors or [])
    if meta.get("schema_version") != 2:
        errors = ["legacy artifact lacks measurement-validity metadata", *reasons]
        return {"status": "unknown", "valid": None, "errors": errors, "reasons": errors}
    run = meta.get("run") or {}
    if not run.get("completed"):
        reasons.append("run did not complete")
    if run.get("sampler_error"):
        reasons.append(f"sampler error: {run['sampler_error']}")
    reasons.extend(f"workload error: {err}" for err in run.get("workload_errors", []))
    settle = meta.get("settle") or {}
    if settle.get("required"):
        seed = meta.get("seed")
        if not isinstance(seed, dict):
            reasons.append("seed accounting missing")
        else:
            requested = seed.get("requested_docs")
            loaded = seed.get("loaded_docs")
            successful = seed.get("successful_docs")
            if requested != loaded:
                reasons.append(f"seed corpus incomplete: requested {requested}, loaded {loaded}")
            if requested != successful:
                reasons.append(f"seed incomplete: requested {requested}, successful {successful}")
            if seed.get("failed_batches"):
                reasons.append(f"seed add failures: {len(seed['failed_batches'])}")
            reasons.extend(f"seed error: {error}" for error in seed.get("errors", []))
        if settle.get("completed") is not True:
            reasons.append("seed did not settle")
    failed_ops = sum(1 for op in ops if op.get("rc") != 0)
    if failed_ops:
        reasons.append(f"operation failures: {failed_ops}")
    interval = float(meta.get("interval_s") or 0)
    for index, planned in enumerate(meta.get("phases") or []):
        name, seconds = planned
        phase_name = f"{index}-{name}"
        rows = phases.get(phase_name, [])
        observed = rows[-1]["mono"] - rows[0]["mono"] if len(rows) >= 2 else 0.0
        minimum_samples = max(2, math.floor(float(seconds) / interval)) if interval > 0 else 2
        if len(rows) < minimum_samples:
            reasons.append(f"{phase_name}: {len(rows)} samples, need at least {minimum_samples}")
        tolerance = interval * 1.5 if interval > 0 else 0.0
        if observed + tolerance < float(seconds):
            reasons.append(f"{phase_name}: observed {observed:.1f}s of planned {seconds}s")
    starts = {row.get("process_starttime") for row in samples}
    if None in starts or len(starts) != 1:
        reasons.append("process identity missing or changed")
    return {"status": "invalid" if reasons else "valid", "valid": not reasons,
            "errors": reasons, "reasons": reasons,
            "planned_phase_count": len(meta.get("phases") or []),
            "observed_phase_count": sum(1 for name in phases if re.match(r"^\d+-(idle|load)$", name))}


def is_finite_number(value) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool) and
            math.isfinite(float(value)))


def load_jsonl(p: Path, numeric_fields: tuple[str, ...], *, sample_rows: bool = False) -> tuple[list[dict], list[str]]:
    rows = []
    errors = []
    if not p.exists():
        return rows, errors
    with p.open() as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"{p.name} line {line_number}: invalid JSON: {exc.msg}")
                continue
            if not isinstance(row, dict):
                errors.append(f"{p.name} line {line_number}: expected a JSON object")
                continue
            invalid = next((field for field in numeric_fields
                            if not is_finite_number(row.get(field))), None)
            if invalid is not None:
                errors.append(f"{p.name} line {line_number}: invalid numeric {invalid}")
                continue
            if not isinstance(row.get("phase"), str):
                errors.append(f"{p.name} line {line_number}: invalid phase")
                continue
            if sample_rows:
                files = row.get("files")
                tasks = row.get("tasks")
                if (not isinstance(files, dict) or
                        any(not is_finite_number(value) for value in files.values())):
                    errors.append(f"{p.name} line {line_number}: invalid numeric files")
                    continue
                if not isinstance(tasks, dict):
                    errors.append(f"{p.name} line {line_number}: invalid tasks")
                    continue
                task_fields = ("starttime", "utime", "stime", "vcs", "nvcs", "slices")
                bad_task = any(
                    not isinstance(task, dict) or
                    any(not is_finite_number(task.get(field)) for field in task_fields)
                    for task in tasks.values())
                if bad_task:
                    errors.append(f"{p.name} line {line_number}: invalid numeric task counters")
                    continue
            rows.append(row)
    return rows, errors


def summarize(out: Path) -> dict:
    sample_fields = ("t", "mono", "rss_kb", "pss_kb", "anon_kb", "threads", "fds",
                     "proc_vcs", "proc_nvcs", "utime", "stime", "process_starttime",
                     "status_calls")
    samples, sample_errors = load_jsonl(out / "samples.jsonl", sample_fields, sample_rows=True)
    ops, op_errors = load_jsonl(out / "ops.jsonl", ("ms", "rc"))
    artifact_errors = [*sample_errors, *op_errors]
    labels = {}
    if (out / "stack_labels.json").exists():
        labels = json.loads((out / "stack_labels.json").read_text(encoding="utf-8"))
    phases: dict[str, list[dict]] = defaultdict(list)
    for s in samples:
        phases[s.get("phase", "?")].append(s)
    meta = json.loads((out / "meta.json").read_text(encoding="utf-8")) if (out / "meta.json").exists() else {}
    summary: dict = {"n_samples": len(samples), "phases": {}}


    pre_load_rss = None
    for pname, rows in phases.items():
        if len(rows) < 2:
            continue
        t0 = rows[0]["mono"]
        hours = [(r["mono"] - t0) / 3600.0 for r in rows]
        rss = [r["rss_kb"] / 1024.0 for r in rows]
        pss = [r["pss_kb"] / 1024.0 for r in rows]
        anon = [r["anon_kb"] / 1024.0 for r in rows]
        rs, _, rci, n = ols(hours, rss)
        ps, _, pci, _ = ols(hours, pss)
        as_, _, aci, _ = ols(hours, anon)
        wall = rows[-1]["mono"] - rows[0]["mono"]
        same_process = (rows[0].get("process_starttime") is not None and
                        rows[0].get("process_starttime") == rows[-1].get("process_starttime"))
        cpu_ticks = ((rows[-1]["utime"] + rows[-1]["stime"]) -
                     (rows[0]["utime"] + rows[0]["stime"]))
        cpu_pct = (cpu_ticks / CLK_TCK / wall * 100
                   if same_process and cpu_ticks >= 0 and wall > 0 else None)
        proc_vcs_delta = rows[-1]["proc_vcs"] - rows[0]["proc_vcs"]
        proc_vcs = proc_vcs_delta if same_process and proc_vcs_delta >= 0 else None
        acc, twall, rejected_deltas = thread_deltas(rows)
        all_vcs = sum(d["vcs"] for d in acc.values())
        by_tid = []
        for identity, d in acc.items():
            top_w = max(d["wchan"].items(), key=lambda kv: kv[1])[0] if d["wchan"] else "-"
            by_tid.append({
                "identity": identity, "tid": d["tid"], "comm": d["comm"],
                "label": labels.get(identity, "(unknown)"),
                "voluntary_ctxt_switches_s": round(d["vcs"] / d["secs"], 2) if d["secs"] else 0.0,
                "nvcs_s": round(d["nvcs"] / d["secs"], 3) if d["secs"] else 0.0,
                "slices_s": round(d["slices"] / d["secs"], 2) if d["secs"] else 0.0,
                "cpu_pct": round(d["cpu"] / CLK_TCK / d["secs"] * 100, 3) if d["secs"] else 0.0,
                "wchan": top_w,
            })
        by_tid.sort(key=lambda r: -r["voluntary_ctxt_switches_s"])

        def aggregate_threads(key_fn, lifetimes=acc):
            grouped: dict[str, dict] = {}
            for identity, d in lifetimes.items():
                key = key_fn(identity, d)
                group = grouped.setdefault(key, {"observed_lifetimes": 0, "vcs": 0,
                                                  "cpu_ticks": 0, "observed_thread_s": 0.0})
                group["observed_lifetimes"] += 1
                group["vcs"] += d["vcs"]
                group["cpu_ticks"] += d["cpu"]
                group["observed_thread_s"] += d["secs"]
            result = {}
            for key, group in grouped.items():
                denom = group.pop("observed_thread_s")
                result[key] = {
                    "observed_lifetimes": group["observed_lifetimes"],
                    "voluntary_ctxt_switches_per_observed_thread_s": round(group["vcs"] / denom, 2),
                    "cpu_pct_per_observed_thread": round(group["cpu_ticks"] / CLK_TCK / denom * 100, 3),
                }
            return result

        by_comm = aggregate_threads(lambda _identity, d: d["comm"])
        by_label = aggregate_threads(lambda identity, _d: labels.get(identity, "(unknown)"))
        files0, files1 = rows[0]["files"], rows[-1]["files"]
        file_delta = {k: files1.get(k, 0) - files0.get(k, 0) for k in set(files0) | set(files1)
                      if files1.get(k, 0) != files0.get(k, 0)}
        status_rows = [r["status"] for r in rows if r.get("status")]
        ph = {
            "samples": n, "wall_s": round(wall, 1),
            "rss_mib": {"first": round(rss[0], 1), "last": round(rss[-1], 1), "min": round(min(rss), 1),
                        "max": round(max(rss), 1), "slope_mib_h": round(rs, 2), "exploratory_iid_ols_ci95": round(rci, 2) if rci == rci else None,
                        "block_slopes": [round(b, 2) for b in block_slopes(hours, rss)]},
            "pss_mib": {"first": round(pss[0], 1), "last": round(pss[-1], 1), "slope_mib_h": round(ps, 2),
                        "exploratory_iid_ols_ci95": round(pci, 2) if pci == pci else None},
            "anon_mib": {"first": round(anon[0], 1), "last": round(anon[-1], 1), "slope_mib_h": round(as_, 2),
                         "exploratory_iid_ols_ci95": round(aci, 2) if aci == aci else None},
            "cpu_pct": round(cpu_pct, 3) if cpu_pct is not None else None,
            "voluntary_ctxt_switches_s_observed": round(all_vcs / wall, 1) if wall > 0 else None,
            "main_thread_voluntary_ctxt_switches_s": (
                round(proc_vcs / wall, 1) if proc_vcs is not None and wall > 0 else None
            ),
            "scheduler_wakeups": {"measured": False, "reason": "requires scheduler tracing; /proc context switches are not wakeups"},
            "rejected_counter_intervals": rejected_deltas,
            "threads": {"first": rows[0]["threads"], "last": rows[-1]["threads"], "max": max(r["threads"] for r in rows)},
            "fds": {"first": rows[0]["fds"], "last": rows[-1]["fds"], "max": max(r["fds"] for r in rows)},
            "data_dir_total_mib": {"first": round(files0.get("__total__", 0) / 2**20, 1),
                                   "last": round(files1.get("__total__", 0) / 2**20, 1)},
            "file_delta_bytes": dict(sorted(file_delta.items(), key=lambda kv: -abs(kv[1]))[:12]),
            "status_calls_in_phase": rows[-1]["status_calls"] - rows[0]["status_calls"],
            "status_memoryUsageMb": [s.get("memoryUsageMb") for s in status_rows][-3:],
            "status_worker_last": status_rows[-1].get("worker") if status_rows else None,
            "top_waking_threads": by_tid[:12],
            "by_comm": dict(sorted(by_comm.items(), key=lambda kv: -kv[1]["voluntary_ctxt_switches_per_observed_thread_s"])),
            "by_label": dict(sorted(by_label.items(), key=lambda kv: -kv[1]["voluntary_ctxt_switches_per_observed_thread_s"])),
        }
        pops = [o for o in ops if o.get("phase") == pname]
        if pops:
            lat: dict[str, list[float]] = defaultdict(list)
            errs: dict[str, int] = defaultdict(int)
            for o in pops:
                lat[o["op"]].append(o["ms"])
                if o["rc"] != 0:
                    errs[o["op"]] += 1
            ph["ops"] = {k: {"n": len(v), "p50_ms": round(sorted(v)[len(v) // 2], 1),
                             "p95_ms": round(sorted(v)[int(len(v) * 0.95)], 1), "max_ms": round(max(v), 1),
                             "errors": errs.get(k, 0)} for k, v in lat.items()}
        summary["phases"][pname] = ph
        if pname.endswith("-idle") and pre_load_rss is None:
            pre_load_rss = rss[-1]
        if pname.endswith("-load"):
            summary["rss_before_load_mib"] = round(pre_load_rss, 1) if pre_load_rss else None
            summary["rss_end_of_load_mib"] = round(rss[-1], 1)
    # RSS recovery: last idle phase vs pre-load idle.
    idle_names = [p for p in summary["phases"] if p.endswith("-idle")]
    if len(idle_names) >= 2:
        a, b = summary["phases"][idle_names[0]], summary["phases"][idle_names[-1]]
        summary["rss_recovery"] = {
            "idle_before_last_mib": a["rss_mib"]["last"],
            "idle_after_last_mib": b["rss_mib"]["last"],
            "retained_mib": round(b["rss_mib"]["last"] - a["rss_mib"]["last"], 1),
            "load_peak_mib": max(ph["rss_mib"]["max"] for n, ph in summary["phases"].items() if n.endswith("-load")) if any(
                n.endswith("-load") for n in summary["phases"]) else None,
        }
    summary["regression_assumptions"] = "OLS intervals assume iid residuals and are exploratory, not leak proof; inspect block slopes and raw time series"
    summary["validation"] = validate_summary(meta, samples, ops, phases, artifact_errors)
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (out / "summary.md").write_text(render_md(summary), encoding="utf-8")
    print_console(render_md(summary))
    return summary


def print_console(text: str) -> None:
    """Print text the console may not be able to encode (cp1252 on Windows, ascii under LC_ALL=C)."""
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def render_md(s: dict) -> str:
    L = [f"# soak summary ({s['n_samples']} samples)", "", f"Validation: {s['validation']['status']}", ""]
    for reason in s['validation']['reasons']:
        L.append(f"- {reason}")
    L += ["", "Regression note: iid-residual OLS intervals are exploratory, not leak proof; block slopes are reported.", ""]
    L.append("| phase | wall s | RSS first→last MiB | RSS slope MiB/h (exploratory iid ±95%) | PSS slope | anon slope | CPU % | voluntary context switches/s | threads | fds | status calls |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for name, p in s["phases"].items():
        r = p["rss_mib"]
        L.append(f"| {name} | {p['wall_s']} | {r['first']}→{r['last']} | {r['slope_mib_h']} (±{r['exploratory_iid_ols_ci95']}) "
                 f"| {p['pss_mib']['slope_mib_h']} (±{p['pss_mib']['exploratory_iid_ols_ci95']}) | {p['anon_mib']['slope_mib_h']} (±{p['anon_mib']['exploratory_iid_ols_ci95']}) "
                 f"| {p['cpu_pct']} | {p['voluntary_ctxt_switches_s_observed']} | {p['threads']['first']}→{p['threads']['last']} "
                 f"| {p['fds']['first']}→{p['fds']['last']} | {p['status_calls_in_phase']} |")
    if s.get("rss_recovery"):
        rr = s["rss_recovery"]
        L += ["", f"RSS recovery: idle-before {rr['idle_before_last_mib']} MiB, load peak {rr['load_peak_mib']} MiB, "
                  f"idle-after {rr['idle_after_last_mib']} MiB, retained {rr['retained_mib']} MiB"]
    for name, p in s["phases"].items():
        L += ["", f"## {name}", "", f"block slopes MiB/h: {p['rss_mib']['block_slopes']}", "",
              "| tid | comm | voluntary context switches/s | cpu % | wchan | label |", "|---|---|---|---|---|---|"]
        for t in p["top_waking_threads"]:
            L.append(f"| {t['tid']} | {t['comm']} | {t['voluntary_ctxt_switches_s']} | {t['cpu_pct']} | {t['wchan']} | {t['label']} |")
        L += ["", "by label:", ""]
        for k, v in list(p["by_label"].items())[:15]:
            L.append(f"- {v['voluntary_ctxt_switches_per_observed_thread_s']} voluntary context switches/observed-thread-s, "
                     f"{v['cpu_pct_per_observed_thread']}% cpu/observed-thread, "
                     f"{v['observed_lifetimes']} observed lifetimes: {k}")
        if p.get("ops"):
            L += ["", "ops: " + ", ".join(f"{k} n={v['n']} p50={v['p50_ms']}ms p95={v['p95_ms']}ms err={v['errors']}"
                                           for k, v in p["ops"].items())]
        if p["file_delta_bytes"]:
            L += ["", "file deltas (bytes): " + ", ".join(f"{k}={v:+d}" for k, v in p["file_delta_bytes"].items())]
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------- cli
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="start a daemon, seed, run phases, sample, summarize")
    r.add_argument("--work", required=True, help="isolated work dir (data, config, socket, out/)")
    r.add_argument("--build-dir", help="meson build dir (uses src/daemon/yams-daemon and tools/yams-cli/yams-cli)")
    r.add_argument("--yams", help="yams CLI path (overrides --build-dir)")
    r.add_argument("--daemon", help="yams-daemon path (overrides --build-dir)")
    r.add_argument("--corpus", help="BEIR corpus.jsonl to seed (queries.jsonl beside it is used for load)")
    r.add_argument("--docs", type=int, default=3000)
    r.add_argument("--add-batch", type=int, default=100, help="paths per `yams add` call while seeding")
    r.add_argument("--phases", default="idle:1800,load:1800,idle:1800", help="name:seconds,... (idle|load)")
    r.add_argument("--interval", type=float, default=10.0, help="sample period seconds")
    r.add_argument("--status-every", type=float, default=0.0, help="opt-in `yams status --json` period; 0 disables (default)")
    r.add_argument("--load-period", type=float, default=5.0, help="seconds between load rounds (add+search+grep)")
    r.add_argument("--warmup", type=int, default=120, help="seconds after seed/settle before phases start")
    r.add_argument("--settle-max", type=int, default=1800)
    r.add_argument("--settle-poll", type=int, default=15)
    r.add_argument("--settle-cpu", type=float, default=5.0, help="daemon CPU%% below which seeding counts as settled")
    r.add_argument("--stacks-per-phase", type=int, default=0, help="opt-in gdb stack snapshots per phase (0 disables, default)")
    r.add_argument("--stacks-at-end", action="store_true",
                   help="one gdb stack snapshot after the timed phases, to label threads")
    r.add_argument("--stack-depth", type=int, default=20)
    r.add_argument("--daemon-prefix",
                   help="command prefix for the daemon, e.g. 'taskset -c 2-5' or "
                        "'systemd-run --user --scope -p CPUQuota=400%% -p MemoryMax=8G taskset -c 2-5'")
    r.add_argument("--ready-timeout", type=int, default=300)
    r.add_argument("--log-level", default="info")
    r.add_argument("--no-plugins", action="store_true")
    r.add_argument("--fresh", action="store_true", help="wipe --work first")
    r.add_argument("--keep-daemon", action="store_true", help="leave the daemon running at the end")
    r.add_argument("--config-append", help="file whose TOML is appended to the generated config")
    r.add_argument("--env", action="append", help="extra KEY=VALUE for daemon and CLI")
    s = sub.add_parser("summarize", help="recompute summary from --work/out")
    s.add_argument("--work", required=True)
    a = ap.parse_args()
    if a.cmd == "summarize":
        summary = summarize(Path(a.work) / "out")
        return 0 if summary["validation"]["valid"] is True else 3
    if sys.platform != "linux":
        die("run needs Linux (/proc sampling)")
    return Soak(a).run()


if __name__ == "__main__":
    sys.exit(main())
