#!/usr/bin/env python3
"""Write seed inputs for the direct-P2P stream harnesses and the topology codec harness.

Stream harnesses read their parameters with FuzzedDataProvider, which takes integers and
booleans from the *end* of the input and the frame stream (length-prefixed, big-endian u32) from
the front. Each seed is therefore: framed session bytes + parameter bytes, where the last byte
is consumed first.
"""

from __future__ import annotations

import json
import struct
import sys
from pathlib import Path

PEER = "fuzz-peer"
LOCAL = "fuzz-local"
CORPUS = "fuzz-corpus"
PIN = "0123456789abcdef" * 4


def frame(obj: dict | bytes) -> bytes:
    payload = obj if isinstance(obj, bytes) else json.dumps(obj, separators=(",", ":")).encode()
    return struct.pack(">I", len(payload)) + payload


def hello(kind: str = "hello") -> dict:
    return {
        "type": kind,
        "protocol": 4,
        "schema_version": 4,
        "node_id": PEER,
        "corpus_id": CORPUS,
        "corpus_epoch": 1,
        "max_writer_advance": 128,
        "max_writer_window_bytes": 1048576,
    }


STATE = {
    "type": "state",
    "vv": {"counters_": {}},
    "seen": {},
    "commitments": [],
    "quarantined_writers": [],
}
WINDOW = {"type": "writer_window", "writer": PEER, "vv": {"counters_": {}}, "commitments": []}
REPLICATION = {"type": "replication_mode", "mode": "delta"}
BATCH_EMPTY = {"type": "delta_batch", "count": 0, "has_more": False}
BATCH_ONE = {"type": "delta_batch", "count": 1, "has_more": False}
RECORD = {
    "type": "delta_record",
    "key": "user/fuzz",
    "writer": PEER,
    "counter": 1,
    "payload_size": 5,
}


def handshake_seeds() -> dict[str, bytes]:
    session = frame(hello()) + frame(STATE) + frame(WINDOW)
    # Tail bytes, read last-first: allowFirstContact, then mode (3 = full acceptor).
    return {
        "01_accept_session.bin": session + bytes([3, 1]),
        "02_accept_no_first_contact.bin": session + bytes([3, 0]),
        # mode 0: a single hello frame preceded by its size selector.
        "03_hello_frame.bin": json.dumps(hello()).encode() + bytes([0, 0, 1]),
    }


def delta_stream_seeds() -> dict[str, bytes]:
    # Parameter tail (consumed last byte first): maxDeltasPerBatch, maxBatches,
    # maxDeltasPerSession, maxWireBytesPerSession, maxSnapshotRecords, maxSnapshotWireBytes,
    # mode, remainingDeltas, remainingBytes. Single bytes keep every value small and in range.
    def tail(mode: int) -> bytes:
        return bytes([0x40, 0x10, mode, 0x40, 0x40, 0x40, 0x10, 0x04, 0x08][::-1])

    single = frame(BATCH_ONE) + frame(RECORD) + frame(b"hello")
    empty = frame(BATCH_EMPTY)
    bootstrap = frame(REPLICATION) + frame(BATCH_EMPTY)
    return {
        "01_batch_one_record.bin": single + tail(0),
        "02_session_empty.bin": empty + tail(1),
        "03_bootstrap_delta_mode.bin": bootstrap + tail(2),
    }


def inbound_seeds() -> dict[str, bytes]:
    session = (
        frame(hello())
        + frame(STATE)
        + frame(WINDOW)
        + frame(REPLICATION)
        + frame(BATCH_EMPTY)
        + frame({"type": "delta_ack", "merged": 0})
    )
    return {"01_inbound_session.bin": session + bytes([1])}


def json_seeds() -> dict[str, bytes]:
    return {
        "01_hello.json": json.dumps(hello()).encode(),
        "02_state.json": json.dumps(STATE).encode(),
        "03_record.json": json.dumps(RECORD).encode(),
        "04_deep_nesting.json": b"[" * 40 + b"]" * 40,
        "05_unbalanced_close.json": b"]]]]" + b"[" * 30 + b"]" * 30,
    }


def connstr_seeds() -> dict[str, bytes]:
    return {
        "01_host_port.txt": b"127.0.0.1:7443",
        "02_scheme_ipv6_query.txt": (
            f"yams://[::1]:7443?corpus={CORPUS}&epoch=2&pin={PIN}&remember=false".encode()
        ),
        "03_pin.txt": PIN.upper().encode(),
        "04_prefixed_pin.txt": f"sha256:{PIN}".encode(),
    }


def topology_seeds() -> dict[str, bytes]:
    def string(value: str) -> bytes:
        data = value.encode()
        return struct.pack("<I", len(data)) + data

    header = struct.pack("<II", 0x59414D54, 1) + bytes([0, 0, 0, 0]) + struct.pack("<QQ", 0, 7)
    strings = string("snap-1") + string("connected") + string("space") + string("relations")
    empty_batch = header + strings + struct.pack("<III", 0, 0, 0)
    legacy = json.dumps({"snapshot_id": "s", "clusters": [], "memberships": []}).encode()
    envelope = json.dumps(
        {"format": "zstd_binary_v1", "compression": "none", "data_b64": "AA=="}
    ).encode()
    # Leading byte selects the mode: odd = binary codec, even = JSON/envelope path.
    return {
        "01_empty_binary.bin": b"\x01" + empty_batch,
        "02_legacy_json.bin": b"\x00" + legacy,
        "03_envelope.bin": b"\x00" + envelope,
    }


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <corpus-root>", file=sys.stderr)
        return 2
    root = Path(sys.argv[1])
    for target, seeds in {
        "p2p_json": json_seeds(),
        "p2p_connstr": connstr_seeds(),
        "p2p_handshake": handshake_seeds(),
        "p2p_delta_stream": delta_stream_seeds(),
        "p2p_inbound": inbound_seeds(),
        "topology_codec": topology_seeds(),
    }.items():
        directory = root / target
        directory.mkdir(parents=True, exist_ok=True)
        for name, data in seeds.items():
            (directory / name).write_bytes(data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
