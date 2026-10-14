#!/usr/bin/env python3
"""Convert LongMemEval_S from HuggingFace to BEIR format for yams benchmarking.

Downloads the cleaned LongMemEval_S dataset and converts it to BEIR format
(corpus.jsonl, queries.jsonl, qrels/test.tsv) so the existing
retrieval_quality_bench can consume it directly.

Every corpus document carries a `metadata` object with `event_time` (ISO-8601,
from the session's haystack date) and `session_id`; the bench copies it onto the
ingested documents and the topology extractor reads both keys. `--round-docs`
adds one document per session round (a user turn plus the replies that follow)
with the same `event_time`/`session_id` plus `round_index`. `--full-haystack`
also writes `haystacks.jsonl`, one line per question with its haystack session
ids and dates, so per-question evaluation can scope the shared corpus.

Usage:
    uv run --with datasets scripts/prepare_longmemeval_s.py
    uv run --with datasets scripts/prepare_longmemeval_s.py --max-questions 50
    uv run --with datasets scripts/prepare_longmemeval_s.py --full-haystack --round-docs
    python3 scripts/prepare_longmemeval_s.py --self-test

Output: ~/.cache/yams/benchmarks/longmemeval_s/
"""

import argparse
import json
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

# LongMemEval haystack dates look like "2023/05/20 (Sat) 02:21".
_LME_DATE = re.compile(
    r"^(\d{4})/(\d{2})/(\d{2})(?:\s*\([A-Za-z]+\))?(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$"
)
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?)?$")


def normalize_event_time(raw) -> str:
    """Return an ISO-8601 `event_time` for a haystack date.

    `YYYY/MM/DD (Day) HH:MM` becomes `YYYY-MM-DDTHH:MM:00`; an ISO value passes
    through; anything else is returned unchanged (the extractor ignores values it
    cannot parse).
    """
    text = str(raw).strip()
    match = _LME_DATE.match(text)
    if match:
        year, month, day, hour, minute, second = match.groups()
        if hour is None:
            return f"{year}-{month}-{day}"
        return f"{year}-{month}-{day}T{hour}:{minute}:{second or '00'}"
    if _ISO_DATE.match(text):
        return text
    return text


def _as_turn(turn) -> dict:
    if isinstance(turn, str):
        turn = json.loads(turn)
    return turn


def _turn_lines(turn: dict) -> str:
    role = turn.get("role", "unknown")
    content = turn.get("content", "")
    label = "User" if role == "user" else "Assistant"
    return f"{label}: {content}"


def serialize_session(turns: list, date: str) -> str:
    """Serialize a chat session into searchable text.

    Each turn may be a dict or a JSON string encoding {role, content}.
    """
    lines = [f"[{date}]"]
    for turn in turns:
        lines.append(_turn_lines(_as_turn(turn)))
    return "\n".join(lines)


def session_rounds(turns: list) -> list:
    """Group turns into rounds: a user turn opens a round, replies join it."""
    rounds = []
    for turn in turns:
        turn = _as_turn(turn)
        if turn.get("role") == "user" or not rounds:
            rounds.append([turn])
        else:
            rounds[-1].append(turn)
    return rounds


def serialize_round(round_turns: list, date: str) -> str:
    lines = [f"[{date}]"]
    lines.extend(_turn_lines(turn) for turn in round_turns)
    return "\n".join(lines)


def round_doc_id(session_id: str, index: int) -> str:
    return f"{session_id}__r{index}"


@dataclass
class Conversion:
    corpus: dict = field(default_factory=dict)  # doc id -> {_id, title, text, metadata}
    queries: list = field(default_factory=list)  # [{_id, text, metadata}]
    qrels: list = field(default_factory=list)  # [(query_id, session_id, score)]
    haystacks: list = field(default_factory=list)  # [{question_id, haystack_session_ids, ...}]
    abstention_count: int = 0
    question_type_counts: dict = field(default_factory=dict)
    session_count: int = 0
    round_doc_count: int = 0


def convert(records, *, full_haystack=False, include_abstention=False, round_docs=False):
    """Build the BEIR corpus/queries/qrels (and the haystack sidecar) in memory."""
    needed_sids = set()  # session IDs referenced by answer_session_ids
    if not full_haystack:
        for item in records:
            for sid in item.get("answer_session_ids", []):
                needed_sids.add(sid)

    conv = Conversion()
    # One document per unique session, deduplicated across questions; the first
    # question that mentions a session supplies its date.
    for item in records:
        qid = item["question_id"]
        qtype = item.get("question_type", "unknown")
        conv.question_type_counts[qtype] = conv.question_type_counts.get(qtype, 0) + 1

        is_abstention = qid.endswith("_abs") or qtype == "false_premise"

        session_ids = item["haystack_session_ids"]
        dates = item["haystack_dates"]
        sessions = item["haystack_sessions"]

        for sid, date, turns in zip(session_ids, dates, sessions):
            if sid in conv.corpus or not (full_haystack or sid in needed_sids):
                continue
            event_time = normalize_event_time(date)
            conv.corpus[sid] = {
                "_id": sid,
                "title": f"Chat Session - {date}",
                "text": serialize_session(turns, date),
                "metadata": {"event_time": event_time, "session_id": sid},
            }
            conv.session_count += 1
            if round_docs:
                for index, round_turns in enumerate(session_rounds(turns)):
                    rid = round_doc_id(sid, index)
                    conv.corpus[rid] = {
                        "_id": rid,
                        "title": f"Chat Session - {date} - round {index}",
                        "text": serialize_round(round_turns, date),
                        "metadata": {
                            "event_time": event_time,
                            "session_id": sid,
                            "round_index": index,
                        },
                    }
                    conv.round_doc_count += 1

        if full_haystack:
            conv.haystacks.append(
                {
                    "question_id": qid,
                    "haystack_session_ids": list(session_ids),
                    "haystack_dates": [normalize_event_time(d) for d in dates],
                }
            )

        conv.queries.append(
            {"_id": qid, "text": item["question"], "metadata": {"question_type": qtype}}
        )

        answer_sids = item.get("answer_session_ids", [])
        if is_abstention:
            conv.abstention_count += 1
            if include_abstention:
                for sid in answer_sids:
                    conv.qrels.append((qid, sid, 0))
        else:
            for sid in answer_sids:
                conv.qrels.append((qid, sid, 1))

    return conv


def write_outputs(out: Path, conv: Conversion, *, include_abstention, full_haystack, round_docs):
    out.mkdir(parents=True, exist_ok=True)
    (out / "qrels").mkdir(exist_ok=True)

    with open(out / "corpus.jsonl", "w") as f:
        f.writelines(json.dumps(doc, ensure_ascii=False) + "\n" for doc in conv.corpus.values())

    with open(out / "queries.jsonl", "w") as f:
        f.writelines(json.dumps(q, ensure_ascii=False) + "\n" for q in conv.queries)

    with open(out / "qrels" / "test.tsv", "w") as f:
        f.write("query-id\tcorpus-id\tscore\n")
        f.writelines(f"{qid}\t{did}\t{score}\n" for qid, did, score in conv.qrels)

    if full_haystack:
        with open(out / "haystacks.jsonl", "w") as f:
            f.writelines(json.dumps(entry, ensure_ascii=False) + "\n" for entry in conv.haystacks)

    meta = {
        "dataset": "LongMemEval_S",
        "source": "xiaowu0162/longmemeval-cleaned",
        "split": "longmemeval_s_cleaned",
        "total_questions": len(conv.queries),
        "total_sessions": conv.session_count,
        "total_documents": len(conv.corpus),
        "total_round_documents": conv.round_doc_count,
        "total_qrels": len(conv.qrels),
        "abstention_questions": conv.abstention_count,
        "include_abstention": include_abstention,
        "full_haystack": full_haystack,
        "round_docs": round_docs,
        "question_type_counts": conv.question_type_counts,
    }
    with open(out / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)
    return meta


def _synthetic_records():
    """Three sessions shared across two questions, plus an abstention question."""
    s1 = [
        {"role": "user", "content": "I adopted a dog named Pepper."},
        {"role": "assistant", "content": "Congratulations on Pepper!"},
        {"role": "user", "content": "She likes carrots."},
        {"role": "assistant", "content": "Many dogs do."},
    ]
    s2 = [
        json.dumps({"role": "user", "content": "Planning a trip to Lisbon in June."}),
        json.dumps({"role": "assistant", "content": "Lisbon is warm in June."}),
    ]
    s3 = [
        {"role": "assistant", "content": "Reminder: dentist on Friday."},
        {"role": "user", "content": "Thanks."},
    ]
    return [
        {
            "question_id": "q1",
            "question_type": "single-session-user",
            "question": "What is the name of my dog?",
            "haystack_session_ids": ["s1", "s2"],
            "haystack_dates": ["2023/05/20 (Sat) 02:21", "2023/05/21 (Sun) 10:00"],
            "haystack_sessions": [s1, s2],
            "answer_session_ids": ["s1"],
        },
        {
            "question_id": "q2",
            "question_type": "temporal-reasoning",
            "question": "Where am I travelling in June?",
            "haystack_session_ids": ["s2", "s3"],
            "haystack_dates": ["2023/05/21 (Sun) 10:00", "2023/06/01 (Thu) 08:30"],
            "haystack_sessions": [s2, s3],
            "answer_session_ids": ["s2"],
        },
        {
            "question_id": "q3_abs",
            "question_type": "false_premise",
            "question": "When did I sell my boat?",
            "haystack_session_ids": ["s3"],
            "haystack_dates": ["2023/06/01 (Thu) 08:30"],
            "haystack_sessions": [s3],
            "answer_session_ids": ["s3"],
        },
    ]


def self_test() -> int:
    """Exercise the converter on a synthetic fixture; no dataset download."""
    assert normalize_event_time("2023/05/20 (Sat) 02:21") == "2023-05-20T02:21:00"
    assert normalize_event_time("2023/05/20") == "2023-05-20"
    assert normalize_event_time("2023-05-20T02:21:00Z") == "2023-05-20T02:21:00Z"
    assert normalize_event_time("someday") == "someday"

    records = _synthetic_records()

    # Default: answer sessions only (abstention answers included, as before), session-level
    # docs, metadata on every doc, no sidecar.
    default = convert(records)
    assert set(default.corpus) == {"s1", "s2", "s3"}, default.corpus.keys()
    assert default.session_count == 3
    assert default.corpus["s1"]["title"] == "Chat Session - 2023/05/20 (Sat) 02:21"
    assert default.corpus["s1"]["text"].startswith("[2023/05/20 (Sat) 02:21]\nUser: I adopted")
    assert default.corpus["s1"]["metadata"] == {
        "event_time": "2023-05-20T02:21:00",
        "session_id": "s1",
    }
    assert default.corpus["s2"]["metadata"]["event_time"] == "2023-05-21T10:00:00"
    assert [q["_id"] for q in default.queries] == ["q1", "q2", "q3_abs"]
    assert default.qrels == [("q1", "s1", 1), ("q2", "s2", 1)]
    assert default.abstention_count == 1
    assert default.haystacks == []
    assert default.round_doc_count == 0

    with_abs = convert(records, include_abstention=True)
    assert ("q3_abs", "s3", 0) in with_abs.qrels
    assert with_abs.corpus == default.corpus

    # Full haystack: every session, per-question membership and dates kept.
    full = convert(records, full_haystack=True)
    assert set(full.corpus) == {"s1", "s2", "s3"}
    assert full.corpus == default.corpus  # the fixture's haystacks are all answers
    assert full.haystacks == [
        {
            "question_id": "q1",
            "haystack_session_ids": ["s1", "s2"],
            "haystack_dates": ["2023-05-20T02:21:00", "2023-05-21T10:00:00"],
        },
        {
            "question_id": "q2",
            "haystack_session_ids": ["s2", "s3"],
            "haystack_dates": ["2023-05-21T10:00:00", "2023-06-01T08:30:00"],
        },
        {
            "question_id": "q3_abs",
            "haystack_session_ids": ["s3"],
            "haystack_dates": ["2023-06-01T08:30:00"],
        },
    ]

    # Round docs: one per user-opened round, session docs unchanged.
    rounds = convert(records, full_haystack=True, round_docs=True)
    assert rounds.session_count == 3
    # s1: 2 user rounds; s2: 1; s3: a leading assistant turn is its own round, then 1 user round.
    assert rounds.round_doc_count == 5, rounds.round_doc_count
    assert rounds.corpus["s1"] == full.corpus["s1"]
    r0 = rounds.corpus["s1__r0"]
    assert r0["text"] == (
        "[2023/05/20 (Sat) 02:21]\nUser: I adopted a dog named Pepper.\n"
        "Assistant: Congratulations on Pepper!"
    )
    assert r0["metadata"] == {
        "event_time": "2023-05-20T02:21:00",
        "session_id": "s1",
        "round_index": 0,
    }
    assert rounds.corpus["s1__r1"]["metadata"]["round_index"] == 1
    assert rounds.corpus["s3__r0"]["text"].endswith("]\nAssistant: Reminder: dentist on Friday.")
    assert rounds.corpus["s3__r1"]["text"].endswith("]\nUser: Thanks.")
    assert "s3__r2" not in rounds.corpus
    assert rounds.qrels == default.qrels  # qrels stay at session level

    # Files: default layout has no sidecar; full haystack writes it.
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "default"
        meta = write_outputs(
            out, default, include_abstention=False, full_haystack=False, round_docs=False
        )
        assert sorted(p.name for p in out.iterdir()) == [
            "corpus.jsonl",
            "metadata.json",
            "qrels",
            "queries.jsonl",
        ]
        lines = (out / "corpus.jsonl").read_text().splitlines()
        assert [json.loads(line)["_id"] for line in lines] == ["s1", "s2", "s3"]
        assert json.loads(lines[0])["metadata"]["session_id"] == "s1"
        assert (out / "qrels" / "test.tsv").read_text() == (
            "query-id\tcorpus-id\tscore\nq1\ts1\t1\nq2\ts2\t1\n"
        )
        assert meta["total_sessions"] == 3 and meta["total_round_documents"] == 0

        out_full = Path(tmp) / "full"
        meta_full = write_outputs(
            out_full, rounds, include_abstention=False, full_haystack=True, round_docs=True
        )
        side = [
            json.loads(line) for line in (out_full / "haystacks.jsonl").read_text().splitlines()
        ]
        assert [e["question_id"] for e in side] == ["q1", "q2", "q3_abs"]
        assert side[1]["haystack_session_ids"] == ["s2", "s3"]
        assert meta_full["total_documents"] == 8 and meta_full["round_docs"] is True

    print("self-test passed")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / ".cache" / "yams" / "benchmarks" / "longmemeval_s",
        help="Output directory for BEIR-format files",
    )
    parser.add_argument(
        "--max-questions",
        type=int,
        default=None,
        help="Limit number of questions (for quick iteration)",
    )
    parser.add_argument(
        "--include-abstention",
        action="store_true",
        help="Include false-premise (abstention) questions in qrels with score=0",
    )
    parser.add_argument(
        "--full-haystack",
        action="store_true",
        help="Include all haystack sessions in corpus (default: only qrel-referenced) and "
        "write haystacks.jsonl with each question's haystack session ids and dates",
    )
    parser.add_argument(
        "--round-docs",
        action="store_true",
        help="Also emit one document per session round (<session>__r<n>) with round_index metadata",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run the converter on a synthetic fixture and exit (no download)",
    )
    args = parser.parse_args()

    if args.self_test:
        sys.exit(self_test())

    try:
        from datasets import load_dataset
    except ImportError:
        print(
            "Error: 'datasets' package required.\n"
            "  uv pip install datasets\n"
            "Or run with:\n"
            "  uv run --with datasets scripts/prepare_longmemeval_s.py",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Loading LongMemEval_S from HuggingFace...")
    # Stream to avoid downloading all splits (the _m split is >2GB and
    # overflows pyarrow's int32 block size).
    ds = load_dataset(
        "xiaowu0162/longmemeval-cleaned",
        split="longmemeval_s_cleaned",
        streaming=True,
    )
    # Materialize the streamed dataset into a list
    ds = list(ds)

    if args.max_questions and args.max_questions < len(ds):
        ds = ds[: args.max_questions]
        print(f"Subsampled to {len(ds)} questions")

    conv = convert(
        ds,
        full_haystack=args.full_haystack,
        include_abstention=args.include_abstention,
        round_docs=args.round_docs,
    )
    if not args.full_haystack:
        print(f"Qrel-referenced sessions: {conv.session_count} (use --full-haystack for all)")

    out = args.output_dir
    write_outputs(
        out,
        conv,
        include_abstention=args.include_abstention,
        full_haystack=args.full_haystack,
        round_docs=args.round_docs,
    )

    print(f"\nDone! Output: {out}")
    print(f"  Corpus:  {conv.session_count} sessions, {len(conv.corpus)} documents")
    if args.round_docs:
        print(f"  Rounds:  {conv.round_doc_count} round documents")
    print(f"  Queries: {len(conv.queries)} questions")
    print(f"  Qrels:   {len(conv.qrels)} judgments")
    print(f"  Abstention (excluded): {conv.abstention_count}")
    print(f"  Question types: {conv.question_type_counts}")
    if args.full_haystack:
        print(f"  Haystacks: {out / 'haystacks.jsonl'}")
    print(
        "\nRun benchmark:\n"
        "  YAMS_TEST_SAFE_SINGLE_INSTANCE=1 YAMS_BENCH_DATASET=longmemeval_s "
        "./builddir/tests/benchmarks/retrieval_quality_bench"
    )


if __name__ == "__main__":
    main()
