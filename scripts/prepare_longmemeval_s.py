#!/usr/bin/env python3
"""Convert LongMemEval_S from HuggingFace to BEIR format for yams benchmarking.

Downloads the cleaned LongMemEval_S dataset and converts it to BEIR format
(corpus.jsonl, queries.jsonl, qrels/test.tsv) so the existing
retrieval_quality_bench can consume it directly.

Usage:
    uv run --with datasets scripts/prepare_longmemeval_s.py
    uv run --with datasets scripts/prepare_longmemeval_s.py --max-questions 50

Output: ~/.cache/yams/benchmarks/longmemeval_s/
"""

import argparse
import json
import os
import sys
from pathlib import Path


def serialize_session(turns: list, date: str) -> str:
    """Serialize a chat session into searchable text.

    Each turn may be a dict or a JSON string encoding {role, content}.
    """
    lines = [f"[{date}]"]
    for turn in turns:
        if isinstance(turn, str):
            turn = json.loads(turn)
        role = turn.get("role", "unknown")
        content = turn.get("content", "")
        label = "User" if role == "user" else "Assistant"
        lines.append(f"{label}: {content}")
    return "\n".join(lines)


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
        help="Include all haystack sessions in corpus (default: only qrel-referenced)",
    )
    args = parser.parse_args()

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

    # First pass: collect qrel-referenced session IDs so we can filter corpus
    needed_sids = set()  # session IDs referenced by answer_session_ids
    if not args.full_haystack:
        for item in ds:
            for sid in item.get("answer_session_ids", []):
                needed_sids.add(sid)
        print(f"Qrel-referenced sessions: {len(needed_sids)} (use --full-haystack for all)")

    # Build corpus: one document per unique session, deduplicated across questions
    corpus = {}  # session_id -> {_id, title, text}
    queries = []  # [{_id, text, metadata}]
    qrels = []  # [(query_id, session_id, score)]
    abstention_count = 0
    question_type_counts = {}

    for item in ds:
        qid = item["question_id"]
        qtype = item.get("question_type", "unknown")
        question_type_counts[qtype] = question_type_counts.get(qtype, 0) + 1

        is_abstention = qid.endswith("_abs") or qtype == "false_premise"

        # Collect sessions into corpus
        session_ids = item["haystack_session_ids"]
        dates = item["haystack_dates"]
        sessions = item["haystack_sessions"]

        for sid, date, turns in zip(session_ids, dates, sessions):
            if sid not in corpus and (args.full_haystack or sid in needed_sids):
                corpus[sid] = {
                    "_id": sid,
                    "title": f"Chat Session - {date}",
                    "text": serialize_session(turns, date),
                }

        # Build query
        queries.append(
            {
                "_id": qid,
                "text": item["question"],
                "metadata": {"question_type": qtype},
            }
        )

        # Build qrels
        answer_sids = item.get("answer_session_ids", [])
        if is_abstention:
            abstention_count += 1
            if args.include_abstention:
                for sid in answer_sids:
                    qrels.append((qid, sid, 0))
        else:
            for sid in answer_sids:
                qrels.append((qid, sid, 1))

    # Write output
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    (out / "qrels").mkdir(exist_ok=True)

    # corpus.jsonl
    with open(out / "corpus.jsonl", "w") as f:
        for doc in corpus.values():
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    # queries.jsonl
    with open(out / "queries.jsonl", "w") as f:
        for q in queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")

    # qrels/test.tsv
    with open(out / "qrels" / "test.tsv", "w") as f:
        f.write("query-id\tcorpus-id\tscore\n")
        for qid, did, score in qrels:
            f.write(f"{qid}\t{did}\t{score}\n")

    # metadata.json
    meta = {
        "dataset": "LongMemEval_S",
        "source": "xiaowu0162/longmemeval-cleaned",
        "split": "longmemeval_s_cleaned",
        "total_questions": len(queries),
        "total_sessions": len(corpus),
        "total_qrels": len(qrels),
        "abstention_questions": abstention_count,
        "include_abstention": args.include_abstention,
        "question_type_counts": question_type_counts,
    }
    with open(out / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\nDone! Output: {out}")
    print(f"  Corpus:  {len(corpus)} sessions")
    print(f"  Queries: {len(queries)} questions")
    print(f"  Qrels:   {len(qrels)} judgments")
    print(f"  Abstention (excluded): {abstention_count}")
    print(f"  Question types: {question_type_counts}")
    print(
        f"\nRun benchmark:\n"
        f"  YAMS_TEST_SAFE_SINGLE_INSTANCE=1 YAMS_BENCH_DATASET=longmemeval_s "
        f"./builddir/tests/benchmarks/retrieval_quality_bench"
    )


if __name__ == "__main__":
    main()
