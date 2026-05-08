#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "numpy",
#   "scikit-learn",
#   "sentence-transformers",
# ]
# ///
"""
R1 — Cluster Hypothesis Empirical Check (Phase V Gap)

The cluster hypothesis (van Rijsbergen 1979) states that documents relevant to
the same query tend to cluster together under a useful similarity measure. All
of YAMS's routing/clustering work (Phases P/R/S/U/V) implicitly depends on this
holding for our corpora. Five phases of negative results suggest it might not.

This script tests the hypothesis empirically without touching YAMS:

  For each query with ≥2 relevant docs in qrels:
    intra = mean pairwise cosine across the relevant set
    rand  = mean pairwise cosine across a same-size random subset
    lift  = intra / rand

  Aggregate the lift across queries. Lift >> 1 → hypothesis holds; lift ≈ 1 →
  relevant docs are no more similar to each other than random docs are to each
  other, and routing-via-clusters cannot work in principle.

Embedding: sentence-transformers/all-MiniLM-L6-v2 (fast, well-known, semantic).
If the hypothesis fails under a SOTA semantic embedding, it will certainly fail
under YAMS's Simeon FWHT projection — the test is intentionally generous.

Usage:
  python scripts/cluster_hypothesis_check.py \
      --beir-dir ~/.cache/yams/benchmarks/scifact \
      --max-queries 0    # 0 = all
"""

import argparse
import json
import random
import sys
from pathlib import Path

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


def load_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def load_qrels(qrels_dir):
    qrels = {}
    test = qrels_dir / "test.tsv"
    if not test.exists():
        test = qrels_dir / "qrels-test.tsv"
    if not test.exists():
        candidates = list(qrels_dir.glob("*.tsv"))
        if not candidates:
            raise FileNotFoundError(f"no .tsv qrels found in {qrels_dir}")
        test = candidates[0]
    with open(test, encoding="utf-8") as f:
        next(f, None)  # header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 3:
                continue
            qid, did, score = parts[0], parts[1], parts[2]
            try:
                if int(score) <= 0:
                    continue
            except ValueError:
                continue
            qrels.setdefault(qid, []).append(did)
    return qrels


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--beir-dir", required=True, type=Path)
    ap.add_argument("--max-queries", type=int, default=0,
                    help="0 = all queries with ≥2 relevant docs")
    ap.add_argument("--max-corpus", type=int, default=0,
                    help="0 = all docs; otherwise random subsample for embedding speed")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")
    args = ap.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)

    corpus_path = args.beir_dir / "corpus.jsonl"
    queries_path = args.beir_dir / "queries.jsonl"
    qrels_dir = args.beir_dir / "qrels"
    if not corpus_path.exists() or not queries_path.exists() or not qrels_dir.exists():
        print(f"missing scifact files in {args.beir_dir}", file=sys.stderr)
        sys.exit(2)

    print(f"[r1] loading corpus from {corpus_path}", flush=True)
    corpus_id_to_text = {}
    for r in load_jsonl(corpus_path):
        did = str(r.get("_id") or r.get("id"))
        title = r.get("title", "") or ""
        text = r.get("text", "") or ""
        corpus_id_to_text[did] = (title + " " + text).strip()

    print(f"[r1] loading queries from {queries_path}", flush=True)
    query_id_to_text = {}
    for r in load_jsonl(queries_path):
        qid = str(r.get("_id") or r.get("id"))
        query_id_to_text[qid] = (r.get("text", "") or "").strip()

    print(f"[r1] loading qrels from {qrels_dir}", flush=True)
    qrels = load_qrels(qrels_dir)
    eligible = {qid: dids for qid, dids in qrels.items() if len(dids) >= 2}
    print(f"[r1] {len(eligible)} queries with ≥2 relevant docs (of {len(qrels)} total)",
          flush=True)
    if args.max_queries > 0:
        eligible = dict(list(eligible.items())[: args.max_queries])
        print(f"[r1] limited to {len(eligible)} queries", flush=True)

    # Collect doc ids we need: union of relevant + sampled-random-pool docs.
    needed_ids = set()
    for dids in eligible.values():
        needed_ids.update(dids)
    all_corpus_ids = list(corpus_id_to_text.keys())
    rng = random.Random(args.seed)
    pool_size = max(2 * len(needed_ids), 2000)
    if pool_size > len(all_corpus_ids):
        pool_size = len(all_corpus_ids)
    random_pool_ids = rng.sample(all_corpus_ids, pool_size)
    needed_ids.update(random_pool_ids)
    needed_ids = [d for d in needed_ids if d in corpus_id_to_text]
    print(f"[r1] embedding {len(needed_ids)} docs (relevant + random pool)", flush=True)

    print(f"[r1] loading model {args.model}", flush=True)
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(args.model)

    texts = [corpus_id_to_text[d] for d in needed_ids]
    print(f"[r1] encoding {len(texts)} doc texts...", flush=True)
    emb = model.encode(texts, batch_size=64, show_progress_bar=True,
                       convert_to_numpy=True, normalize_embeddings=True)
    id_to_emb = {d: emb[i] for i, d in enumerate(needed_ids)}

    def mean_pairwise_cos(ids):
        vs = np.stack([id_to_emb[d] for d in ids if d in id_to_emb])
        if vs.shape[0] < 2:
            return None
        sim = cosine_similarity(vs)
        upper = sim[np.triu_indices_from(sim, k=1)]
        return float(np.mean(upper))

    intras = []
    rands = []
    lifts = []
    skipped = 0
    for qid, rel_ids in eligible.items():
        rel_ids = [d for d in rel_ids if d in id_to_emb]
        if len(rel_ids) < 2:
            skipped += 1
            continue
        intra = mean_pairwise_cos(rel_ids)
        # Same-size random sample, exclude relevant docs from the pool.
        pool_excl = [d for d in random_pool_ids if d not in set(rel_ids) and d in id_to_emb]
        rand_ids = rng.sample(pool_excl, min(len(rel_ids), len(pool_excl)))
        rand = mean_pairwise_cos(rand_ids)
        if intra is None or rand is None:
            skipped += 1
            continue
        intras.append(intra)
        rands.append(rand)
        lifts.append(intra / rand if rand != 0 else None)

    print()
    print("=== R1 — Cluster Hypothesis Empirical Check ===")
    print(f"corpus dir:           {args.beir_dir}")
    print(f"embedding model:      {args.model}")
    print(f"queries evaluated:    {len(intras)}")
    print(f"skipped (not enough): {skipped}")
    print(f"mean intra-relevant cosine:   {np.mean(intras):.4f}")
    print(f"mean random-baseline cosine:  {np.mean(rands):.4f}")
    if any(v is not None for v in lifts):
        ll = [v for v in lifts if v is not None]
        print(f"mean intra/random lift:       {np.mean(ll):.3f}")
        print(f"median intra/random lift:     {np.median(ll):.3f}")
        n_above_1 = sum(1 for v in ll if v > 1.0)
        print(f"queries with lift > 1:        {n_above_1}/{len(ll)} "
              f"({100.0 * n_above_1 / len(ll):.1f}%)")
    print()
    print("Interpretation:")
    print("  lift >> 1 (e.g., 2x+):  cluster hypothesis holds → routing/clusters viable")
    print("  lift ≈ 1.2 - 1.5:       weak signal; routing wins likely small")
    print("  lift ≈ 1:               hypothesis empirically fails for this corpus+embedding")


if __name__ == "__main__":
    main()
