#!/usr/bin/env python3
"""Phase 0 report for the simeon adaptivity matrix.

Reads JSONL cells produced by `run_simeon_adaptivity_matrix.sh` and emits:
  * Per-corpus pivot tables: backend x alpha vs MRR / Recall@10 / nDCG@10.
  * Fusion-contribution summary (dense_contrib vs bm25_contrib).
  * Promotion-bar verdicts for the candidate levers defined in the plan
    (docs/plans tracked under .claude/plans/).

Usage (with uv):
  uv run tests/scripts/analyze_simeon_adaptivity.py \
      --matrix-dir bench_results/simeon_adaptivity/<run-id> \
      --output docs/benchmarks/simeon_adaptivity_report.md
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


@dataclass
class Cell:
    backend: str
    alpha: float
    corpus: str
    path: Path
    rows: list[dict[str, Any]] = field(default_factory=list)

    @property
    def successful(self) -> list[dict[str, Any]]:
        return [r for r in self.rows if _is_success(r)]


def _is_success(row: dict[str, Any]) -> bool:
    if isinstance(row.get("success"), bool):
        return bool(row.get("success"))
    return str(row.get("status", "")).strip().lower() == "ok"


def _get(row: dict[str, Any], path: Iterable[str], default: float = 0.0) -> float:
    cur: Any = row
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    try:
        val = float(cur)
        if math.isnan(val) or math.isinf(val):
            return default
        return val
    except (TypeError, ValueError):
        return default


def _mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _parse_cell_path(matrix_dir: Path, jsonl_path: Path) -> tuple[str, float, str] | None:
    try:
        rel = jsonl_path.relative_to(matrix_dir)
    except ValueError:
        return None
    parts = rel.parts
    if len(parts) < 4 or parts[-1] != "bench.jsonl":
        return None
    backend = parts[0]
    alpha_seg = parts[1]
    corpus = parts[2]
    if not alpha_seg.startswith("alpha_"):
        return None
    try:
        alpha = float(alpha_seg.removeprefix("alpha_"))
    except ValueError:
        return None
    return backend, alpha, corpus


def load_cells(matrix_dir: Path) -> list[Cell]:
    cells: list[Cell] = []
    for jsonl in sorted(matrix_dir.rglob("bench.jsonl")):
        parsed = _parse_cell_path(matrix_dir, jsonl)
        if not parsed:
            continue
        backend, alpha, corpus = parsed
        rows: list[dict[str, Any]] = []
        with jsonl.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    print(
                        f"warn: {jsonl}:{line_no}: invalid JSON ({exc}); skipping",
                        flush=True,
                    )
        cells.append(Cell(backend=backend, alpha=alpha, corpus=corpus, path=jsonl, rows=rows))
    return cells


def summarize_cell(cell: Cell) -> dict[str, Any]:
    rows = cell.successful
    return {
        "backend": cell.backend,
        "alpha": cell.alpha,
        "corpus": cell.corpus,
        "path": str(cell.path),
        "n_rows": len(cell.rows),
        "n_success": len(rows),
        "hybrid_mrr": _mean([_get(r, ["hybrid", "mrr"]) for r in rows]),
        "hybrid_recall_at_k": _mean(
            [_get(r, ["hybrid", "recall_at_k"]) for r in rows]
        ),
        "hybrid_ndcg_at_k": _mean([_get(r, ["hybrid", "ndcg_at_k"]) for r in rows]),
        "keyword_mrr": _mean([_get(r, ["keyword", "mrr"]) for r in rows]),
        "keyword_recall_at_k": _mean(
            [_get(r, ["keyword", "recall_at_k"]) for r in rows]
        ),
        "fusion_alpha": _mean([_get(r, ["fusion_alpha"]) for r in rows]),
        "dense_contrib": _mean([_get(r, ["dense_contrib"]) for r in rows]),
        "bm25_contrib": _mean([_get(r, ["bm25_contrib"]) for r in rows]),
        "other_contrib": _mean([_get(r, ["other_contrib"]) for r in rows]),
    }


def pivot_by_corpus(
    summaries: list[dict[str, Any]],
) -> dict[str, dict[tuple[str, float], dict[str, Any]]]:
    pivot: dict[str, dict[tuple[str, float], dict[str, Any]]] = {}
    for s in summaries:
        pivot.setdefault(s["corpus"], {})[(s["backend"], s["alpha"])] = s
    return pivot


def _fmt(val: float, digits: int = 4) -> str:
    return "n/a" if val is None else f"{val:.{digits}f}"


def render_pivot_tables(
    pivot: dict[str, dict[tuple[str, float], dict[str, Any]]],
) -> list[str]:
    lines: list[str] = []
    for corpus in sorted(pivot.keys()):
        lines.append(f"### corpus = `{corpus}`")
        lines.append("")
        lines.append(
            "| backend | α (vector_weight) | rows | MRR (hybrid) | Recall@10 (hybrid) | nDCG@10 (hybrid) | dense_contrib | bm25_contrib |"
        )
        lines.append(
            "| --- | --- | --- | --- | --- | --- | --- | --- |"
        )
        cells = pivot[corpus]
        for key in sorted(cells.keys(), key=lambda k: (k[0], k[1])):
            s = cells[key]
            lines.append(
                "| {backend} | {alpha} | {n} | {mrr} | {recall} | {ndcg} | {dense} | {bm25} |".format(
                    backend=s["backend"],
                    alpha=_fmt(s["alpha"], 2),
                    n=s["n_success"],
                    mrr=_fmt(s["hybrid_mrr"]),
                    recall=_fmt(s["hybrid_recall_at_k"]),
                    ndcg=_fmt(s["hybrid_ndcg_at_k"]),
                    dense=_fmt(s["dense_contrib"], 3),
                    bm25=_fmt(s["bm25_contrib"], 3),
                )
            )
        lines.append("")
    return lines


def evaluate_p0_hybrid_alpha(
    pivot: dict[str, dict[tuple[str, float], dict[str, Any]]],
) -> tuple[bool, list[str]]:
    """P0: some α ≠ default improves MRR by ≥ +0.03 on at least one corpus
    without regressing the others > 0.01."""
    notes: list[str] = []
    improvements: list[tuple[str, float, float, float]] = []
    regressions: list[tuple[str, float, float, float]] = []

    for corpus, cells in pivot.items():
        simeon_cells = {
            alpha: s for (backend, alpha), s in cells.items() if backend == "simeon"
        }
        if not simeon_cells:
            continue
        baseline_alpha = min(simeon_cells.keys(), key=lambda a: abs(a - 0.5))
        baseline_mrr = simeon_cells[baseline_alpha]["hybrid_mrr"]
        for alpha, s in simeon_cells.items():
            if alpha == baseline_alpha:
                continue
            delta = s["hybrid_mrr"] - baseline_mrr
            if delta >= 0.03:
                improvements.append((corpus, alpha, delta, s["hybrid_mrr"]))
            elif delta <= -0.01:
                regressions.append((corpus, alpha, delta, s["hybrid_mrr"]))

    if not improvements:
        notes.append("No α sweep improved hybrid MRR by ≥ +0.03 on any corpus.")
        return False, notes

    winning_alphas = {alpha for _, alpha, _, _ in improvements}
    disqualifying = [r for r in regressions if r[1] in winning_alphas]

    for corpus, alpha, delta, mrr in improvements:
        notes.append(
            f"`{corpus}` α={alpha:.2f}: +{delta:.3f} MRR vs. α≈0.5 baseline (MRR={mrr:.4f})"
        )
    for corpus, alpha, delta, mrr in disqualifying:
        notes.append(
            f"regression: `{corpus}` α={alpha:.2f}: Δ={delta:.3f} MRR (MRR={mrr:.4f})"
        )

    passed = bool(improvements) and not disqualifying
    return passed, notes


def evaluate_p1_idf(
    summaries: list[dict[str, Any]],
) -> tuple[bool, list[str]]:
    """P1 IDF bar: simeon's dense_contrib is consistently swamped, i.e.,
    bm25_contrib >> dense_contrib on prose corpora (proxy for head-term
    dominance). We cannot compute per-token energy from the current JSONL;
    flag this as a proxy check and note explicit follow-up."""
    notes: list[str] = []
    simeon = [s for s in summaries if s["backend"] == "simeon"]
    if not simeon:
        notes.append("No simeon cells — cannot evaluate P1.")
        return False, notes

    suspicious = [
        s
        for s in simeon
        if s["dense_contrib"] < 0.1 and s["bm25_contrib"] > 0.8
    ]
    for s in suspicious:
        notes.append(
            f"`{s['corpus']}` α={s['alpha']:.2f}: dense_contrib={s['dense_contrib']:.3f} "
            f"bm25_contrib={s['bm25_contrib']:.3f} — simeon not contributing to fused rank"
        )
    if not suspicious:
        notes.append(
            "No cells show dense_contrib<0.1 with bm25_contrib>0.8; head-term dominance "
            "proxy not triggered."
        )
        return False, notes

    notes.append(
        "Proxy triggered. Full P1 bar (top-5 tokens >30% of sketch energy) requires "
        "per-token energy emission — follow-up item."
    )
    return True, notes


def evaluate_p2_intent(summaries: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    """P2: needs `query_intent_bucket` column; skip until present."""
    return False, [
        "Skipped: `query_intent_bucket` not yet emitted by bench JSONL; see Phase 0.1 follow-up.",
    ]


def evaluate_p3_rerank(cells: list[Cell]) -> tuple[bool, list[str]]:
    """P3: deferred by plan."""
    return False, [
        "Deferred: P3 learned reranker waits on P0/P1 results and RelevanceLabelStore volume."
    ]


def render_promotion_bars(
    pivot: dict[str, dict[tuple[str, float], dict[str, Any]]],
    summaries: list[dict[str, Any]],
    cells: list[Cell],
) -> list[str]:
    lines: list[str] = []
    lines.append("## Promotion bars")
    lines.append("")
    lines.append("| Lever | Verdict | Notes |")
    lines.append("| --- | --- | --- |")

    passed_p0, notes_p0 = evaluate_p0_hybrid_alpha(pivot)
    passed_p1, notes_p1 = evaluate_p1_idf(summaries)
    passed_p2, notes_p2 = evaluate_p2_intent(summaries)
    passed_p3, notes_p3 = evaluate_p3_rerank(cells)

    def _verdict(ok: bool) -> str:
        return "PASS" if ok else "FAIL"

    def _fold_notes(xs: list[str]) -> str:
        return "<br>".join(xs) if xs else "—"

    lines.append(
        f"| P0 hybrid α | {_verdict(passed_p0)} | {_fold_notes(notes_p0)} |"
    )
    lines.append(f"| P1 IDF | {_verdict(passed_p1)} | {_fold_notes(notes_p1)} |")
    lines.append(f"| P2 query-intent n-gram | SKIP | {_fold_notes(notes_p2)} |")
    lines.append(f"| P3 learned rerank | SKIP | {_fold_notes(notes_p3)} |")
    lines.append("")
    return lines


def build_report(matrix_dir: Path, cells: list[Cell]) -> str:
    summaries = [summarize_cell(c) for c in cells]
    pivot = pivot_by_corpus(summaries)

    skipped_log = matrix_dir / "skipped_cells.log"
    skipped_lines: list[str] = []
    if skipped_log.exists():
        skipped_lines = [
            ln.strip() for ln in skipped_log.read_text().splitlines() if ln.strip()
        ]

    manifest_path = matrix_dir / "matrix_manifest.env"
    manifest_kv: dict[str, str] = {}
    if manifest_path.exists():
        for line in manifest_path.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                manifest_kv[k.strip()] = v.strip()

    lines: list[str] = []
    lines.append("# Simeon adaptivity — Phase 0 report")
    lines.append("")
    lines.append(f"Matrix dir: `{matrix_dir}`")
    lines.append("")
    if manifest_kv:
        lines.append("## Manifest")
        lines.append("")
        for k in sorted(manifest_kv.keys()):
            lines.append(f"- `{k}` = `{manifest_kv[k]}`")
        lines.append("")
    lines.append("## Pivot tables")
    lines.append("")
    lines.extend(render_pivot_tables(pivot))
    lines.extend(render_promotion_bars(pivot, summaries, cells))
    if skipped_lines:
        lines.append("## Skipped cells")
        lines.append("")
        for ln in skipped_lines:
            lines.append(f"- {ln}")
        lines.append("")
    lines.append("## Raw cell summaries")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps(summaries, indent=2, sort_keys=True))
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--matrix-dir",
        required=True,
        help="Directory produced by run_simeon_adaptivity_matrix.sh",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Destination Markdown report path",
    )
    parser.add_argument(
        "--json-out",
        default=None,
        help="Optional JSON summary dump (cell-level summaries)",
    )
    args = parser.parse_args()

    matrix_dir = Path(args.matrix_dir).resolve()
    output = Path(args.output).resolve()
    if not matrix_dir.is_dir():
        print(f"error: matrix dir not found: {matrix_dir}", flush=True)
        return 1

    cells = load_cells(matrix_dir)
    if not cells:
        print(f"error: no bench.jsonl cells found under {matrix_dir}", flush=True)
        return 2

    report = build_report(matrix_dir, cells)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")
    print(f"wrote report: {output}")

    if args.json_out:
        json_path = Path(args.json_out).resolve()
        json_path.parent.mkdir(parents=True, exist_ok=True)
        summaries = [summarize_cell(c) for c in cells]
        json_path.write_text(json.dumps(summaries, indent=2, sort_keys=True))
        print(f"wrote json summary: {json_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
