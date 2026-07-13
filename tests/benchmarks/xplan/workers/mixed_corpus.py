"""Materialize and score heterogeneous BEIR corpora in one YAMS index."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import quote


@dataclass(frozen=True)
class PreparedMixedCorpus:
    manifest_path: Path
    identity_path: Path
    document_count: int
    query_count: int
    sources: tuple[str, ...]


def _source_key(value: str) -> str:
    key = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    if not key:
        raise ValueError("mixed corpus source names must contain a letter or digit")
    return key


def _namespaced_id(source: str, raw_id: str) -> str:
    return f"{source}__{quote(raw_id, safe='-_.')}"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"mixed corpus input is missing {path}")
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"expected an object at {path}:{line_number}")
        rows.append(value)
    return rows


def _raw_id(row: dict[str, Any]) -> str:
    return str(row.get("_id", row.get("id", ""))).strip()


def _read_qrels(path: Path) -> list[tuple[str, str, int]]:
    if not path.is_file():
        raise ValueError(f"mixed corpus input is missing {path}")
    rows: list[tuple[str, str, int]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split("\t")
        if line_number == 1 and fields[0].lower() in {"query-id", "query_id"}:
            continue
        if len(fields) < 3:
            raise ValueError(f"invalid qrel at {path}:{line_number}")
        try:
            score = int(fields[2])
        except ValueError as exc:
            raise ValueError(f"invalid qrel score at {path}:{line_number}") from exc
        if score > 0:
            rows.append((fields[0], fields[1], score))
    return rows


def _rendered_content(document: dict[str, Any]) -> str:
    title = str(document.get("title", ""))
    text = str(document.get("text", ""))
    return f"{title}\n\n{text}" if title else text


def _source_signature(root: Path) -> dict[str, Any]:
    paths = (root / "corpus.jsonl", root / "queries.jsonl", root / "qrels" / "test.tsv")
    return {
        str(path): {"size": path.stat().st_size, "mtime_ns": path.stat().st_mtime_ns}
        for path in paths
    }


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def materialize_mixed_beir_manifest(
    dataset_roots: dict[str, Path],
    output_dir: Path,
    *,
    documents_per_dataset: int = 0,
    queries_per_dataset: int = 0,
) -> PreparedMixedCorpus:
    """Merge BEIR datasets into one manifest while preserving source identity.

    Documents and queries are namespaced. Identical rendered documents are represented once,
    matching YAMS content-addressed ingestion, and every qrel alias is remapped to that canonical
    document.
    """
    if len(dataset_roots) < 2:
        raise ValueError("mixed corpus benchmarks require at least two datasets")
    if documents_per_dataset < 0 or queries_per_dataset < 0:
        raise ValueError("mixed corpus document/query limits cannot be negative")

    normalized: dict[str, Path] = {}
    original_names: dict[str, str] = {}
    for name, raw_root in dataset_roots.items():
        source = _source_key(name)
        if source in normalized:
            raise ValueError(
                f"mixed corpus source names collide after normalization: {name}"
            )
        root = Path(raw_root).resolve()
        normalized[source] = root
        original_names[source] = name

    spec = {
        "version": 2,
        "documents_per_dataset": documents_per_dataset,
        "queries_per_dataset": queries_per_dataset,
        "sources": [
            {
                "name": source,
                "original_name": original_names[source],
                "root": str(root),
                "signature": _source_signature(root),
            }
            for source, root in normalized.items()
        ],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "benchmark_manifest.json"
    identity_path = output_dir / "mixed_corpus_identity.json"
    if manifest_path.is_file() and identity_path.is_file():
        identity = json.loads(identity_path.read_text(encoding="utf-8"))
        if identity.get("spec") == spec:
            return PreparedMixedCorpus(
                manifest_path=manifest_path,
                identity_path=identity_path,
                document_count=int(identity["document_count"]),
                query_count=int(identity["query_count"]),
                sources=tuple(normalized),
            )

    documents_out: list[dict[str, str]] = []
    queries_out: list[dict[str, str]] = []
    qrels_by_pair: dict[tuple[str, str], int] = {}
    canonical_by_content: dict[str, str] = {}
    document_sources: dict[str, set[str]] = defaultdict(set)
    document_hash_sources: dict[str, set[str]] = defaultdict(set)
    document_hashes: dict[str, str] = {}
    document_aliases: dict[str, str] = {}
    query_sources: dict[str, str] = {}
    source_counts: dict[str, dict[str, int]] = {}

    for source, root in normalized.items():
        documents = {_raw_id(row): row for row in _read_jsonl(root / "corpus.jsonl")}
        documents.pop("", None)
        queries = {_raw_id(row): row for row in _read_jsonl(root / "queries.jsonl")}
        queries.pop("", None)
        qrels = _read_qrels(root / "qrels" / "test.tsv")

        qrels_by_query: dict[str, list[tuple[str, int]]] = defaultdict(list)
        for query_id, document_id, score in qrels:
            if query_id in queries and document_id in documents:
                qrels_by_query[query_id].append((document_id, score))

        selected_query_ids = sorted(
            query_id for query_id in queries if qrels_by_query.get(query_id)
        )
        if queries_per_dataset:
            selected_query_ids = selected_query_ids[:queries_per_dataset]

        required_document_ids = {
            document_id
            for query_id in selected_query_ids
            for document_id, _ in qrels_by_query[query_id]
        }
        selected_document_ids = set(required_document_ids)
        if documents_per_dataset == 0:
            selected_document_ids.update(documents)
        else:
            for document_id in sorted(documents):
                if len(selected_document_ids) >= documents_per_dataset:
                    break
                selected_document_ids.add(document_id)

        alias_to_canonical: dict[str, str] = {}
        for document_id in sorted(selected_document_ids):
            document = documents[document_id]
            alias_id = _namespaced_id(source, document_id)
            content = _rendered_content(document)
            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            canonical_id = canonical_by_content.get(content)
            if canonical_id is None:
                canonical_id = alias_id
                canonical_by_content[content] = canonical_id
                documents_out.append(
                    {
                        "id": canonical_id,
                        "title": str(document.get("title", "")),
                        "text": str(document.get("text", "")),
                    }
                )
            alias_to_canonical[document_id] = canonical_id
            document_aliases[alias_id] = canonical_id
            document_sources[canonical_id].add(source)
            document_hash_sources[content_hash].add(source)
            document_hashes[canonical_id] = content_hash

        for query_id in selected_query_ids:
            mixed_query_id = _namespaced_id(source, query_id)
            queries_out.append(
                {"id": mixed_query_id, "text": str(queries[query_id].get("text", ""))}
            )
            query_sources[mixed_query_id] = source
            for document_id, score in qrels_by_query[query_id]:
                canonical_id = alias_to_canonical.get(document_id)
                if canonical_id is None:
                    continue
                pair = (mixed_query_id, canonical_id)
                qrels_by_pair[pair] = max(qrels_by_pair.get(pair, 0), score)

        source_counts[source] = {
            "input_documents": len(documents),
            "selected_documents": len(selected_document_ids),
            "selected_queries": len(selected_query_ids),
            "selected_qrels": sum(
                1
                for query_id, _ in qrels_by_pair
                if query_sources.get(query_id) == source
            ),
        }

    documents_out.sort(key=lambda row: row["id"])
    queries_out.sort(key=lambda row: row["id"])
    qrels_out = [
        {"query_id": query_id, "doc_id": document_id, "score": score}
        for (query_id, document_id), score in sorted(qrels_by_pair.items())
    ]
    query_order = [row["id"] for row in queries_out]
    manifest = {
        "name": "mixed-beir-" + "-".join(normalized),
        "documents": documents_out,
        "queries": queries_out,
        "qrels": qrels_out,
    }
    identity = {
        "spec": spec,
        "document_count": len(documents_out),
        "query_count": len(queries_out),
        "qrel_count": len(qrels_out),
        "source_counts": source_counts,
        "query_order": query_order,
        "query_sources": query_sources,
        "document_sources": {
            document_id: sorted(sources)
            for document_id, sources in document_sources.items()
        },
        "document_hash_sources": {
            document_hash: sorted(sources)
            for document_hash, sources in document_hash_sources.items()
        },
        "document_hashes": document_hashes,
        "document_aliases": document_aliases,
    }
    _write_json(manifest_path, manifest)
    _write_json(identity_path, identity)
    return PreparedMixedCorpus(
        manifest_path=manifest_path,
        identity_path=identity_path,
        document_count=len(documents_out),
        query_count=len(queries_out),
        sources=tuple(normalized),
    )


def analyze_mixed_cluster_overlap(
    snapshot_path: Path, identity_path: Path
) -> dict[str, Any]:
    """Describe how source-exclusive documents occupy topology clusters.

    Content duplicated across input corpora is reported separately and excluded from purity so a
    deduplicated singleton cannot make an otherwise pure partition look cross-source.
    """
    if not snapshot_path.is_file() or not identity_path.is_file():
        return {"metrics": {}, "clusters": []}

    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    identity = json.loads(identity_path.read_text(encoding="utf-8"))
    document_hash_sources = {
        str(document_hash): tuple(str(source) for source in sources)
        for document_hash, sources in dict(
            identity.get("document_hash_sources") or {}
        ).items()
        if isinstance(sources, list)
    }
    memberships = snapshot.get("memberships") or []
    if not isinstance(memberships, list):
        return {"metrics": {}, "clusters": []}

    cluster_members: dict[str, list[dict[str, Any]]] = defaultdict(list)
    topology_overlap_memberships = 0
    for membership in memberships:
        if not isinstance(membership, dict):
            continue
        cluster_id = str(membership.get("cluster_id") or "")
        document_hash = str(membership.get("document_hash") or "")
        if not cluster_id or not document_hash:
            continue
        cluster_members[cluster_id].append(membership)
        topology_overlap_memberships += int(
            bool(membership.get("overlap_cluster_ids") or [])
        )

    source_clusters: dict[str, set[str]] = defaultdict(set)
    source_documents: dict[str, int] = defaultdict(int)
    source_documents_in_shared: dict[str, int] = defaultdict(int)
    source_exclusive_clusters: dict[str, int] = defaultdict(int)
    cluster_rows: list[dict[str, Any]] = []
    exclusive_document_count = 0
    intrinsic_shared_document_count = 0
    known_document_count = 0
    unknown_document_count = 0
    shared_cluster_count = 0
    shared_cluster_document_count = 0
    analyzable_cluster_count = 0
    singleton_count = 0
    weighted_pure_documents = 0
    weighted_entropy = 0.0

    for cluster_id, cluster_memberships in sorted(cluster_members.items()):
        source_counts: dict[str, int] = defaultdict(int)
        intrinsic_shared = 0
        unknown = 0
        overlap_members = 0
        for membership in cluster_memberships:
            document_hash = str(membership["document_hash"])
            sources = document_hash_sources.get(document_hash, ())
            overlap_members += int(bool(membership.get("overlap_cluster_ids") or []))
            if not sources:
                unknown += 1
                unknown_document_count += 1
                continue
            known_document_count += 1
            if len(sources) != 1:
                intrinsic_shared += 1
                intrinsic_shared_document_count += 1
                continue
            source = sources[0]
            source_counts[source] += 1
            source_documents[source] += 1
            source_clusters[source].add(cluster_id)

        exclusive_in_cluster = sum(source_counts.values())
        exclusive_document_count += exclusive_in_cluster
        shared = len(source_counts) > 1
        if exclusive_in_cluster:
            analyzable_cluster_count += 1
            maximum = max(source_counts.values())
            purity = maximum / exclusive_in_cluster
            entropy = -sum(
                (count / exclusive_in_cluster) * math.log2(count / exclusive_in_cluster)
                for count in source_counts.values()
            )
            weighted_pure_documents += maximum
            weighted_entropy += entropy * exclusive_in_cluster
            if shared:
                shared_cluster_count += 1
                shared_cluster_document_count += exclusive_in_cluster
                for source, count in source_counts.items():
                    source_documents_in_shared[source] += count
            elif len(source_counts) == 1:
                source_exclusive_clusters[next(iter(source_counts))] += 1
        else:
            purity = 0.0
            entropy = 0.0

        singleton_count += int(len(cluster_memberships) == 1)
        cluster_rows.append(
            {
                "cluster_id": cluster_id,
                "member_count": len(cluster_memberships),
                "source_exclusive_member_count": exclusive_in_cluster,
                "source_counts": dict(sorted(source_counts.items())),
                "intrinsic_shared_document_count": intrinsic_shared,
                "unknown_document_count": unknown,
                "topology_overlap_member_count": overlap_members,
                "cross_source": shared,
                "source_purity": purity,
                "source_entropy_bits": entropy,
            }
        )

    cluster_count = len(cluster_rows)
    membership_count = sum(row["member_count"] for row in cluster_rows)
    metrics: dict[str, float] = {
        "mixed_cluster_count": float(cluster_count),
        "mixed_cluster_analyzable_count": float(analyzable_cluster_count),
        "mixed_cluster_singleton_rate": (
            singleton_count / cluster_count if cluster_count else 0.0
        ),
        "mixed_cluster_intrinsic_shared_document_rate": (
            intrinsic_shared_document_count / known_document_count
            if known_document_count
            else 0.0
        ),
        "mixed_cluster_unknown_document_count": float(unknown_document_count),
        "mixed_cluster_shared_count": float(shared_cluster_count),
        "mixed_cluster_shared_rate": (
            shared_cluster_count / analyzable_cluster_count
            if analyzable_cluster_count
            else 0.0
        ),
        "mixed_cluster_shared_document_rate": (
            shared_cluster_document_count / exclusive_document_count
            if exclusive_document_count
            else 0.0
        ),
        "mixed_cluster_weighted_source_purity": (
            weighted_pure_documents / exclusive_document_count
            if exclusive_document_count
            else 0.0
        ),
        "mixed_cluster_weighted_source_entropy_bits": (
            weighted_entropy / exclusive_document_count
            if exclusive_document_count
            else 0.0
        ),
        "mixed_cluster_topology_overlap_membership_rate": (
            topology_overlap_memberships / membership_count if membership_count else 0.0
        ),
    }

    sources = sorted(source_clusters)
    for source in sources:
        metric_source = _source_key(source)
        metrics[f"mixed_cluster_count_{metric_source}"] = float(
            len(source_clusters[source])
        )
        metrics[f"mixed_cluster_exclusive_count_{metric_source}"] = float(
            source_exclusive_clusters[source]
        )
        metrics[f"mixed_cluster_cross_source_exposure_{metric_source}"] = (
            source_documents_in_shared[source] / source_documents[source]
            if source_documents[source]
            else 0.0
        )

    for left_index, left in enumerate(sources):
        for right in sources[left_index + 1 :]:
            union = source_clusters[left] | source_clusters[right]
            intersection = source_clusters[left] & source_clusters[right]
            metrics[
                "mixed_cluster_overlap_jaccard_"
                f"{_source_key(left)}_{_source_key(right)}"
            ] = len(intersection) / len(union) if union else 0.0

    return {
        "algorithm": snapshot.get("algorithm"),
        "snapshot_id": snapshot.get("snapshot_id"),
        "metrics": metrics,
        "clusters": cluster_rows,
    }


def analyze_mixed_corpus_debug(
    debug_path: Path, identity_path: Path, *, top_k: int
) -> dict[str, float]:
    """Compute per-source quality and cross-source interference from one mixed run."""
    if not debug_path.is_file() or not identity_path.is_file():
        return {}
    identity = json.loads(identity_path.read_text(encoding="utf-8"))
    query_order = list(identity.get("query_order") or [])
    query_sources = dict(identity.get("query_sources") or {})
    document_sources = dict(identity.get("document_sources") or {})
    document_hash_sources = dict(identity.get("document_hash_sources") or {})
    document_hashes = dict(identity.get("document_hashes") or {})
    if not query_order:
        return {}

    events_by_index: dict[int, dict[str, Any]] = {}
    for line in debug_path.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("search_type") != "hybrid" or not isinstance(
            event.get("query_index"), int
        ):
            continue
        events_by_index[int(event["query_index"])] = event

    samples: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    oracle_samples: dict[str, list[tuple[float, ...]]] = defaultdict(list)
    cross_results = 0
    returned_results = 0
    cross_top1 = 0
    top1_results = 0
    unknown_results = 0
    route_same_source = 0
    route_cross_source = 0
    route_unknown = 0
    route_by_source: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    post_fusion_results = 0
    post_fusion_cross_source = 0
    post_fusion_top1 = 0
    post_fusion_cross_source_top1 = 0
    post_fusion_unknown = 0
    relevant_fragment_covered = 0
    relevant_fragment_total = 0
    relevant_fragment_query_hits = 0
    relevant_fragment_queries = 0
    relevant_fragment_unknown = 0
    relevant_fragment_by_source: dict[str, list[int]] = defaultdict(
        lambda: [0, 0, 0, 0]
    )
    for query_index, event in sorted(events_by_index.items()):
        if query_index < 0 or query_index >= len(query_order):
            continue
        query_id = query_order[query_index]
        source = query_sources.get(query_id)
        if not source:
            continue
        relevant = {str(value) for value in event.get("relevant_doc_ids") or []}
        returned = [
            str(value).removesuffix(".txt")
            for value in event.get("returned_doc_ids") or []
        ]
        returned = returned[:top_k] if top_k > 0 else returned
        first_rank = next(
            (
                rank
                for rank, document_id in enumerate(returned, start=1)
                if document_id in relevant
            ),
            None,
        )
        reciprocal_rank = 0.0 if first_rank is None else 1.0 / first_rank
        recall = (
            len(relevant.intersection(returned)) / len(relevant) if relevant else 0.0
        )
        hit = 1.0 if first_rank is not None else 0.0
        samples[source].append((reciprocal_rank, recall, hit))

        trace = event.get("relevant_decision_trace") or {}
        traced_relevant = trace.get("relevant_docs") if isinstance(trace, dict) else None
        if isinstance(traced_relevant, list) and traced_relevant:
            relevant_count = len(traced_relevant)
            pre_count = sum(bool(row.get("in_pre_fusion")) for row in traced_relevant)
            post_count = sum(bool(row.get("in_post_fusion")) for row in traced_relevant)
            returned_count = sum(
                bool(row.get("in_returned_topk")) for row in traced_relevant
            )
            pre_hit = pre_count > 0
            returned_hit = returned_count > 0
            stage_presence = trace.get("stage_relevant_presence") or {}
            text_relevant = bool(
                (stage_presence.get("text") or {}).get("any_relevant")
            )
            vector_relevant = bool(
                (stage_presence.get("vector") or {}).get("any_relevant")
            )
            oracle_samples[source].append(
                (
                    float(pre_hit),
                    pre_count / relevant_count,
                    float(post_count > 0),
                    post_count / relevant_count,
                    float(returned_hit),
                    returned_count / relevant_count,
                    float(pre_hit and not returned_hit),
                    float(vector_relevant and not text_relevant),
                )
            )

        stats = event.get("search_stats") or {}
        routed_hashes = [
            value
            for value in str(
                stats.get("topology_weak_query_allowed_candidate_hashes", "")
            ).split("\t")
            if value
        ]
        routed_hash_set = set(routed_hashes)
        known_relevant_hashes: list[str] = []
        for document_id in relevant:
            document_hash = document_hashes.get(document_id)
            if document_hash:
                known_relevant_hashes.append(document_hash)
            else:
                relevant_fragment_unknown += 1
        if known_relevant_hashes:
            covered = sum(
                document_hash in routed_hash_set
                for document_hash in known_relevant_hashes
            )
            relevant_fragment_covered += covered
            relevant_fragment_total += len(known_relevant_hashes)
            relevant_fragment_query_hits += int(covered > 0)
            relevant_fragment_queries += 1
            source_coverage = relevant_fragment_by_source[source]
            source_coverage[0] += covered
            source_coverage[1] += len(known_relevant_hashes)
            source_coverage[2] += int(covered > 0)
            source_coverage[3] += 1
        for document_hash in routed_hashes:
            sources = document_hash_sources.get(document_hash)
            if not sources:
                route_unknown += 1
                route_by_source[source][2] += 1
            elif source in sources:
                route_same_source += 1
                route_by_source[source][0] += 1
            else:
                route_cross_source += 1
                route_by_source[source][1] += 1

        post_fusion_ids = [
            value.removesuffix(".txt")
            for value in str(stats.get("trace_post_fusion_top_doc_ids", "")).split("\t")
            if value
        ]
        post_fusion_ids = post_fusion_ids[:top_k] if top_k > 0 else post_fusion_ids
        for rank, document_id in enumerate(post_fusion_ids, start=1):
            sources = document_sources.get(document_id)
            post_fusion_results += 1
            if not sources:
                post_fusion_unknown += 1
                continue
            is_cross_source = source not in sources
            post_fusion_cross_source += int(is_cross_source)
            if rank == 1:
                post_fusion_top1 += 1
                post_fusion_cross_source_top1 += int(is_cross_source)

        for rank, document_id in enumerate(returned, start=1):
            sources = document_sources.get(document_id)
            returned_results += 1
            if not sources:
                unknown_results += 1
                continue
            is_cross_source = source not in sources
            cross_results += int(is_cross_source)
            if rank == 1:
                top1_results += 1
                cross_top1 += int(is_cross_source)

    metrics: dict[str, float] = {
        "mixed_cross_source_result_rate": (
            cross_results / returned_results if returned_results else 0.0
        ),
        "mixed_cross_source_top1_rate": cross_top1 / top1_results
        if top1_results
        else 0.0,
        "mixed_unknown_result_count": float(unknown_results),
        "mixed_topology_route_source_purity": (
            route_same_source / (route_same_source + route_cross_source + route_unknown)
            if route_same_source + route_cross_source + route_unknown
            else 0.0
        ),
        "mixed_topology_route_cross_source_rate": (
            route_cross_source
            / (route_same_source + route_cross_source + route_unknown)
            if route_same_source + route_cross_source + route_unknown
            else 0.0
        ),
        "mixed_topology_route_unknown_hash_count": float(route_unknown),
        "mixed_topology_relevant_fragment_coverage": (
            relevant_fragment_covered / relevant_fragment_total
            if relevant_fragment_total
            else 0.0
        ),
        "mixed_topology_relevant_fragment_hit_rate": (
            relevant_fragment_query_hits / relevant_fragment_queries
            if relevant_fragment_queries
            else 0.0
        ),
        "mixed_topology_relevant_fragment_unknown_document_count": float(
            relevant_fragment_unknown
        ),
        "mixed_post_fusion_cross_source_result_rate": (
            post_fusion_cross_source / post_fusion_results
            if post_fusion_results
            else 0.0
        ),
        "mixed_post_fusion_cross_source_top1_rate": (
            post_fusion_cross_source_top1 / post_fusion_top1
            if post_fusion_top1
            else 0.0
        ),
        "mixed_post_fusion_unknown_result_count": float(post_fusion_unknown),
    }
    source_mrr: list[float] = []
    source_recall: list[float] = []
    for source, source_samples in sorted(samples.items()):
        metric_source = _source_key(source)
        mrr = mean(sample[0] for sample in source_samples)
        recall = mean(sample[1] for sample in source_samples)
        hit_rate = mean(sample[2] for sample in source_samples)
        source_mrr.append(mrr)
        source_recall.append(recall)
        metrics[f"mixed_query_count_{metric_source}"] = float(len(source_samples))
        metrics[f"mixed_mrr_{metric_source}"] = mrr
        metrics[f"mixed_recall_at_k_{metric_source}"] = recall
        metrics[f"mixed_hit_rate_{metric_source}"] = hit_rate
        source_oracles = oracle_samples[source]
        if source_oracles:
            for metric_name, column in (
                ("candidate_pre_fusion_hit_rate", 0),
                ("candidate_pre_fusion_recall", 1),
                ("candidate_post_fusion_hit_rate", 2),
                ("candidate_post_fusion_recall", 3),
                ("returned_trace_hit_rate", 4),
                ("returned_trace_recall", 5),
                ("ranking_loss_query_rate", 6),
                ("vector_unique_rescue_rate", 7),
            ):
                metrics[f"mixed_{metric_name}_{metric_source}"] = mean(
                    sample[column] for sample in source_oracles
                )
        route_counts = route_by_source[source]
        route_total = sum(route_counts)
        metrics[f"mixed_topology_route_source_purity_{metric_source}"] = (
            route_counts[0] / route_total if route_total else 0.0
        )
        metrics[f"mixed_topology_route_cross_source_rate_{metric_source}"] = (
            route_counts[1] / route_total if route_total else 0.0
        )
        fragment_coverage = relevant_fragment_by_source[source]
        metrics[f"mixed_topology_relevant_fragment_coverage_{metric_source}"] = (
            fragment_coverage[0] / fragment_coverage[1] if fragment_coverage[1] else 0.0
        )
        metrics[f"mixed_topology_relevant_fragment_hit_rate_{metric_source}"] = (
            fragment_coverage[2] / fragment_coverage[3] if fragment_coverage[3] else 0.0
        )

    if source_mrr:
        metrics["mixed_source_macro_mrr"] = mean(source_mrr)
        metrics["mixed_source_min_mrr"] = min(source_mrr)
    if source_recall:
        metrics["mixed_source_macro_recall_at_k"] = mean(source_recall)
        metrics["mixed_source_min_recall_at_k"] = min(source_recall)
    return metrics
