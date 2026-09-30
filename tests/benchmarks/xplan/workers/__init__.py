"""Registered xplan workers."""

from __future__ import annotations

from typing import Callable

from workers.base import WorkerContext, WorkerResult
from workers.code_intelligence import run_code_intelligence
from workers.external_script import run_external_script
from workers.ingestion_e2e import run_ingestion_e2e
from workers.ops_timeline import run_ops_timeline
from workers.output_efficiency import run_output_efficiency
from workers.p2p_memory_sync import run_p2p_memory_sync
from workers.repair_ability import run_repair_ability
from workers.retrieval_load import run_retrieval_load
from workers.retrieval_quality import run_retrieval_quality

WorkerFn = Callable[[WorkerContext], WorkerResult]

REGISTRY: dict[str, WorkerFn] = {
    "code_intelligence": run_code_intelligence,
    "ingestion_e2e": run_ingestion_e2e,
    "retrieval_load": run_retrieval_load,
    "repair_ability": run_repair_ability,
    "ops_timeline": run_ops_timeline,
    "output_efficiency": run_output_efficiency,
    "retrieval_quality": run_retrieval_quality,
    "external_script": run_external_script,
    "p2p_memory_sync": run_p2p_memory_sync,
}


def get_worker(name: str) -> WorkerFn:
    try:
        return REGISTRY[name]
    except KeyError as exc:
        known = ", ".join(sorted(REGISTRY))
        raise KeyError(f"unknown worker {name!r}; known: {known}") from exc
