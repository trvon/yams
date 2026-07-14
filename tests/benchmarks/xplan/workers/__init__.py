"""Registered xplan workers."""

from __future__ import annotations

from typing import Callable

from workers.base import WorkerContext, WorkerResult
from workers.external_script import run_external_script
from workers.ingestion_e2e import run_ingestion_e2e
from workers.ops_timeline import run_ops_timeline
from workers.repair_ability import run_repair_ability
from workers.retrieval_load import run_retrieval_load
from workers.retrieval_quality import run_retrieval_quality

WorkerFn = Callable[[WorkerContext], WorkerResult]

REGISTRY: dict[str, WorkerFn] = {
    "ingestion_e2e": run_ingestion_e2e,
    "retrieval_load": run_retrieval_load,
    "repair_ability": run_repair_ability,
    "ops_timeline": run_ops_timeline,
    "retrieval_quality": run_retrieval_quality,
    "external_script": run_external_script,
}


def get_worker(name: str) -> WorkerFn:
    try:
        return REGISTRY[name]
    except KeyError as exc:
        known = ", ".join(sorted(REGISTRY))
        raise KeyError(f"unknown worker {name!r}; known: {known}") from exc
