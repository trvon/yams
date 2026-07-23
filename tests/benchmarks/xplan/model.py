"""Experiment plan loading and arm expansion for yams xplan."""

from __future__ import annotations

import itertools
import json
import re
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional until PyYAML available
    yaml = None


_ARM_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def sanitize_arm_name(name: str) -> str:
    cleaned = _ARM_SAFE.sub("_", name.strip())
    return cleaned.strip("._-") or "arm"


def _as_str_map(raw: dict[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if not raw:
        return out
    for key, value in raw.items():
        if isinstance(value, bool):
            out[str(key)] = "1" if value else "0"
        else:
            out[str(key)] = str(value)
    return out


def _load_raw(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    data: Any
    if suffix == ".json":
        data = json.loads(text)
    elif suffix in {".yaml", ".yml"}:
        if yaml is None:
            # Prefer sibling .json when PyYAML is unavailable.
            sibling = path.with_suffix(".json")
            if sibling.is_file():
                data = json.loads(sibling.read_text(encoding="utf-8"))
            else:
                raise RuntimeError(
                    "PyYAML is required to load .yaml plans without a sibling .json. "
                    "Install pyyaml or use the .json plan."
                )
        else:
            data = yaml.safe_load(text)
    else:
        # Extension-less / unknown: try JSON first, then YAML.
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            if yaml is None:
                raise RuntimeError(
                    f"cannot parse plan {path}: not JSON and PyYAML is not installed"
                ) from None
            data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError(f"plan root must be an object: {path}")
    return data


@dataclass
class Step:
    worker: str
    params: dict[str, Any] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    timeout_sec: int | None = None
    metrics: list[str] = field(default_factory=list)
    allow_stub: bool = False

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> Step:
        return cls(
            worker=str(raw["worker"]),
            params=dict(raw.get("params") or {}),
            env=_as_str_map(raw.get("env")),
            timeout_sec=int(raw["timeout_sec"]) if raw.get("timeout_sec") is not None else None,
            metrics=[str(m) for m in (raw.get("metrics") or [])],
            allow_stub=bool(raw.get("allow_stub", False)),
        )


@dataclass
class Arm:
    name: str
    factors: dict[str, Any] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def safe_name(self) -> str:
        return sanitize_arm_name(self.name)


@dataclass
class ValidateSpec:
    require_files: list[str] = field(default_factory=lambda: ["metrics.json"])
    require_metrics: list[str] = field(default_factory=list)
    require_metric_status: list[str] = field(default_factory=lambda: ["ok"])

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> ValidateSpec:
        raw = raw or {}
        status = raw.get("require_metric_status")
        return cls(
            require_files=[str(x) for x in (raw.get("require_files") or ["metrics.json"])],
            require_metrics=[str(x) for x in (raw.get("require_metrics") or [])],
            require_metric_status=[str(x) for x in status]
            if status is not None
            else ["ok"],
        )


@dataclass
class PairedVectorOracleSpec:
    arm: str
    component: str = "vector"
    top_k: int = 10
    identity_file: str = "datasets/mixed_beir/mixed_corpus_identity.json"

    @classmethod
    def from_dict(
        cls, raw: dict[str, Any] | None
    ) -> PairedVectorOracleSpec | None:
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValueError("summarize.paired_vector_oracle must be an object")
        arm = str(raw.get("arm") or "").strip()
        component = str(raw.get("component") or "vector").strip()
        identity_file = str(
            raw.get("identity_file")
            or "datasets/mixed_beir/mixed_corpus_identity.json"
        ).strip()
        top_k = int(raw.get("top_k") or 10)
        if not arm:
            raise ValueError("summarize.paired_vector_oracle.arm is required")
        if not component:
            raise ValueError("summarize.paired_vector_oracle.component is required")
        if not identity_file:
            raise ValueError("summarize.paired_vector_oracle.identity_file is required")
        if top_k <= 0:
            raise ValueError("summarize.paired_vector_oracle.top_k must be positive")
        return cls(
            arm=arm,
            component=component,
            top_k=top_k,
            identity_file=identity_file,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "arm": self.arm,
            "component": self.component,
            "top_k": self.top_k,
            "identity_file": self.identity_file,
        }


@dataclass
class PairedQueryAnalysisSpec:
    metrics: list[str] = field(
        default_factory=lambda: ["mrr", "recall_at_k", "ndcg_at_k"]
    )
    confidence_level: float = 0.95
    bootstrap_samples: int = 5000
    seed: int = 1729
    identity_file: str = "datasets/mixed_beir/mixed_corpus_identity.json"

    @classmethod
    def from_dict(
        cls, raw: dict[str, Any] | None
    ) -> PairedQueryAnalysisSpec | None:
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValueError("summarize.paired_query_analysis must be an object")
        metrics = [str(value).strip() for value in (raw.get("metrics") or [])]
        confidence_level = float(raw.get("confidence_level", 0.95))
        bootstrap_samples = int(raw.get("bootstrap_samples", 5000))
        seed = int(raw.get("seed", 1729))
        identity_file = str(
            raw.get("identity_file")
            or "datasets/mixed_beir/mixed_corpus_identity.json"
        ).strip()
        if not metrics or any(not metric for metric in metrics):
            raise ValueError("summarize.paired_query_analysis.metrics is required")
        if not 0.0 < confidence_level < 1.0:
            raise ValueError(
                "summarize.paired_query_analysis.confidence_level must be between 0 and 1"
            )
        if bootstrap_samples < 100:
            raise ValueError(
                "summarize.paired_query_analysis.bootstrap_samples must be at least 100"
            )
        if not identity_file:
            raise ValueError(
                "summarize.paired_query_analysis.identity_file is required"
            )
        return cls(
            metrics=metrics,
            confidence_level=confidence_level,
            bootstrap_samples=bootstrap_samples,
            seed=seed,
            identity_file=identity_file,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "metrics": self.metrics,
            "confidence_level": self.confidence_level,
            "bootstrap_samples": self.bootstrap_samples,
            "seed": self.seed,
            "identity_file": self.identity_file,
        }


@dataclass
class PromotionGatesSpec:
    candidate_arms: list[str]
    quality_metric: str = "mrr"
    quality_ci_lower_min: float = 0.0
    per_source_metrics: list[str] = field(
        default_factory=lambda: ["mrr", "recall_at_k", "ndcg_at_k"]
    )
    max_source_regression: float = 0.005
    latency_relative_max: dict[str, float] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> PromotionGatesSpec | None:
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise ValueError("summarize.promotion_gates must be an object")
        candidate_arms = [
            str(value).strip() for value in (raw.get("candidate_arms") or [])
        ]
        quality_metric = str(raw.get("quality_metric") or "mrr").strip()
        per_source_metrics = [
            str(value).strip() for value in (raw.get("per_source_metrics") or [])
        ]
        latency_relative_max = {
            str(metric): float(limit)
            for metric, limit in (raw.get("latency_relative_max") or {}).items()
        }
        max_source_regression = float(raw.get("max_source_regression", 0.005))
        if not candidate_arms or any(not arm for arm in candidate_arms):
            raise ValueError("summarize.promotion_gates.candidate_arms is required")
        if not quality_metric:
            raise ValueError("summarize.promotion_gates.quality_metric is required")
        if not per_source_metrics or any(not metric for metric in per_source_metrics):
            raise ValueError(
                "summarize.promotion_gates.per_source_metrics is required"
            )
        if max_source_regression < 0.0:
            raise ValueError(
                "summarize.promotion_gates.max_source_regression cannot be negative"
            )
        if not latency_relative_max or any(
            not metric or limit < 1.0
            for metric, limit in latency_relative_max.items()
        ):
            raise ValueError(
                "summarize.promotion_gates.latency_relative_max must contain ratios >= 1"
            )
        return cls(
            candidate_arms=candidate_arms,
            quality_metric=quality_metric,
            quality_ci_lower_min=float(raw.get("quality_ci_lower_min", 0.0)),
            per_source_metrics=per_source_metrics,
            max_source_regression=max_source_regression,
            latency_relative_max=latency_relative_max,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_arms": self.candidate_arms,
            "quality_metric": self.quality_metric,
            "quality_ci_lower_min": self.quality_ci_lower_min,
            "per_source_metrics": self.per_source_metrics,
            "max_source_regression": self.max_source_regression,
            "latency_relative_max": self.latency_relative_max,
        }


@dataclass
class SummarizeSpec:
    primary: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    paired_vector_oracle: PairedVectorOracleSpec | None = None
    paired_query_analysis: PairedQueryAnalysisSpec | None = None
    promotion_gates: PromotionGatesSpec | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> SummarizeSpec:
        raw = raw or {}
        return cls(
            primary=[str(x) for x in (raw.get("primary") or [])],
            group_by=[str(x) for x in (raw.get("group_by") or [])],
            paired_vector_oracle=PairedVectorOracleSpec.from_dict(
                raw.get("paired_vector_oracle")
            ),
            paired_query_analysis=PairedQueryAnalysisSpec.from_dict(
                raw.get("paired_query_analysis")
            ),
            promotion_gates=PromotionGatesSpec.from_dict(raw.get("promotion_gates")),
        )


@dataclass
class ExperimentPlan:
    name: str
    description: str
    build_dir: str
    artifact_root: str
    timeout_sec: int
    continue_on_arm_failure: bool
    mode: str
    fixed_env: dict[str, str]
    fixed_params: dict[str, Any]
    factors: dict[str, list[Any]]
    arms: list[Arm]
    steps: list[Step]
    validate: ValidateSpec
    summarize: SummarizeSpec
    source_path: Path | None = None
    raw: dict[str, Any] = field(default_factory=dict)
    repeats: int = 1
    baseline: str | None = None

    @classmethod
    def load(cls, path: Path) -> ExperimentPlan:
        path = path.resolve()
        raw = _load_raw(path)
        name = str(raw.get("name") or path.stem)
        fixed = raw.get("fixed") or {}
        factors_raw = raw.get("factors") or {}
        factors: dict[str, list[Any]] = {
            str(k): list(v) for k, v in factors_raw.items() if isinstance(v, list)
        }

        arms: list[Arm] = []
        if raw.get("arms"):
            for item in raw["arms"]:
                if not isinstance(item, dict) or "name" not in item:
                    raise ValueError(f"invalid arm entry in {path}")
                arms.append(
                    Arm(
                        name=str(item["name"]),
                        factors=dict(item.get("factors") or {}),
                        env=_as_str_map(item.get("env")),
                        params=dict(item.get("params") or {}),
                    )
                )
        elif factors:
            keys = sorted(factors.keys())
            for values in itertools.product(*(factors[k] for k in keys)):
                factor_map = {k: v for k, v in zip(keys, values, strict=True)}
                label = "_".join(f"{k}-{v}" for k, v in factor_map.items())
                arms.append(Arm(name=label, factors=factor_map))
        else:
            arms.append(Arm(name="default"))

        steps = [Step.from_dict(s) for s in (raw.get("steps") or [])]
        if not steps:
            raise ValueError(f"plan has no steps: {path}")
        source_raw = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
        repeats = raw.get("repeats")
        if repeats is None:
            repeats = source_raw.get("repeats")
        baseline = raw.get("baseline")
        if baseline is None:
            baseline = source_raw.get("baseline")

        return cls(
            name=name,
            description=str(raw.get("description") or ""),
            build_dir=str(raw.get("build_dir") or "build/release"),
            artifact_root=str(raw.get("artifact_root") or "build/benchmarks"),
            timeout_sec=int(raw.get("timeout_sec") or 600),
            continue_on_arm_failure=bool(raw.get("continue_on_arm_failure", True)),
            mode=str(raw.get("mode") or "synthetic"),
            fixed_env=_as_str_map(fixed.get("env")),
            fixed_params=dict(fixed.get("params") or {}),
            factors=factors,
            arms=arms,
            steps=steps,
            validate=ValidateSpec.from_dict(raw.get("validate")),
            summarize=SummarizeSpec.from_dict(raw.get("summarize")),
            source_path=path,
            raw=raw,
            repeats=max(1, int(repeats or 1)),
            baseline=str(baseline) if baseline else None,
        )

    def resolved_dict(self, *, stamp: str, repo_root: Path, git_sha: str | None) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "stamp": stamp,
            "source_path": str(self.source_path) if self.source_path else None,
            "repo_root": str(repo_root),
            "git_sha": git_sha,
            "build_dir": self.build_dir,
            "artifact_root": self.artifact_root,
            "timeout_sec": self.timeout_sec,
            "continue_on_arm_failure": self.continue_on_arm_failure,
            "mode": self.mode,
            "repeats": self.repeats,
            "baseline": self.baseline,
            "fixed": {"env": self.fixed_env, "params": self.fixed_params},
            "factors": self.factors,
            "arms": [
                {
                    "name": a.name,
                    "safe_name": a.safe_name,
                    "factors": a.factors,
                    "env": a.env,
                    "params": a.params,
                }
                for a in self.arms
            ],
            "steps": [
                {
                    "worker": s.worker,
                    "params": s.params,
                    "env": s.env,
                    "timeout_sec": s.timeout_sec,
                    "metrics": s.metrics,
                    "allow_stub": s.allow_stub,
                }
                for s in self.steps
            ],
            "validate": {
                "require_files": self.validate.require_files,
                "require_metrics": self.validate.require_metrics,
                "require_metric_status": self.validate.require_metric_status,
            },
            "summarize": {
                "primary": self.summarize.primary,
                "group_by": self.summarize.group_by,
                "paired_vector_oracle": (
                    self.summarize.paired_vector_oracle.to_dict()
                    if self.summarize.paired_vector_oracle
                    else None
                ),
                "paired_query_analysis": (
                    self.summarize.paired_query_analysis.to_dict()
                    if self.summarize.paired_query_analysis
                    else None
                ),
                "promotion_gates": (
                    self.summarize.promotion_gates.to_dict()
                    if self.summarize.promotion_gates
                    else None
                ),
            },
            "raw": deepcopy(self.raw),
        }


@dataclass
class ArmContext:
    plan: ExperimentPlan
    arm: Arm
    repo_root: Path
    build_dir: Path
    run_dir: Path
    arm_dir: Path
    stamp: str
    dry_run: bool = False

    def merged_env(self, step: Step) -> dict[str, str]:
        env: dict[str, str] = {}
        env.update(self.plan.fixed_env)
        env.update(self.arm.env)
        env.update(step.env)
        return env

    def merged_params(self, step: Step) -> dict[str, Any]:
        params: dict[str, Any] = {}
        params.update(self.plan.fixed_params)
        params.update(self.arm.params)
        params.update(step.params)
        # Expose factor values as params for workers that key off them.
        for key, value in self.arm.factors.items():
            params.setdefault(key, value)
        return params
