"""BEIR corpus resolve + download for xplan retrieval_quality plans.

Cache layout matches C++ beir_loader.h:
  ~/.cache/yams/benchmarks/<dataset>/{corpus.jsonl,queries.jsonl,qrels/test.tsv}

Official zips: https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/
"""

from __future__ import annotations

import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Iterable
from urllib.request import urlretrieve

# Mirrors retrieval_quality_bench SUPPORTED_BEIR_DATASETS / beir_loader names.
BEIR_DATASETS = frozenset(
    {
        "scifact",
        "nfcorpus",
        "fiqa",
        "arguana",
        "scidocs",
        "trec-covid",
        "nq",
        "hotpotqa",
        "webis-touche2020",
        "dbpedia-entity",
    }
)

# Zip basename when it differs from the cache directory name we use.
_ZIP_NAMES: dict[str, str] = {
    "touche-2020": "webis-touche2020",
    "dbpedia-entity": "dbpedia-entity",
}

_BEIR_BASE = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets"


def is_beir_dataset(name: str) -> bool:
    return str(name or "").strip().lower() in BEIR_DATASETS


def cache_root() -> Path:
    return Path.home() / ".cache" / "yams" / "benchmarks"


def cache_dir(dataset: str) -> Path:
    return cache_root() / dataset.strip().lower()


def corpus_present(root: Path) -> bool:
    """Accept plain or gzipped BEIR layout."""
    corpus = (root / "corpus.jsonl").exists() or (root / "corpus.jsonl.gz").exists()
    queries = (root / "queries.jsonl").exists() or (root / "queries.jsonl.gz").exists()
    qrels = (root / "qrels" / "test.tsv").exists() or (root / "qrels" / "test.tsv.gz").exists()
    return corpus and queries and qrels


def zip_name(dataset: str) -> str:
    key = dataset.strip().lower()
    return _ZIP_NAMES.get(key, key)


def download_url(dataset: str) -> str:
    return f"{_BEIR_BASE}/{zip_name(dataset)}.zip"


def ensure_beir_dataset(
    dataset: str,
    *,
    download: bool = True,
    force: bool = False,
) -> Path:
    """Return path to a usable BEIR corpus dir; download+unzip if missing.

    Raises FileNotFoundError when missing and download is False, or RuntimeError
    when download/extract fails.
    """
    name = dataset.strip().lower()
    if not is_beir_dataset(name):
        raise ValueError(f"unknown BEIR dataset {dataset!r}; known: {sorted(BEIR_DATASETS)}")

    dest = cache_dir(name)
    if not force and corpus_present(dest):
        return dest

    if not download:
        raise FileNotFoundError(
            f"BEIR dataset {name!r} not found at {dest} "
            f"(need corpus.jsonl + queries.jsonl + qrels/test.tsv). "
            f"Run: python3 tests/benchmarks/xplan/runner.py download-beir {name}"
        )

    dest.parent.mkdir(parents=True, exist_ok=True)
    zname = zip_name(name)
    zip_path = dest.parent / f"{zname}.zip"
    url = download_url(name)

    try:
        urlretrieve(url, zip_path)
    except Exception as exc:  # noqa: BLE001
        # curl fallback (better progress / some proxy setups)
        try:
            subprocess.run(
                ["curl", "-fL", "-o", str(zip_path), url],
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception as curl_exc:  # noqa: BLE001
            raise RuntimeError(
                f"failed to download {url}: urllib={exc}; curl={curl_exc}"
            ) from curl_exc

    if not zip_path.is_file() or zip_path.stat().st_size < 64:
        raise RuntimeError(f"download produced empty/missing zip: {zip_path}")

    extract_root = dest.parent / f".extract-{zname}"
    if extract_root.exists():
        shutil.rmtree(extract_root)
    extract_root.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_root)
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"bad zip {zip_path}: {exc}") from exc

    # Zip usually contains a top-level folder named like the dataset.
    candidate = extract_root / zname
    if not corpus_present(candidate):
        # Fall back: first child dir that looks like BEIR, or extract root itself.
        found = None
        if corpus_present(extract_root):
            found = extract_root
        else:
            for child in sorted(extract_root.iterdir()):
                if child.is_dir() and corpus_present(child):
                    found = child
                    break
        if found is None:
            raise RuntimeError(
                f"unzipped {zip_path} but could not find corpus/queries/qrels under {extract_root}"
            )
        candidate = found

    if dest.exists():
        shutil.rmtree(dest)
    shutil.move(str(candidate), str(dest))
    shutil.rmtree(extract_root, ignore_errors=True)

    if not corpus_present(dest):
        raise RuntimeError(f"after install, corpus still incomplete at {dest}")
    return dest


def ensure_many(datasets: Iterable[str], *, download: bool = True) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for name in datasets:
        out[name] = ensure_beir_dataset(name, download=download)
    return out
