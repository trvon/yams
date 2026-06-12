#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

DEFAULT_EXTENSIONS = {
    ".c",
    ".cc",
    ".cpp",
    ".cxx",
    ".h",
    ".hh",
    ".hpp",
    ".hxx",
    ".m",
    ".mm",
    ".py",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".java",
    ".go",
    ".rs",
    ".swift",
    ".rb",
    ".php",
    ".cs",
    ".kt",
    ".kts",
    ".scala",
    ".sh",
    ".bash",
}

DEFAULT_EXCLUDES = {
    ".git",
    ".hg",
    ".svn",
    ".ccls-cache",
    ".direnv",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "node_modules",
    "dist",
    "vendor",
    "third_party",
    "external",
    "site",
    "subprojects",
    "infer-out",
    "build",
    "builddir",
    "build-debug",
    "build-release",
    "cmake-build-debug",
    "cmake-build-release",
}

LINE_COMMENT_PREFIXES = {
    ".c": "//",
    ".cc": "//",
    ".cpp": "//",
    ".cxx": "//",
    ".h": "//",
    ".hh": "//",
    ".hpp": "//",
    ".hxx": "//",
    ".m": "//",
    ".mm": "//",
    ".js": "//",
    ".jsx": "//",
    ".ts": "//",
    ".tsx": "//",
    ".java": "//",
    ".go": "//",
    ".rs": "//",
    ".swift": "//",
    ".cs": "//",
    ".kt": "//",
    ".kts": "//",
    ".scala": "//",
    ".php": "//",
    ".py": "#",
    ".rb": "#",
    ".sh": "#",
    ".bash": "#",
}

BLOCK_COMMENT_STYLES = {
    ".c": ("/*", "*/"),
    ".cc": ("/*", "*/"),
    ".cpp": ("/*", "*/"),
    ".cxx": ("/*", "*/"),
    ".h": ("/*", "*/"),
    ".hh": ("/*", "*/"),
    ".hpp": ("/*", "*/"),
    ".hxx": ("/*", "*/"),
    ".m": ("/*", "*/"),
    ".mm": ("/*", "*/"),
    ".js": ("/*", "*/"),
    ".jsx": ("/*", "*/"),
    ".ts": ("/*", "*/"),
    ".tsx": ("/*", "*/"),
    ".java": ("/*", "*/"),
    ".go": ("/*", "*/"),
    ".rs": ("/*", "*/"),
    ".swift": ("/*", "*/"),
    ".cs": ("/*", "*/"),
    ".kt": ("/*", "*/"),
    ".kts": ("/*", "*/"),
    ".scala": ("/*", "*/"),
    ".php": ("/*", "*/"),
}

LANGUAGE_NAMES = {
    ".c": "C",
    ".cc": "C++",
    ".cpp": "C++",
    ".cxx": "C++",
    ".h": "C/C++ Header",
    ".hh": "C++ Header",
    ".hpp": "C++ Header",
    ".hxx": "C++ Header",
    ".m": "Objective-C",
    ".mm": "Objective-C++",
    ".py": "Python",
    ".js": "JavaScript",
    ".jsx": "JSX",
    ".ts": "TypeScript",
    ".tsx": "TSX",
    ".java": "Java",
    ".go": "Go",
    ".rs": "Rust",
    ".swift": "Swift",
    ".rb": "Ruby",
    ".php": "PHP",
    ".cs": "C#",
    ".kt": "Kotlin",
    ".kts": "Kotlin",
    ".scala": "Scala",
    ".sh": "Shell",
    ".bash": "Shell",
}


@dataclass
class FileMetric:
    path: str
    language: str
    lines: int
    code_lines: int
    comment_lines: int
    blank_lines: int


@dataclass
class DirectoryMetric:
    path: str
    files: int
    lines: int
    code_lines: int


@dataclass
class Summary:
    root: str
    files_analyzed: int
    total_lines: int
    total_code_lines: int
    total_comment_lines: int
    total_blank_lines: int
    top_files: list[FileMetric]
    top_directories: list[DirectoryMetric]


def should_skip(path: Path, excluded_names: set[str]) -> bool:
    for part in path.parts:
        if part in excluded_names:
            return True
        if part.startswith(("build", "cmake-build")):
            return True
    return False


def iter_source_files(
    root: Path, extensions: set[str], excluded_names: set[str]
) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if should_skip(path.relative_to(root), excluded_names):
            continue
        if path.suffix.lower() in extensions:
            yield path


def classify_language(path: Path) -> str:
    return LANGUAGE_NAMES.get(
        path.suffix.lower(), path.suffix.lower().lstrip(".") or "unknown"
    )


def count_lines(path: Path) -> FileMetric:
    suffix = path.suffix.lower()
    line_comment = LINE_COMMENT_PREFIXES.get(suffix)
    block_comment = BLOCK_COMMENT_STYLES.get(suffix)
    block_start, block_end = block_comment if block_comment else (None, None)

    total = 0
    code = 0
    comment = 0
    blank = 0
    in_block_comment = False

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            total += 1
            stripped = raw_line.strip()
            if not stripped:
                blank += 1
                continue

            if in_block_comment:
                comment += 1
                if block_end and block_end in stripped:
                    in_block_comment = False
                continue

            if line_comment and stripped.startswith(line_comment):
                comment += 1
                continue

            if block_start and stripped.startswith(block_start):
                comment += 1
                if block_end and block_end not in stripped:
                    in_block_comment = True
                continue

            code += 1

    return FileMetric(
        path=str(path),
        language=classify_language(path),
        lines=total,
        code_lines=code,
        comment_lines=comment,
        blank_lines=blank,
    )


def aggregate_directories(
    root: Path, metrics: list[FileMetric], limit: int
) -> list[DirectoryMetric]:
    buckets: dict[str, DirectoryMetric] = {}
    for metric in metrics:
        rel = Path(metric.path).relative_to(root)
        key = str(rel.parent)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = DirectoryMetric(path=key, files=0, lines=0, code_lines=0)
            buckets[key] = bucket
        bucket.files += 1
        bucket.lines += metric.lines
        bucket.code_lines += metric.code_lines

    return sorted(
        buckets.values(),
        key=lambda item: (item.code_lines, item.lines, item.files, item.path),
        reverse=True,
    )[:limit]


def build_summary(root: Path, metrics: list[FileMetric], limit: int) -> Summary:
    top_files = sorted(
        metrics,
        key=lambda item: (item.code_lines, item.lines, item.path),
        reverse=True,
    )[:limit]
    return Summary(
        root=str(root),
        files_analyzed=len(metrics),
        total_lines=sum(item.lines for item in metrics),
        total_code_lines=sum(item.code_lines for item in metrics),
        total_comment_lines=sum(item.comment_lines for item in metrics),
        total_blank_lines=sum(item.blank_lines for item in metrics),
        top_files=top_files,
        top_directories=aggregate_directories(root, metrics, limit),
    )


def render_markdown(summary: Summary) -> str:
    lines = [
        "# Complexity Hotspots",
        "",
        f"- Root: `{summary.root}`",
        f"- Files analyzed: `{summary.files_analyzed}`",
        f"- Total lines: `{summary.total_lines}`",
        f"- Code lines: `{summary.total_code_lines}`",
        f"- Comment lines: `{summary.total_comment_lines}`",
        f"- Blank lines: `{summary.total_blank_lines}`",
        "",
        "## Top files by code lines",
        "",
        "| Rank | Code LOC | Total LOC | Language | File |",
        "| --- | ---: | ---: | --- | --- |",
    ]
    for index, metric in enumerate(summary.top_files, start=1):
        rel = Path(metric.path).relative_to(summary.root)
        lines.append(
            f"| {index} | {metric.code_lines} | {metric.lines} | {metric.language} | `{rel}` |"
        )

    lines.extend(
        [
            "",
            "## Top directories by code lines",
            "",
            "| Rank | Code LOC | Total LOC | Files | Directory |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )
    for index, metric in enumerate(summary.top_directories, start=1):
        lines.append(
            f"| {index} | {metric.code_lines} | {metric.lines} | {metric.files} | `{metric.path}` |"
        )
    return "\n".join(lines)


def render_text(summary: Summary) -> str:
    lines = [
        f"root: {summary.root}",
        f"files_analyzed: {summary.files_analyzed}",
        f"total_lines: {summary.total_lines}",
        f"total_code_lines: {summary.total_code_lines}",
        "top_files:",
    ]
    for metric in summary.top_files:
        rel = Path(metric.path).relative_to(summary.root)
        lines.append(f"  {metric.code_lines:>6} code / {metric.lines:>6} total  {rel}")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simple first-pass complexity hotspot scanner"
    )
    parser.add_argument("root", nargs="?", default=".", help="Repository root to scan")
    parser.add_argument(
        "--format",
        choices=("markdown", "json", "text"),
        default="text",
        help="Output format",
    )
    parser.add_argument(
        "--limit", type=int, default=20, help="Rows to show for top files/dirs"
    )
    parser.add_argument(
        "--exclude-dir",
        action="append",
        default=[],
        help="Additional directory names to exclude",
    )
    parser.add_argument(
        "--extension",
        action="append",
        default=[],
        help="Additional file extension to include (example: .zig)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    excluded = set(DEFAULT_EXCLUDES)
    excluded.update(args.exclude_dir)
    extensions = set(DEFAULT_EXTENSIONS)
    extensions.update(
        ext if ext.startswith(".") else f".{ext}" for ext in args.extension
    )

    metrics = [
        count_lines(path) for path in iter_source_files(root, extensions, excluded)
    ]
    summary = build_summary(root, metrics, max(1, args.limit))

    if args.format == "json":
        print(json.dumps(asdict(summary), indent=2))
    elif args.format == "markdown":
        print(render_markdown(summary))
    else:
        print(render_text(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
