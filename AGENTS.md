---
description: YAMS-first repo agent supplement for Codex (engineering + bug-bounty)
argument-hint: [TASK=<description>] [MODE=<engineering|bug-bounty>] [PBI=<pbi-id>] [PHASE=<start|checkpoint|complete>]
---

# AGENTS.md (Repo Supplement)

This file supplements `docs/prompts/PROMPT-eng-codex.md`.

Use `docs/prompts/PROMPT-eng-codex.md` for the generic Codex/YAMS operating model.
Use this file for YAMS repo specifics, local conventions, and repo-scoped safety.

## Repo Intent

- Primary use: software engineering on YAMS.
- Secondary use: bug-bounty style security research workflows (scoped, non-destructive by default).
- Persistent memory goal: use YAMS as distributed memory across sessions, agents, and handoffs.

## Repo Priorities (Overrides / Additions)

- Prefer YAMS retrieval before local file reads when recovering context or prior decisions.
- Keep memory artifacts small and queryable (checkpoint code + short notes > long prose dumps).
- Index work before `git push`, and before deleting files.
- If blackboard tools are unavailable, continue with YAMS-only workflow without blocking.

## Repo Agent ID

- Canonical format: `opencode-<task-slug>`
- Example: `opencode-list-snippet-hydration`

## Required YAMS Metadata (Repo Standard)

Attach to every `yams add`:

- `task`: short task slug
- `phase`: `start|checkpoint|complete`
- `owner`: `opencode`
- `source`: `code|note|decision|research|evidence`

Recommended in this repo:

- `mode`: `engineering|bug-bounty`
- `pbi`: PBI id if applicable
- `agent_id`: `opencode-<task-slug>`
- `status`: `open|blocked|done`

Bug-bounty additions when applicable:

- `target`: program/asset identifier
- `scope`: in-scope surface label
- `severity`: `low|medium|high|critical`
- `repro`: reproducibility tag
- `impact`: impact summary tag

## YAMS-First Retrieval Behavior

Use this order when you need context for reasoning:

1. `yams search` / `yams grep` to discover artifacts
2. `yams get` (or MCP `get`) to read selected artifacts
3. Local file reads for implementation details only after YAMS retrieval

Behavior rules:

- Prefer YAMS retrieval over ad-hoc local `rg`/`cat` for prior knowledge.
- Do not mark chunks as rejected unless explicit user/model feedback says so.
- Treat `served - used` as weak negative signal (`not_used`), not hard rejection.

Reporting when retrieval is used:

- Include `UsedContext: <chunk_ids or hashes>` when known.
- Include `Citations: <artifact paths/hashes>` or `Citations: none`.
- Preserve `trace_id` in logs/artifacts when available.

## Blackboard Coordination (If Available)

Registration first:

```text
bb_register_agent({ id: "opencode-<task-slug>", name: "OpenCode Agent", capabilities: ["yams", "code", "coordination"] })
```

Minimal coordination pattern:

```text
bb_search_findings({ query: "<keywords>" })
bb_post_finding({ agent_id: "opencode-<task-slug>", topic: "other", title: "<title>", content: "<markdown>" })
bb_create_task({ title: "<work item>", type: "fix", priority: 2, created_by: "opencode-<task-slug>" })
bb_claim_task({ task_id: "<task-id>", agent_id: "opencode-<task-slug>" })
bb_update_task({ task_id: "<task-id>", status: "working" })
```

Fallback:

- If blackboard is unavailable, create a YAMS claim note and proceed.

## Repo Search / Recovery Commands

Common discovery commands:

```bash
yams grep "<pattern>" --cwd .
yams search "$TASK" --limit 20
yams search "task=$TASK" --type keyword --limit 20
```

Session recovery:

```bash
yams search "owner=opencode task=<task>" --type keyword --limit 50
yams search "agent_id=opencode-<task-slug>" --type keyword --limit 50
```

## Repo Structure (Start Here)

- CLI entry points and command wiring: `src/cli/`
- Service layer and command handlers: `src/app/services/`
- Search implementations (grep/semantic): `src/search/`
- Metadata and storage indexing: `src/metadata/`
- Storage engines/backends: `src/storage/`
- Vector DB + embeddings: `src/vector/`
- Daemon client/server/components: `src/daemon/`
- MCP server implementation: `src/mcp/`
- Tests: `tests/`

## Repo Conventions (Condensed)

- Formatting is enforced mechanically by `.clang-format` (LLVM base, 4-space, 100-col, attach braces).
- C++ naming patterns:
  - functions/variables: `camelCase`
  - types: `PascalCase`
  - constants: `kPascalCase`
  - member fields: trailing `_`
  - files: `snake_case`
- Use `Result<T>` for fallible operations and explicit propagation.
- Prefer `YAMS_HAS_*` feature gates from `include/yams/core/cpp23_features.hpp` over raw compiler checks.

## Repo Patterns To Reuse (High Signal)

- `TuneAdvisor`: runtime knob pattern uses static inline atomics and relaxed ordering for advisory reads.
- `InternalEventBus`: typed channels keyed by string names for daemon component coordination.
- `ResourceGovernor`: check admission (`canAdmitWork`, `canScaleUp`, `canLoadModel`) before heavy work.
- `profiling.h`: use `YAMS_*` profiling macros; no-op when Tracy is disabled.
- Async daemon paths use Boost.Asio coroutines (`boost::asio::awaitable<...>`).

## Bug-Bounty Rules (Repo Use)

Apply when `mode=bug-bounty`:

- Stay within stated scope and authorized targets.
- Prefer read-only or low-impact validation first.
- No persistence, destructive payloads, or data exfiltration.
- Redact secrets, tokens, cookies, and personal data before `yams add`.
- Index evidence as short reproducible notes (`source=evidence`) plus sanitized artifacts.

## Ask-First Actions

Always ask before:

- `git push` (index in YAMS first)
- deleting files (index in YAMS first)
- installing new dependencies
- high-impact security testing steps

## Minimal Repo Handoff Template

```text
TASK: $TASK
MODE: $MODE
PBI: $PBI
PHASE: $PHASE
AGENT: opencode-$TASK

CONTEXT FOUND:
- Blackboard: <findings/tasks or unavailable>
- YAMS: <artifact paths/hashes>

ACTIONS:
- <what changed and why>

INDEXED:
- <files/notes indexed or why indexing failed>
- Metadata: mode=$MODE,task=$TASK,phase=$PHASE,owner=opencode,agent_id=opencode-$TASK

NEXT:
- <next step>

USED_CONTEXT:
- <chunk_ids/hashes if known; else unknown>

CITATIONS:
- <artifact names/hashes/paths or none>
```

## When Stuck

- Ask one targeted clarifying question with a recommended default.
- Post a blackboard finding if it helps future agents.
- Prefer small, reversible changes over speculative rewrites.

## Keep This File Lean

- Put generic agent behavior in `docs/prompts/PROMPT-eng-codex.md`.
- Keep this file repo-specific.
- Avoid copying session-specific skill catalogs into this file.
