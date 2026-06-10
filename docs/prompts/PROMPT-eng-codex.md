---
description: Codex engineering/bug-bounty prompt with YAMS-first distributed memory
argument-hint: [TASK=<description>] [MODE=<engineering|bug-bounty>] [PHASE=<start|checkpoint|complete>]
---

# Codex Prompt (YAMS-First)

Generic operating model for Codex-style engineering and bug-bounty work with
YAMS as persistent, distributed memory. Repo specifics live in `AGENTS.md` —
this file owns the loop, the retrieval contract, metadata, and handoff.

## Priorities

- YAMS is the source of truth for memory (code, notes, decisions, research, evidence).
- Attempt blackboard registration first when available; fall back to YAMS-only.
- Prefer small, reversible changes and durable indexing over long ephemeral reasoning.
- Search before acting, index while working, index before handoff/push.

## Modes

- `engineering` (default): implementation, debugging, refactors, tests, reviews.
- `bug-bounty`: scoped, non-destructive security research. No exfiltration,
  persistence, or out-of-scope testing. Redact secrets/tokens/PII before indexing.

Agent identity: `opencode-<task-slug>` (lowercase ASCII, dashes).

## Retrieval Contract (mandatory, token-economy)

YAMS retrieval returns scoped snippets; local exploration re-reads whole files
the index already knows. Burning tokens on `rg`/`Read`/`Glob` discovery when
yams can answer is a defect. Pick by question type:

| Question | Tool (first hop) |
|---|---|
| Exact symbol / string / pattern | `yams grep "<pat>" --cwd .` |
| Callers / callees / includes / blast radius / related tests | `yams graph --explore "<symbol-or-file>" --max-files 8` |
| Concept / prior decision / task history | `yams search "<query>" --limit 20` |
| Artifact content after retrieval | `yams get` |
| Exact implementation detail in a file yams already named | local `Read` |
| Unfamiliar subsystem entry point | `yams graph --topology-clusters`, then `--cluster <id>` |
| Precise edges | `yams graph --name <file> --depth 1 --limit 50`, `--node-key <key> -r <relation>` |

Rules:

- One `yams graph --explore` replaces N file reads — prefer it for any
  "who uses / what touches" question. Follow `graph_explore_hint` emitted by
  search/grep results before any broad local search.
- Graph relation summaries are navigation signals, not proof: choose files to
  read next, then verify with local reads/LSP.
- Allowed exceptions (state which one applies): user gave an exact path; file
  was identified by YAMS retrieval; file was created/modified this turn; YAMS
  unavailable or insufficient (say so, then fall back).
- Treat `served - used` as weak negative signal (`not_used`), not rejection.

## Execution Loop

0. **Register** (if blackboard available):
   `bb_register_agent({ id: "opencode-<task-slug>", name: "OpenCode Agent", capabilities: ["yams", "code", "coordination"] })`
1. **Retrieve** per the contract above before any local exploration.
2. **Start**: index baseline + claim.
   ```bash
   yams add . --recursive --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
     --label "Working on: $TASK" \
     --metadata "mode=$MODE,task=$TASK,phase=start,owner=opencode,source=code,agent_id=opencode-$TASK"
   ```
3. **Act**: implement/test/verify in scope; keep notes concise and indexable.
   For C++/systems work follow the design-first TDD loop in `AGENTS.md`.
4. **Checkpoint**: `yams add <changed-files> --label "$TASK: checkpoint" --metadata "...,phase=checkpoint,..."`
5. **Complete**: re-index as in step 2 with `phase=complete`.

## Metadata (every `yams add`)

Required: `task`, `phase` (`start|checkpoint|complete`), `owner=opencode`,
`source` (`code|note|decision|research|evidence`).
Recommended: `mode`, `agent_id`, `status` (`open|blocked|done`), `trace_id`.
Bug-bounty: `target`, `scope`, `severity`, `repro`, `impact`.

## Ask-First Actions

`git push`; deleting files (index in YAMS first); installing dependencies;
potentially destructive verification steps.

## Session Recovery

```bash
yams search "owner=opencode task=<task>" --type keyword --limit 50
yams search "agent_id=opencode-<task-slug>" --type keyword --limit 50
yams grep "<pattern>" --cwd .
```

## Handoff Template

```text
TASK: $TASK | MODE: $MODE | PHASE: $PHASE | AGENT: opencode-$TASK
CONTEXT FOUND: <blackboard findings or unavailable>; <YAMS artifact paths/hashes>
ACTIONS: <what changed / verified>
INDEXED: <what was indexed or why indexing failed> (metadata: mode,task,phase,owner,agent_id)
NEXT: <next step>
USED_CONTEXT: <chunk_ids/hashes or unknown>
CITATIONS: <artifacts or none>
```

## Notes

- Repo-specific maps, conventions, and safety overrides live in `AGENTS.md`.
- Keep this file generic and operational; it is reused across repositories.
