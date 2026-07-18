---
description: Codex engineering/bug-bounty prompt with YAMS-first distributed memory
argument-hint: "[TASK=<description>] [MODE=<engineering|bug-bounty>] [PHASE=<start|checkpoint|complete>]"
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

YAMS search/list/grep returns scoped snippets and candidate identifiers; those
results are discovery, not recovered memory. Hydrate the selected saved-memory
artifacts before relying on them. Local exploration re-reads whole files the
index already knows. Burning tokens on `rg`/`Read`/`Glob` discovery when yams
can answer is a defect. Pick by question type:

| Question | Tool (first hop) |
|---|---|
| Exact symbol / string / pattern | `yams grep "<pat>" --cwd .` |
| Callers / callees / includes / blast radius / related tests | `yams graph --explore "<symbol-or-file>" --max-files 8` |
| Concept / prior decision / task history | `yams search "<query>" --limit 10` |
| Inspect a selected saved-memory artifact | run the emitted `yams cat --hash <hash>` hint |
| Export a selected artifact | `yams get --hash <hash> -o <path>` |
| Exact implementation detail in a file yams already named | local `Read` |
| Unfamiliar subsystem entry point | `yams graph --topology-clusters`, then `--cluster <id>` |
| Precise edges | `yams graph --name <file> --depth 1 --limit 50`, `--node-key <key> -r <relation>` |

Rules:

- Search, list, and grep only select candidates. Before using a prior note,
  decision, research item, or evidence item, hydrate the most relevant one to
  three hits with `yams cat --hash <hash>`. Do not claim recovered context from
  snippets alone.
- Do not `cat` every code hit. Use `graph --explore` to narrow code context,
  then locally read only the exact implementation detail that remains needed.
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
1. **Retrieve + hydrate** per the contract above before any local exploration.
2. **Start**: check index health and store a concise claim.
   ```bash
   yams status

   # Only if the repo index is missing/stale; do not task-tag a healthy repo.
   yams add . --recursive --include "*.cpp,*.hpp,*.h,*.py,*.ts,*.js,*.md" \
     --label "Repository baseline" \
     --metadata "mode=$MODE,task=repo-index,phase=checkpoint,owner=opencode,source=code"

   yams add - --name "claim-$TASK.md" \
     --metadata "mode=$MODE,task=$TASK,phase=start,owner=opencode,source=note,agent_id=opencode-$TASK"
   ```
   Send `TASK`, `SCOPE`, `GOAL`, and `NEXT` directly on stdin. In restricted
   shells, invoke `yams` directly and use the executor's stdin channel; a
   `printf | yams` wrapper can change command-prefix sandbox authorization.
3. **Act**: implement/test/verify in scope; keep notes concise and indexable.
   For C++/systems work follow the design-first TDD loop in `AGENTS.md`.
4. **Checkpoint**: index changed files and a concise `source=note` record of
   decisions, open questions, and the next action.
5. **Complete**: store the completed Handoff Template as a `source=note`,
   `phase=complete` artifact, then re-index changed files with
   `phase=complete` metadata.

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
# Select exact task memories, then hydrate the relevant artifacts.
yams list --format json --show-metadata \
  --metadata "owner=opencode" --metadata "task=<task>" \
  --metadata "source=note" --limit 10
yams cat --hash <selected-note-decision-research-or-evidence-hash>

# Fall back to concept/pattern discovery, then execute the emitted cat/graph hint.
yams search "<task concept or prior decision>" --limit 10
yams grep "<pattern>" --cwd .
```

Recovery is incomplete until selected saved-memory artifacts have been
hydrated. Record their hashes in `USED_CONTEXT` so the handoff is reproducible.
Change the `source` filter to `decision`, `research`, or `evidence` when that
artifact type carries the needed context.

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
