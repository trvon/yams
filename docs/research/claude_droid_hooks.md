# Claude Code & Droid Hook References

_Last updated: 2026-01-28_

## Claude Code (code.claude.com/docs/en/hooks)

- **Lifecycle events**: `SessionStart`, `UserPromptSubmit`, `PreToolUse`, `PermissionRequest`, `PostToolUse`, `PostToolUseFailure`, `SubagentStart/Stop`, `Stop`, `PreCompact`, `SessionEnd`, `Notification`.
- **Configuration**: Hooks live in `~/.claude/settings.json`, `.claude/settings.json`, `.claude/settings.local.json`, or managed policy. Structure is `<event> -> matcher -> hooks` where matcher can be literal, regex, `*`, or omitted for events without tool filters.
- **Hook types**: `command` (shell) and `prompt` (Haiku-based evaluation). Prompt hooks supported for `Stop`, `SubagentStop`, `UserPromptSubmit`, `PreToolUse`, `PermissionRequest` to make context-aware decisions.
- **Plugin & component hooks**: Plugins define hooks in `hooks/hooks.json` and reference `${CLAUDE_PLUGIN_ROOT}`. Skills/agents can embed hooks in frontmatter with optional `once` flag.
- **Permissions & decision control**:
  - PreToolUse: `permissionDecision` (`allow`, `deny`, `ask`), `updatedInput`, `additionalContext`.
  - PermissionRequest: `decision.behavior` (`allow`/`deny`), `updatedInput`, `message`, `interrupt` flag.
  - PostToolUse: `decision` (`block`), `reason`, `additionalContext`.
  - UserPromptSubmit: JSON `decision` to block/allow, plus `additionalContext` or plain stdout injection.
  - Stop/SubagentStop: `decision` (`block`) and `reason` to keep working.
- **Input payload**: Hook receives JSON on stdin with session info, event name, tool input/response, transcripts, `permission_mode`, etc. `CLAUDE_ENV_FILE` is provided to `SessionStart`/`Setup` hooks to persist env vars; `CLAUDE_PROJECT_DIR` + `${CLAUDE_PLUGIN_ROOT}` available for script paths.
- **Output semantics**: Exit code `0` = success (stdout hidden except for prompt/session hooks or when `additionalContext` is set), `2` = blocking error (stderr shown to Claude), others = non-blocking. Structured JSON (when exit=0) exposes `continue`, `stopReason`, `suppressOutput`, `systemMessage`, and hook-specific payloads.
- **Execution**: Hooks run in parallel with a 60s default timeout (per hook), deduplicated if identical. Security guidance emphasizes validating inputs, quoting variables, blocking traversal, using absolute paths, and reviewing hooks before enabling.

## Droid / Factory CLI (docs.factory.ai)

- **Lifecycle parity**: Events mirror Claude Code but mapped to Factory defaults—`PreToolUse`, `PostToolUse`, `Notification`, `UserPromptSubmit`, `Stop`, `SubagentStop`, `PreCompact`, `SessionStart`, `SessionEnd`. Permissions use `permission_mode` (`default`, `plan`, `acceptEdits`, `bypassPermissions`).
- **Configuration files**: `~/.factory/settings.json` (user), `.factory/settings.json`, `.factory/settings.local.json`, plus enterprise policy overlays. Hook structure identical: matchers + hook arrays, currently only `command` type.
- **Env variables**: `$FACTORY_PROJECT_DIR` for repo-root relative scripts, `${DROID_PLUGIN_ROOT}` for plugin bundles. Strong guidance to use absolute paths since Droid may change cwd while executing.
- **Plugin hooks**: Same JSON schema as user hooks with optional `description`; automatically merged at runtime.
- **Decision controls**:
  - PreToolUse: `hookSpecificOutput.permissionDecision`, `permissionDecisionReason`, `updatedInput`.
  - PostToolUse: `decision`, `reason`, `hookSpecificOutput.additionalContext`.
  - UserPromptSubmit, Stop/SubagentStop, SessionStart follow same JSON semantics as Claude, with stdout/context injection rules.
- **Hook IO & behavior**: Hooks read stdin JSON (session metadata plus event-specific payload), exit codes match Claude semantics (0 success, 2 blocking). JSON `continue` / `stopReason` fields also supported to halt follow-on workflow. Logging/notification surfaces use `/hooks`, `droid --debug`, transcript (Ctrl-R) to inspect progress.
- **Security**: Same warnings about arbitrary command execution, emphasizing quoting, sanitization, path checks, sensitive file filters, and review of hook changes (Factory snapshots hook config and requires `/hooks` review on modification).

## Key Similarities for Integration

1. **JSON schema**: Both ecosystems stream stdin JSON with `session_id`, `transcript_path`, `cwd`, `permission_mode`, `hook_event_name`, and tool payloads. Export pipeline can treat them uniformly when collecting context.
2. **Exit code semantics**: `0` success, `2` blocking, other codes non-blocking; structured JSON optionally extends behavior when exit=0.
3. **Permission control**: Both have identical fields for gating tool usage (`permissionDecision`, `decision`, `reason`, etc.), allowing shared templates when generating auto hooks.
4. **Project env hints**: `CLAUDE_PROJECT_DIR` vs `FACTORY_PROJECT_DIR` should be surfaced in exported bundles so shell templates can run in either ecosystem.
5. **Security posture**: Both docs stress absolute paths, quoting, and audits; our YAMS hook exporter should surface warnings when bundling sample hooks or recommending scripts for remote agents.

## OpenCode Compaction Hooks

- **Doc**: [opencode.ai/docs/plugins](https://opencode.ai/docs/plugins/) → "Compaction hooks" section (Jan 28, 2026).
- **Events**: Standard session events include `session.created`, `session.compacted`, plus the pre-compaction hook `experimental.session.compacting` that fires **before** the compaction prompt is built.
- **Hook Behavior**: The experimental hook receives `output.context` and optional `output.prompt`; appending strings to `context` injects text into the default compaction prompt, while setting `output.prompt` replaces it entirely.
- **Plugin Guidance**: same doc notes hooking should avoid console output and stresses using absolute paths for scripts.
- **Implication for hook exporter**: When targeting OpenCode’s compaction lifecycle, provide both pre-compaction snippets (for `experimental.session.compacting`) and post-compaction reminders (e.g., `session.compacted`) so agents can load bundles immediately after the window shrinks.
