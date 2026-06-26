# CLI Stub Inventory

This document tracks all CLI commands and options that are currently implemented as stubs or are planned for future releases. This is used for developer coordination and to ensure user-facing documentation remains accurate.

## Phase 2 / Stubbed Commands (Not implemented)

These commands are present in the CLI parser but do not have active implementations.

| Command | Type | Status | Description |
|---------|------|--------|-------------|
| `auth keygen` | Subcommand | Phase 2 | Generate authentication keys |
| `auth list-keys` | Subcommand | Phase 2 | List authentication keys |
| `auth revoke` | Subcommand | Phase 2 | Revoke an authentication key |
| `auth token` | Subcommand | Phase 2 | Generate JWT token |
| `auth api-key` | Subcommand | Phase 2 | Generate API key |

## Partially Implemented / Options Stubs

These are valid commands or options, but specific sub-functionalities are currently stubs.

| Command | Option / Subcommand | Status | Description |
|---------|-------------------|--------|-------------|
| `download` | `--list` | Not implemented | Option to list download jobs |
| `tune` | `replay` | Stub | Replay labeled sessions against available policies (R6) |

---

## Instructions for Developers

- If you are implementing one of the stubs above, update this list and the relevant user documentation.
- If you implement a command that was previously listed as a stub, move it to the "Implemented" section (not currently in this file) and update the user guides.
- Always verify that any new "stubbed" commands are clearly marked in user-facing documentation to avoid confusion.
