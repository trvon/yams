# Contributing

Thank you for your interest in contributing to YAMS. This guide defines how to propose, implement, and submit changes efficiently and safely.

## Scope and communication
- Use GitHub Issues for bugs and feature requests; provide repro steps and environment.
- For larger changes, open an Issue first to align on scope and approach.
- Security issues: do not open a public issue. Report privately via GitHub Security Advisories or email support@yamsmemory.ai.

## Development workflow (TL;DR)
1. Fork and create a feature branch: feature/<short-topic>, fix/<short-topic>, or doc/<short-topic>.
2. Build and test locally (Debug for iteration, Release for validation).
3. Keep PRs focused (one topic per PR). Write tests and docs alongside code.
4. Ensure CI passes; address review feedback promptly.

### Build and test

**Linux/macOS:**
```bash
# Release
./setup.sh Release
meson compile -C build/release
meson test -C build/release --print-errorlogs

# Debug
./setup.sh Debug
meson compile -C builddir
meson test -C builddir --print-errorlogs
```

**Windows:**
```pwsh
# Release
./setup.ps1 Release
meson compile -C build/release
meson test -C build/release --print-errorlogs

# Debug
./setup.ps1 Debug
meson compile -C builddir
meson test -C builddir --print-errorlogs
```

**Sanitizers (Debug, Linux/macOS):**
```bash
CFLAGS='-fsanitize=address,undefined' CXXFLAGS='-fsanitize=address,undefined' \
  meson setup builddir --reconfigure
meson compile -C builddir
meson test -C builddir
```

## Commit messages
- Use imperative mood, present tense. Keep subject ≤ 72 chars.
- Reference issues with closes #NNN or refs #NNN when applicable.
- Example: storage: fix crash on empty blob import (closes #123)

## Pull requests
- Keep changes minimal and atomic; avoid mixing refactors with behavioral changes.
- Include:
  - Rationale and scope
  - Implementation notes (assumptions, tradeoffs)
  - Tests added/updated
  - Docs updated (if user-visible change)
- Checklist before opening:
  - [ ] Builds clean (Debug/Release) with no new warnings
  - [ ] Tests pass locally (include negative and edge cases)
  - [ ] clang-format applied; static analysis warnings addressed or justified
  - [ ] Dependencies unchanged or properly updated via Conan
  - [ ] Performance impact assessed for hot paths (if relevant)
- CI must be green before review/merge.

## Coding standards (C++)
- C++17/20, modern idioms (RAII, smart pointers, algorithms).
- Enforce const-correctness; prefer std::optional/variant over sentinel values.
- Avoid raw owning pointers; prefer value types or std::unique_ptr/std::shared_ptr.
- Error handling: exceptions for exceptional failures; clear error messages.
- Naming:
  - Classes/Types: PascalCase
  - Methods/Variables: camelCase
  - Constants/Macros: SCREAMING_SNAKE_CASE
  - Member fields: _name or m_name
- Structure: headers (*.hpp) for interfaces; implementations (*.cpp) separate.
- Formatting: clang-format (project config). No unrelated formatting churn.

## Testing
- Add unit tests for new code and regressions; test error paths and edge cases.
- Keep tests deterministic and isolated; avoid external network calls.
- For async/background work, include timing-safe assertions or fakes.
- Use coverage tools locally to validate critical-path coverage when feasible.

## Documentation
- Update or add docs for user-facing changes:
  - User guides (docs/user_guide/*)
  - API (docs/api/*)
  - Admin/Operations (docs/admin/*, docs/operations/*)
  - Developer docs (docs/developer/*)
- Keep docs concise and technical; avoid marketing language.

## Dependencies (Conan)
- Update dependencies in the conanfile and re-run conan install for each config.
- Pin exact versions where possible; prefer reproducible builds.
- Avoid introducing new dependencies unless necessary; justify in the PR.
- If a lockfile is used in CI, update it as part of the PR.

## Performance-sensitive changes
- Benchmark hot paths; include before/after numbers, method, and environment.
- Avoid unbounded memory growth; prefer streaming and bounded caches.
- Use constexpr and move semantics to minimize overhead when appropriate.

## Security
- Do not log sensitive data.
- Validate inputs at boundaries; prefer safe containers over raw buffers.
- Report vulnerabilities privately (GitHub Security Advisories or support@yamsmemory.ai).

## Licensing
- YAMS is GPL-3.0-or-later. Ensure new files include an SPDX header when applicable:
```text
SPDX-License-Identifier: GPL-3.0-or-later
```
- Only contribute code you are authorized to submit under the project license.

## Code review and merge
- Expect focused, technical feedback (correctness, clarity, safety, performance).
- Address comments with additional commits; avoid force-pushing during review unless asked.
- Maintainers will choose the merge strategy (squash or rebase) to keep history clean.
- Post-merge: maintainers may follow up with minor style/docs adjustments for consistency.

## Release notes
- For user-visible changes, propose a concise entry for the changelog/release notes in the PR description.
- Mention migration steps if behavior or configuration changes.

Thank you for keeping contributions precise, tested, and well-documented.
