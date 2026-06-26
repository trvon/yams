# Contributing

Thank you for your interest in contributing to YAMS. This guide defines how to propose, implement, and submit changes efficiently and safely.

## Scope and communication

- Use GitHub Issues for bugs and feature requests; provide repro steps and environment.
- For larger changes, open an Issue first to align on scope and approach.
- Security issues: do not open a public issue. Report privately via GitHub Security Advisories or email <support@yamsmemory.ai>.

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
meson setup build/asan --buildtype debug -Denable-asan=true -Db_sanitize=address -Db_lundef=false
meson compile -C build/asan
meson test -C build/asan

meson setup builddir --buildtype debug -Denable-tsan=true -Db_sanitize=thread -Db_lundef=false
meson compile -C builddir
meson test -C builddir
```

## AI usage policy

AI tools may be used in an **assistive** role, but we do not want pull requests that are fully or predominantly AI-generated.

If you use AI for any part of a contribution:

- Disclose that AI was used and how it was used.
- Manually review the entire change before opening the PR.
- Be prepared to explain every part of the submitted change.
- Make sure the final technical judgment is your own.

**Examples of acceptable use:**

- Brainstorming approaches.
- Drafting small refactors you fully review and revise.
- Generating repetitive boilerplate from a design you already understand.
- Improving wording or structure in docs that you then verify.

**Examples of unacceptable use:**

- Submitting large, mostly AI-authored patches you do not fully understand.
- Using AI to answer maintainers or contributors in your place.
- Using AI to generate bug reports, feature requests, or PR discussion as if it were your own firsthand reasoning.

When in doubt, favor smaller PRs, more disclosure, and more human review.

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
  - Builds clean (Debug/Release) with no new warnings
  - Tests pass locally (include negative and edge cases)
  - clang-format applied; static analysis warnings addressed or justified
  - Dependencies unchanged or properly updated via Conan
  - Performance impact assessed for hot paths (if relevant)
- CI must be green before review/merge.

## Coding standards (C++)

- C++20, modern idioms (RAII, smart pointers, algorithms).
- Enforce const-correctness; prefer std::optional/variant over sentinel values.
- Avoid raw owning pointers; prefer value types or std::unique_ptr/std::shared_ptr.
- Error handling: exceptions for exceptional failures; clear error messages.
- Naming:
  - Classes/Types: PascalCase
  - Methods/Variables: camelCase
  - Constants: kPascalCase
  - Member fields: trailing `_` (e.g. `member_`)
  - Files: snake_case
- Structure: headers (*.hpp) for interfaces; implementations (*.cpp) separate.
- Formatting: clang-format (project config). No unrelated formatting churn.

## Testing

- Add unit tests for new code and regressions; test error paths and edge cases.
- Keep tests deterministic and isolated; avoid external network calls.
- For async/background work, include timing-safe assertions or fakes.
- Coverage is local-only now. Enable the tracked hooks with
  `git config core.hooksPath .githooks`; the pre-push hook runs
  `scripts/run-local-coverage.sh` for pushed C/C++ / Meson changes, prefers
  ccache plus a fast linker when available, and prints a `gcovr` summary.
  Default mode is unit-only for speed; add
  `YAMS_COVERAGE_INCLUDE_INTEGRATION=1` when you need integration coverage.
  Temporary bypass: `YAMS_SKIP_COVERAGE_HOOK=1 git push`.
- Optimize test runtime by reducing repeated setup and monolithic suite shape,
  not by deleting correctness checks. Slow stress/soak, migration,
  diagnostics-heavy, and long-wait coverage should keep an explicit suite or
  binary instead of living in the default fast path.
- When refactoring tests, prefer seeded fixtures, pre-migrated DB helpers,
  narrower binaries, and deterministic synchronization primitives over repeated
  bootstrap, broad `SECTION()` fan-out, or fixed sleeps.
- If a binary becomes a TSAN bottleneck, split it by behavior cluster so Meson
  can parallelize it while preserving failure locality.
- See [`docs/developer/testing.md`](testing.md) for the detailed suite-shaping
  policy and validation expectations.

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
- Report vulnerabilities privately (GitHub Security Advisories or <support@yamsmemory.ai>).

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
- Keep the final merge or squash title concise and user-facing when the change should appear in release notes; `release-please` uses commit subjects as changelog input.
- Post-merge: maintainers may follow up with minor style/docs adjustments for consistency.

## Release notes

- For user-visible changes, include a 1-2 bullet release note in the PR description.
- Focus on user-visible behavior, impact, and migration steps; do not restate CI, refactors, test-only work, or broad implementation churn.
- Prefer concise conventional subjects such as `feat:`, `fix:`, `perf:`, or `security:` over checkpoint-style commit titles.
- Mention migration steps if behavior or configuration changes.

Thank you for keeping contributions precise, tested, and well-documented.
