# Contributing to YAMS

Thanks for your interest in contributing!

## Bug reports and features
- Tracker: SourceHut tickets (GitHub Issues will be disabled once SourceHut is stable).
- Filing URL: https://sr.ht/~trvon/yams/trackers
- Use the ticket template in `TICKET_TEMPLATE.md` for bug reports and feature requests.
- Please include: version/commit, platform, repro steps, expected/actual behavior, logs. For security issues, see SECURITY.md.

## Development workflow
- Toolchain: CMake + Conan 2, C++20
- Build: `cmake --preset yams-release && cmake --build --preset yams-release`
- Tests: `ctest --test-dir build/yams-release`
- For CI/test speed and stability: `export YAMS_DISABLE_MODEL_PRELOAD=1`

## Style and lint
- C/C++ formatting: clang-format (see .clang-format). Use our pre-commit hook.
- Optional: clang-tidy (enable via `YAMS_LINT_TIDY=1` in the pre-commit hook).

## Git hooks
- Repo-local hooks live under `.githooks/`. Enable once: `git config core.hooksPath .githooks`
- Pre-commit runs clang-format on staged C/C++ files and can run additional lint.

## Commit messages
- Use concise subject lines; add body with rationale and context when needed.
- Reference tickets (e.g., `sr.ht#123`) when relevant.

## Security
- Please don’t open public tickets for vulnerabilities. See SECURITY.md.

## Code of Conduct
- We follow the Contributor Covenant. See CODE_OF_CONDUCT.md.

---

## Governance and Process
- Please review GOVERNANCE.md for roles, decision process, and releases.
- Material changes require an RFC (`docs/rfcs`) and, once accepted, an ADR (`docs/adrs`).
- All commits must include DCO sign-off: add a line `Signed-off-by: Your Name <you@example.com>` to your commit message.
- CODEOWNERS reviews are required; see `.github/CODEOWNERS`.
