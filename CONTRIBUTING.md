# Contributing to YAMS

Thanks for your interest in YAMS — contributions of all sizes are welcome.

Bug reports, docs fixes, tests, small cleanups, design feedback, and larger features all help. If you're not sure whether something is worth opening, it probably is.

## Start here

- Please be respectful and constructive.
- If you're unsure whether something is a bug or a feature request, open the issue with your best guess.
- If your report is incomplete, that's okay — include what you know and we can help narrow it down.
- For security issues, please do **not** file a public issue. See `SECURITY.md`.

## Reporting bugs and requesting features

We use the GitHub issue templates in `.github/ISSUE_TEMPLATE/` for this repository.

Helpful details include:
- `yams --version`
- platform details
- exact commands or steps
- expected vs actual behavior
- relevant logs or backtraces
- a small repro, if you have one

For feature requests, lead with the problem you're trying to solve. A strong problem statement is often more useful than a fully specified solution.

## Development workflow

The supported local development workflow is:
- **Meson + Conan 2**
- **Ninja**
- **C++20**

Quick debug loop:

```bash
conan install . -of build/debug -s build_type=Debug -b missing
meson setup build/debug \
  --prefix /usr/local \
  --native-file build/debug/build/Debug/generators/conan_meson_native.ini
meson compile -C build/debug
meson test -C build/debug
```

Additional references:
- GCC flow: `docs/BUILD.md`
- build system details: `docs/developer/build_system.md`

If you're making a small docs-only or narrow change, you do not need to run the entire project test matrix. Please run the smallest relevant check you can.

For CI/test speed and stability during local work:

```bash
export YAMS_DISABLE_MODEL_PRELOAD=1
```

## Style and lint

- C/C++ formatting is enforced with `clang-format` (see `.clang-format`).
- Repo-local git hooks live in `.githooks/`.
- Enable them once with:

```bash
git config core.hooksPath .githooks
```

The pre-commit hook formats staged C/C++ files and can run extra lint checks.

## Pull requests

Please aim for changes that are:
- focused
- well-described
- tested at the smallest sensible scope

A helpful PR usually explains:
- what changed
- why it changed
- how it was tested
- any follow-up work or tradeoffs

## AI usage policy

AI tools may be used in an **assistive** role, but we do not want pull requests that are fully or predominantly AI-generated.

If you use AI for any part of a contribution:
- disclose that AI was used and how it was used
- manually review the entire change before opening the PR
- be prepared to explain every part of the submitted change
- make sure the final technical judgment is your own

Examples of acceptable use:
- brainstorming approaches
- drafting small refactors you fully review and revise
- generating repetitive boilerplate from a design you already understand
- improving wording or structure in docs that you then verify

Examples of unacceptable use:
- submitting large, mostly AI-authored patches you do not fully understand
- using AI to answer maintainers or contributors in your place
- using AI to generate bug reports, feature requests, or PR discussion as if it were your own firsthand reasoning

When in doubt, favor smaller PRs, more disclosure, and more human review.

## Commit messages

- Use a concise subject line.
- Add a body when rationale or context would help reviewers.
- Reference tickets when relevant.
- DCO sign-off is encouraged: `Signed-off-by: Your Name <you@example.com>`

## Project process

- Review `GOVERNANCE.md` for roles, decision-making, and releases.
- Material changes should go through the RFC/ADR process when appropriate (`docs/rfcs`, `docs/adrs`).
- `CODEOWNERS` reviews are encouraged where applicable.

## Code of Conduct

We want YAMS to be a welcoming place to collaborate. Please read `CODE_OF_CONDUCT.md`.
