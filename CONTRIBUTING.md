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

- build and toolchain flow: `docs/BUILD.md`

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

## AI-assisted contributions

AI tools are welcome in YAMS development. Use them to explore the codebase,
prototype an approach, draft documentation, or help with repetitive work.

The contributor still owns the result. Before opening a pull request:

- review the full change
- run the smallest relevant checks
- understand the behavior and tradeoffs well enough to discuss them
- describe material AI assistance in the PR when it helps reviewers understand the work

Do not submit changes you have not reviewed or cannot explain. Keep bug reports,
feature requests, and review discussions grounded in your own observation,
testing, and judgment.

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
