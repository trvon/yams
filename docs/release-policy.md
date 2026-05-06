# Release Policy

- Versioning: Semantic Versioning (SemVer)
- Cadence: monthly minor releases; patch releases as needed
- Branching: `main` (active), tags `vMAJOR.MINOR.PATCH`
- Requirements to cut release:
  - CI passing
  - CHANGELOG updated
  - Stability notes updated if public interfaces change
  - Release notes drafted
- Responsibilities: Release Manager (default: @trvon)

## Release note shape

- GitHub release pages should summarize the current version only, not paste the full `CHANGELOG.md`.
- Prefer a short structure: highlights, breaking or migration notes, and notable fixes.
- Keep release notes user-facing. Omit CI, test-only, refactor-only, and docs-only churn unless it changes user or operator behavior.
- Link the full `CHANGELOG.md` from the release page for detailed history.
