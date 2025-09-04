# Running GitHub Actions locally with Act

This repo is configured to run the main CI workflow locally using Docker + Act.

## Prerequisites
- Docker running
- GitHub CLI: `gh auth login`
- Act extension: `gh extension install nektos/gh-act` (alternatively install `act` binary)

## Quick start

- List workflows/jobs:
  ```bash
  gh act -l
  ```
- Run all push jobs (reuses containers):
  ```bash
  make ci-local
  ```
- Run a specific workflow file:
  ```bash
  make ci-workflow WORKFLOWS=.github/workflows/ci.yml
  ```
- Run a single job:
  ```bash
  make ci-job J=build-and-test
  ```

## Configuration

- `.actrc` maps `ubuntu-latest`/`ubuntu-22.04` to an Act-compatible image:
  - `ghcr.io/catthehacker/ubuntu:act-22.04`
- Cache directory for `actions/cache` is `.act/cache`. The Makefile sets `ACT_CACHE_DIR` accordingly.
- Secrets for local runs can be provided with `-s` flags, or via a `.secrets` file (never commit secrets).

## Tips

- Speed up runs: `--reuse` keeps containers around between runs.
- Narrow matrix: `gh act push -j <job> --matrix 'compiler=gcc,build_type=Debug'`.
- Simulate payload: `gh act push -e ./event.json`.
- Use smaller images: append `:small` to the mapped image in `.actrc` if pulls are slow.

## Limitations

- macOS/Windows runners are not available locally.
- Some marketplace actions may behave differently than GitHub-hosted runners.
