# Source mirroring

GitHub (`github.com/trvon/yams`) is the canonical YAMS source repository. The
Forgejo repository at `git.trevon.dev/trevon/yams` is a one-way availability
mirror; it is not a second source of truth.

## Mirrored refs

The `Mirror source to Forgejo` GitHub workflow mirrors only:

- `refs/heads/main`
- `refs/heads/experimental`
- release tags beginning with `v` or `yams-v`

Each run fetches one approved GitHub ref into a temporary local ref, pushes one
explicit source-to-destination refspec, and compares the Forgejo ref SHA with
the fetched GitHub SHA. Runs are serialized. The workflow does not prune,
force-update, or mirror every ref.

A manual dispatch accepts only `main`, `experimental`, or an approved release
tag. Use it to reconcile an allowed ref after a transient outage.

## Required secrets

Configure this GitHub Actions secret in the canonical repository:

- `FORGEJO_MIRROR_SSH_KEY`: private half of a dedicated Forgejo deploy key with
  write access only to `trevon/yams`.

The workflow currently disables SSH host-key verification, so possession of the
deploy key authenticates GitHub Actions to Forgejo but the runner does not
authenticate the Forgejo endpoint. This accepts DNS/network man-in-the-middle
risk and should be replaced with a pinned host key if that risk changes. The
deploy key must not be reused for package publication, administration, or
another repository.

## Initial reconciliation

1. Compare the approved GitHub and Forgejo branch SHAs.
2. Confirm the Forgejo branch is an ancestor of, or equal to, the canonical
   GitHub branch.
3. Install the deploy-key secret above.
4. Dispatch one allowed branch at a time, starting with `main`.
5. Confirm the workflow's post-push SHA verification succeeds.
6. Reconcile approved release tags only after both branches are verified.

The workflow must be present on GitHub's default branch before relying on its
push and manual-dispatch triggers.

## Failure recovery

A rejected non-fast-forward update indicates drift; it is not permission to
force-push. Stop automation and preserve both SHAs. Determine whether Forgejo
contains unique commits, then restore it through an explicitly reviewed
operator action. Never add automatic pruning or a repository-wide mirror push
to recover from drift.

If SSH authentication or host verification fails, rotate or correct the
repository-scoped deploy key or pinned host entry, then manually dispatch the
same approved ref. If the push succeeds but SHA verification fails, treat the
run as failed and inspect the exact destination ref before retrying.
