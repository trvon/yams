# Project Governance

This document defines how decisions are made for the YAMS project and who is responsible for them.

## Scope and Principles
- The goal is to keep decision-making lightweight, transparent, and oriented around stability and user value.
- Decisions favor lazy consensus when risk is low and repeatability is high.

## Roles
- Maintainer: Responsible for overall direction, release approvals, resolving conflicts, and final decisions.
- Committer: May merge changes in scoped areas, upholds standards and CI gates.
- Reviewer: Reviews and recommends changes.
- Contributor: Contributes via pull requests.
- Security Team: Handles private disclosures and coordinates fixes (see SECURITY.md).
- Release Manager: Leads a release cycle (may rotate).

At present, the sole Maintainer and Owner is: `@trvon`.

## Decision Process
- Lazy consensus: If no substantive objections are raised within 72 hours of a proposal/PR, it is considered approved, provided CI and required reviews pass.
- RFCs: Required for material/architectural changes, public interfaces, storage formats, and CI gates. RFCs are opened under `docs/rfcs` and discussed via PR.
- Voting: When needed, Maintainer(s) vote. With a single Maintainer, decisions default to `@trvon`.
- Conflict resolution: The Maintainer adjudicates. If a Technical Steering Committee (TSC) is created in the future, deadlocks escalate to the TSC.

## Ownership and Reviews
- Code ownership is defined by `CODEOWNERS`. Changes require review from owners of affected areas.
- All PRs must pass CI, follow style rules, and include tests/documentation as applicable.
- DCO sign-off is required for all commits.

## Release Process
- Versioning follows SemVer. User-visible or interface changes require CHANGELOG updates and, when applicable, stability PBI updates.
- Release cadence: monthly minor releases, ad-hoc patch releases as needed.
- A Release Manager (default: `@trvon`) coordinates tagging, artifacts, and notes.

## Security
- Private disclosure only. See SECURITY.md for reporting and handling SLAs.

## Amendments
- Amendments to this governance require Maintainer approval via PR.
