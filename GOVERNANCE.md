# Project Governance

This document describes how the YAMS project makes decisions and how contributions are handled.

We want the project to be approachable: good-faith questions, issues, pull requests, and design discussion are welcome.

## Scope and Principles
- We aim to keep decision-making lightweight, transparent, and oriented around stability and user value.
- Decisions favor lazy consensus when risk is low and repeatability is high.
- Small and medium contributions should not need heavy process overhead.

## Roles
- Maintainer: Guides overall direction, helps with release approvals, and resolves conflicts when needed.
- Committer: Helps review and merge changes in scoped areas and upholds standards and CI gates.
- Reviewer: Reviews and recommends changes.
- Contributor: Contributes via pull requests — always welcome.
- Security Team: Handles private disclosures and coordinates fixes (see SECURITY.md).
- Release Manager: Leads a release cycle (may rotate).

Currently, `@trvon` serves as Maintainer.

## Decision Process
- Lazy consensus: If no substantive objections are raised within 72 hours of a proposal/PR, it is considered approved, provided CI and expected reviews pass.
- RFCs: Encouraged for material or architectural changes, public interfaces, storage formats, and CI gates. Small fixes, docs changes, and routine improvements generally do not need an RFC.
- Voting: When needed, Maintainer(s) vote. With a single Maintainer, decisions rest with `@trvon`.
- Conflict resolution: The Maintainer helps resolve disputes. If a Technical Steering Committee (TSC) is created in the future, deadlocks escalate to the TSC.

## Ownership and Reviews
- Code ownership is defined by `CODEOWNERS`. Changes benefit from review by owners of affected areas.
- All PRs should pass CI, follow style rules, and include tests/documentation as applicable.
- We ask contributors to include DCO sign-off on commits.
- Review feedback should focus on correctness, maintainability, user impact, and kindness.

## Release Process
- Versioning follows SemVer. User-visible or interface changes should come with CHANGELOG updates and, when applicable, stability notes.
- Release cadence: monthly minor releases, ad-hoc patch releases as needed.
- A Release Manager (default: `@trvon`) coordinates tagging, artifacts, and notes.

## Security
- Private disclosure only. See SECURITY.md for reporting and handling SLAs.

## Amendments
- Amendments to this governance are proposed via PR and approved by the Maintainer.
