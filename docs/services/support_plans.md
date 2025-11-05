---
title: Support Plans
---

# Support Plans

Open source first. For guaranteed response times and guidance, we offer paid support for self‑hosted YAMS (and later, managed hosting).

!!! note
    Managed hosting is in private preview. Hosted SLAs will be published for GA. These plans cover self‑hosted YAMS today.

See also:
- Services: [Services & Offerings](README.md)
- Managed Hosting: [Managed Hosting](../hosting/managed.md)

---

## Plan tiers (preview)

### Community (Free)
- Channel: GitHub issues/discussions
- SLA: best‑effort
- For: individuals, experimentation, non‑critical projects

### Standard
- Channel: Email • Hours: business (UTC), Mon–Fri
- Targets: SEV‑2 next business day; SEV‑3/4 within 2 business days
- Coverage: troubleshooting, usage guidance, bug triage

### Professional
- Channels: Email + private Slack/Teams (1 workspace) • Hours: business (UTC)
- Targets: SEV‑1 4h; SEV‑2 same day; SEV‑3/4 next day
- Plus: quarterly architecture review; upgrade guidance; performance advice

### Enterprise
- Channels: Email + Slack/Teams + optional on‑call
- Hours: 24×7 for SEV‑1/2; business hours otherwise
- Targets: SEV‑1 1h; SEV‑2 2h; SEV‑3 next day; SEV‑4 2 business days
- Plus: named contact; PIRs; monthly roadmap sync; custom SLAs/SLOs (MSA/SOW)

!!! tip
    Final SLAs/SLOs and pricing are defined in the MSA/SOW.

---

## Severity levels

- SEV‑1: Production outage or critical loss; no workaround.
- SEV‑2: Major impairment or performance issues; workaround exists.
- SEV‑3: Partial loss of non‑critical functionality; limited impact.
- SEV‑4: Questions, docs, cosmetic issues.

---

## Included

- Troubleshooting and bug triage for YAMS core and supported integrations
- Best practices: storage layout, collections/snapshots, search config
- Upgrade compatibility guidance
- Performance tuning suggestions
- Ops guidance: backup/restore, health checks, observability
- Security recommendations: hardening, portability, isolation

## Out of scope

- Custom feature development (separate Services engagement)
- Unsupported forks/unreleased patches
- Deep debugging of unrelated infra (OS/network/third‑party DBs)
- Running your SRE operations (advisory only unless contracted)
- Date‑certain bug fixes (except by Enterprise SOW)

---

## Hours and channels

- Business hours: 09:00–18:00 UTC, Mon–Fri (excl. holidays)
- Community: GitHub • Standard: Email • Professional: Email + Slack/Teams • Enterprise: + on‑call (SEV‑1/2)

---

## Onboarding

1. Kickoff: environments, contacts, incident paths
2. Discovery (Pro/Ent): architecture, scale, SLAs
3. Runbooks: backup/restore, health checks, upgrades

---

## Reporting

- Standard: ticket history on request • Professional: quarterly summary • Enterprise: monthly report + roadmap alignment

---

## Versioning and upgrades

- Release notes with compatibility guidance
- Pin minors in prod; test upgrades in staging
- Migrations documented when needed; we’ll review Pro/Ent plans

---

## Data protection

- No access to customer data required
- Provide minimal, redacted logs if needed
- NDAs/DPAs available (Pro/Ent); least‑privilege for any access

---

## With Managed Hosting

- Complements in‑house ops for self‑hosted
- Managed (GA) includes baseline SLA; Enterprise upgrades available
- Migration guidance for self‑hosted ↔ managed

---

## FAQs

- Community SLAs? No — best‑effort via GitHub.
- Switch tiers mid‑term? Yes, by agreement.
- One‑off advisory hours? Yes (see Services).
- Usage‑based pricing? Support is subscription; hosting is metered.
- Sign our MSA/security review? Enterprise: yes.

---

## Get started

- Contact: [Services Contact](contact.md)
- Want zero‑ops? See [Managed Hosting](../hosting/managed.md)

---

This page is non‑binding and subject to change. Contractual terms in your MSA/SOW take precedence over this summary.
