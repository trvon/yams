# Security Policy

If you believe you have found a security vulnerability in YAMS, please report it responsibly.

Thank you for taking the time to do that work.

## Reporting
- Please report vulnerabilities privately, not in public tickets.
- Email: `admin@yamsmemory.ai` (PGP optional).
- Include, if you can: affected version or commit, platform, impact, minimal PoC, repro steps, and relevant logs.
- If you are unsure whether something is security-sensitive, email first and we can help triage.

## Response targets
- We aim to acknowledge receipt within 48 hours.
- We aim to triage within 7 days and share next steps.
- For high/critical issues, our target is a fix or mitigation within 90 days; otherwise best effort depending on scope and risk.

## Disclosure
- We prefer coordinated disclosure.
- We will credit reporters unless you ask us not to.
- If warranted, we will request a CVE and share the ID in the advisory.

## Scope
In scope:
- YAMS daemon
- YAMS CLI
- plugins maintained in this repository

Out of scope:
- third-party dependencies (please report those upstream)
- unsupported forks or heavily modified downstream builds

## Supported versions
- `main`
- the latest minor release line
