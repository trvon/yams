---
name: Bug report
about: Create a report to help us improve
title: bug
labels: ''
assignees: trvon

---

- Summary: One clear sentence of the problem
- Version/Commit: output of `yams --version` and commit SHA (if built from source)
- Platform: OS/distro, kernel, CPU, compiler (if relevant)
- Environment:
  - Data dir snapshot: paste `yams daemon status -d` (include SVC and WAIT lines)
  - Env vars: relevant `YAMS_*` (e.g., `env | grep ^YAMS_`)
- Steps to Reproduce:
  1. exact commands
  2. minimal repo/files if needed
- Expected Result: what you expected to happen
- Actual Result: what happened instead
- Logs:
  - Daemon log (last 200 lines): `tail -n 200 ~/.local/state/yams/daemon.log`
  - CLI output (add `YAMS_CLIENT_DEBUG=1` if helpful)
- Attachments: small samples, config snippets, backtrace (if crash)
- Regression? yes/no; if yes, from which version
- Severity/Priority: S1 Critical | S2 Major | S3 Minor | S4 Trivial (suggest)
- Component: daemon | cli | search | vector | model | docs | ci | packaging
- Workarounds tried: any temporary workaround you found

Security Note: Do NOT file security issues here. Email security@yams.dev (see SECURITY.md).
