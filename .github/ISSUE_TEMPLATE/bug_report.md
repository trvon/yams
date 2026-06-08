---
name: Bug report
about: Help us reproduce and fix a problem
title: "[bug]: "
labels: ''
assignees: trvon

---

Thanks for taking the time to report this. If you don't know every answer below, that's okay — fill in what you can.

> Please write reports from your own firsthand experience. AI-assisted editing is okay if you verify every detail, but don't submit generated reports you have not personally checked.

## Summary
A short description of the problem.

## Version
- `yams --version`:
- commit SHA (if built from source):

## Platform
- OS / distro:
- kernel:
- CPU / architecture:
- compiler (if relevant):

## Environment
- Relevant `YAMS_*` variables:
- `yams daemon status -d` output (especially `SVC` / `WAIT` lines, if relevant):

## Steps to reproduce
1.
2.
3.

## Expected behavior
What did you expect to happen?

## Actual behavior
What happened instead?

## Logs or output
Include anything useful, for example:
- CLI output
- `YAMS_CLIENT_DEBUG=1` output
- last ~200 lines of the daemon log
- backtrace / crash report

## Repro data or attachments
If possible, attach a small sample, config snippet, or minimal repro.

## Extra context
- Regression? If yes, from which version?
- Component: daemon | cli | search | vector | model | docs | ci | packaging
- Workarounds tried:
- Suggested severity: S1 Critical | S2 Major | S3 Minor | S4 Trivial

> Security note: please report vulnerabilities privately — see `SECURITY.md`.
