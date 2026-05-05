# YAMS OpenGrep rules

Repo-owned OpenGrep/Semgrep-compatible rules live under `tools/opengrep/rules`.

Profiles:

- `rules/default/`: low-noise hardening checks appropriate for local hooks.
- `rules/audit/`: broader audit checks for security/correctness review. Expect triage.

Run the full local rule set:

```bash
opengrep --config tools/opengrep/rules/default --config tools/opengrep/rules/audit src include
```

Run rule tests:

```bash
# C++ security rules
opengrep test --config tools/opengrep/rules/audit/yams-cpp-security-audit.yml \
  tools/opengrep/tests/cpp/yams_cpp_security_audit.cpp

# GitHub Actions CI security rules
opengrep test --config tools/opengrep/rules/audit/yams-github-actions-audit.yml \
  tools/opengrep/tests/github-actions/yams_github_actions_audit.yml
```

When adding rules, include `ruleid` and `ok` cases under `tools/opengrep/tests/` and keep
patterns repo-specific enough to be actionable.
