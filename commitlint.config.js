// Commits that already landed on experimental with a nonconforming header. Shared history is not
// rewritten, so promotion PRs (which lint every commit since main) skip exactly these headers.
const legacyHeaders = new Set([
  'obs(daemon): time metrics snapshot sections and log slow ones at debug',
  'fix(daemon): RAII exit paths for the metrics and pressure-limited pollers',
]);

module.exports = {
  extends: ['@commitlint/config-conventional'],
  ignores: [(message) => legacyHeaders.has(message.split('\n', 1)[0].trim())],
  rules: {
    // Allowed commit types
    'type-enum': [
      2,
      'always',
      [
        'feat',     // New feature
        'fix',      // Bug fix
        'perf',     // Performance improvement
        'docs',     // Documentation
        'chore',    // Maintenance (deps, CI config, etc.)
        'ci',       // CI/CD changes
        'test',     // Tests
        'refactor', // Code restructuring (no behaviour change)
        'build',    // Build system changes
        'revert',   // Revert a previous commit
        'merge',    // Merge commits
      ],
    ],
    // Allow longer subjects for detailed commit messages
    'header-max-length': [2, 'always', 120],
    // Don't enforce body/footer rules strictly
    'body-max-line-length': [0, 'always', Infinity],
    'footer-max-line-length': [0, 'always', Infinity],
  },
};
