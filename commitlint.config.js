module.exports = {
  extends: ['@commitlint/config-conventional'],
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
