/**
 * ESLint config for the Dataform CLI & Core repo.
 *
 * Scope: currently only used to enforce sandbox-safety rules in `core/`.
 * The rest of the codebase is still linted by tslint (see tslint.json).
 * Add ESLint rules here only when they need capabilities tslint 5.17 cannot
 * express (per-file `overrides`, per-message text, AST rules, etc.).
 */
module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2020,
    sourceType: 'module',
  },
  overrides: [
    {
      // core/ runs inside the V8 compilation sandbox — no Node built-ins.
      files: ['core/**/*.ts'],
      rules: {
        'no-node-builtins': 'error',
      },
    },
    {
      // Tests under core/ run on the host Node runtime, not in the sandbox,
      // so they may legitimately use fs, path, etc. for fixture setup.
      files: ['core/**/*_test.ts', 'core/**/*.test.ts'],
      rules: {
        'no-node-builtins': 'off',
      },
    },
  ],
};
