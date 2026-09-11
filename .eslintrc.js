/**
 * ESLint config for the Dataform repository.
 */
module.exports = {
  root: true,
  ignorePatterns: [
    'bazel-*',
    'node_modules',
    'tests/api/projects/**',
    'tests/integration/*_project/**',
  ],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2020,
    sourceType: 'module',
  },
  env: {
    es2020: true,
    node: true,
  },
  rules: {
    'prefer-const': 'error',
    'no-var': 'error',
    'no-debugger': 'error',
    'no-duplicate-imports': 'error',
    'no-empty': ['error', { allowEmptyCatch: true }],
    'curly': 'error',
    'eqeqeq': ['error', 'smart'],
    'no-unused-expressions': 'error',
    'no-console': 'error',
    'no-eval': 'error',
    'no-throw-literal': 'error',
    'radix': 'error',
    'no-cond-assign': 'error',
    'no-unsafe-finally': 'error',
    'no-caller': 'error',
    'use-isnan': 'error',
    'no-self-assign': 'error',
    'no-shadow-restricted-names': 'error',
    'ordered-imports': 'error',
    'no-new-func': 'error',
  },
  overrides: [
    {
      // JIT compiler dynamically creates executable functions in the V8 sandbox
      files: ['core/jit_compiler.ts'],
      rules: {
        'no-new-func': 'off',
      },
    },
    {
      // core/ runs inside the V8 compilation sandbox — no Node built-ins.
      files: ['core/**/*.ts'],
      rules: {
        'no-node-builtins': 'error',
      },
    },
    {
      // Ambient declarations for webpack internals
      files: ['core/utils.ts', 'core/workflow_settings.ts'],
      rules: {
        'no-var': 'off',
      },
    },
    {
      // Tests, testing helpers, examples, CLI console wrapper, VSCode extension, and common promises logger may use console
      files: [
        '**/*_test.ts',
        '**/*.test.ts',
        'testing/**/*.ts',
        'examples/**/*.ts',
        'cli/console.ts',
        'vscode/**/*.ts',
        'common/promises/index.ts',
      ],
      rules: {
        'no-node-builtins': 'off',
        'no-console': 'off',
      },
    },
  ],
};
