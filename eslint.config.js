/**
 * ESLint flat config for the Dataform repository.
 */
const rawTsParser = require('@typescript-eslint/parser');
const noNodeBuiltins = require('./eslint-rules/no-node-builtins');
const orderedImports = require('./eslint-rules/ordered-imports');

// Compatibility shim for @typescript-eslint/parser v5 with ESLint v10
const tsParser = {
  ...rawTsParser,
  parseForESLint(code, options) {
    const result = rawTsParser.parseForESLint(code, options);
    if (result.scopeManager && !result.scopeManager.addGlobals) {
      result.scopeManager.addGlobals = (names) => {
        for (const name of names) {
          if (!result.scopeManager.globalScope.set.has(name)) {
            result.scopeManager.globalScope.defineImplicitVariable(name, {
              isTypeVariable: false,
              isValueVariable: true,
            });
          }
        }
      };
    }
    return result;
  },
};

module.exports = [
  {
    ignores: [
      'node_modules/**',
      'bazel-*/**',
      'dist/**',
      'tmp/**',
      'tests/api/projects/**',
      'tests/integration/*_project/**',
    ],
  },
  {
    files: ['**/*.ts'],
    languageOptions: {
      parser: tsParser,
      ecmaVersion: 2020,
      sourceType: 'module',
    },
    plugins: {
      local: {
        rules: {
          'no-node-builtins': noNodeBuiltins,
          'ordered-imports': orderedImports,
        },
      },
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
      'local/ordered-imports': 'error',
      'no-new-func': 'error',
    },
  },
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
      'local/no-node-builtins': 'error',
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
      'local/no-node-builtins': 'off',
      'no-console': 'off',
    },
  },
];
