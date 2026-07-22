/**
 * Custom ESLint rule: disallow Node.js built-in imports in code that runs
 * inside the Dataform V8 compilation sandbox (i.e. `core/`).
 *
 * The blocked set is generated at rule load time from Node's own
 * `require('module').builtinModules`, so any built-in Node adds later is
 * caught automatically. Both bare (`path`) and prefixed (`node:path`) forms
 * are covered.
 */
'use strict';

const { builtinModules } = require('module');

const blocked = new Set([
  ...builtinModules,
  ...builtinModules.map((m) => `node:${m}`),
]);

function report(context, node, name) {
  context.report({
    node,
    messageId: 'nodeBuiltin',
    data: { name },
  });
}

module.exports = {
  meta: {
    type: 'problem',
    docs: {
      description:
        'Disallow Node.js built-in imports in Dataform V8 sandbox code.',
    },
    schema: [],
    messages: {
      nodeBuiltin:
        "The Node.js built-in module '{{name}}' is not available in the " +
        'Dataform V8 compilation sandbox. Move code that requires Node ' +
        'built-ins to cli/, which runs on the host Node.js runtime and does ' +
        'not have this restriction.',
    },
  },
  create(context) {
    return {
      // import x from 'path' | import * as x from 'path' | import 'path'
      ImportDeclaration(node) {
        const name = node.source.value;
        if (blocked.has(name)) report(context, node, name);
      },
      // import x = require('path')
      TSImportEqualsDeclaration(node) {
        const ref = node.moduleReference;
        if (
          ref &&
          ref.type === 'TSExternalModuleReference' &&
          ref.expression &&
          typeof ref.expression.value === 'string' &&
          blocked.has(ref.expression.value)
        ) {
          report(context, node, ref.expression.value);
        }
      },
      // require('path') or require('node:path')
      CallExpression(node) {
        if (
          node.callee.type === 'Identifier' &&
          node.callee.name === 'require' &&
          node.arguments.length === 1 &&
          node.arguments[0].type === 'Literal' &&
          typeof node.arguments[0].value === 'string' &&
          blocked.has(node.arguments[0].value)
        ) {
          report(context, node, node.arguments[0].value);
        }
      },
    };
  },
};
