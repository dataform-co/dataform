/**
 * Custom ESLint rule: enforce ordered imports matching Dataform's TSLint configuration.
 *
 * Imports are grouped into:
 *   1. External modules (Node built-ins, third-party packages, relative imports)
 *   2. Internal modules (prefixed with 'df/')
 *
 * External imports must appear before internal imports, and imports within each group
 * must be sorted alphabetically by path (ignoring leading relative path prefixes).
 */
'use strict';

function cleanPath(p) {
  return p.replace(/^(\.\/|\.\.\/)+/, '').toLowerCase();
}

function getGroup(source) {
  return source.startsWith('df/') || source === 'df' ? 2 : 1;
}

module.exports = {
  meta: {
    type: 'layout',
    docs: {
      description: 'Enforce ordered and grouped imports matching legacy TSLint ordered-imports.',
    },
    schema: [],
    messages: {
      groupOrder: "External import '{{name}}' must be declared before internal 'df/...' imports.",
      alphabetical: "Import '{{name}}' is out of alphabetical order within its group.",
    },
  },
  create(context) {
    let currentGroup = 1;
    let prevSourceClean = '';

    return {
      ImportDeclaration(node) {
        if (!node.source || typeof node.source.value !== 'string') {
          return;
        }
        const source = node.source.value;
        const group = getGroup(source);

        if (group < currentGroup) {
          context.report({
            node,
            messageId: 'groupOrder',
            data: { name: source },
          });
        } else if (group > currentGroup) {
          currentGroup = group;
          prevSourceClean = cleanPath(source);
        } else {
          const cleaned = cleanPath(source);
          if (prevSourceClean && cleaned.localeCompare(prevSourceClean) < 0) {
            context.report({
              node,
              messageId: 'alphabetical',
              data: { name: source },
            });
          }
          prevSourceClean = cleaned;
        }
      },
    };
  },
};
