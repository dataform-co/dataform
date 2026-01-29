// Replace Registry in yarn.lock with the specified one.
// This script is executed before any node packages are fetched,
// keep depencency free.

const fs = require('fs');

const args = process.argv.slice(2);
const lockfilePath = args[0];
let newRegistry = args[1];

if (!newRegistry) {
  console.error('Usage: node switch_registry.js /path/to/yarn.lock https://new.registry.com');
  process.exit(1);
}

if (!newRegistry.match('https://.*')) {
  console.error(`Not a valid registry URL: ${newRegistry}`);
  process.exit(1);
}

if (newRegistry.endsWith('/')) {
    newRegistry = newRegistry.slice(0, -1);
}

try {
  const content = fs.readFileSync(lockfilePath, 'utf8');
  const registryRegex = /^(\s+resolved ")(https?:\/\/[^\/]+)(.*)"$/gm;

  const updatedContent = content.replace(registryRegex, (match, prefix, oldOrigin, path) => {
    return `${prefix}${newRegistry}${path}"`;
  });

  fs.writeFileSync(lockfilePath, updatedContent, 'utf8');

  console.log(`yarn lockfile ${lockfilePath} updated successfully to: ${newRegistry}`);
} catch (error) {
  console.error('Error processing yarn.lock:', error.message);
}