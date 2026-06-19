'use strict';

if (require.main === module) {
  var path = require('path');
  var entryPointPath = path.resolve(__dirname, 'jit_worker.js');
  var mainScript = process.argv[1] = entryPointPath;
  try {
    module.constructor._load(mainScript, this, /*isMain=*/true);
  } catch (e) {
    console.error(e.stack || e);
    process.exit(1);
  }
}
