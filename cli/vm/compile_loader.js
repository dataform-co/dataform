require('../../testing/resolver-patch.js');

const compileModule = require('./compile.js');
if (process.send) {
  process.send({ type: "worker_booted" });
}
compileModule.listenForCompileRequest();
