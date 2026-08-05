'use strict';

if (require.main === module) {
  try {
    // jit_loader.js acts as the entrypoint loader for JIT development, loading jit_worker.js
    // directly. The static compiler listener (listenForCompileRequest) is omitted here since
    // static compilation in development is loaded via its own loader (compile_loader.js).
    var jitWorker = require('./jit_worker');
    jitWorker.registerRpcResponseHandler();
    jitWorker.registerJitCompileHandler();
    if (process.send) {
      process.send({ type: 'worker_booted' });
    }
  } catch (e) {
    console.error(e.stack || e);
    process.exit(1);
  }
}
