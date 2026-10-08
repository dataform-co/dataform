// Dev/test entry point for the compilation worker forked by BaseWorker
// (cli/api/commands/base_worker.ts). The published CLI forks worker_bundle.js instead;
// both run cli/vm/worker.ts.
"use strict";

require("../../testing/resolver-patch.js");

require("./worker");
