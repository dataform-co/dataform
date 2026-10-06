import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { VmRunner } from "df/common/vm/vm_runner";

const MODULE_COUNT = 50;

/**
 * Measures two things separately:
 * - cold loads: a fresh VmRunner per round, so every require() resolves, compiles and evaluates
 *   its module (the module cache is empty);
 * - cached requires: repeated require() calls on one runner, which only hit the module cache.
 */
function runBenchmark() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "vm-runner-benchmark-"));
  try {
    fs.mkdirSync(path.join(tmpDir, "includes"));
    fs.writeFileSync(
      path.join(tmpDir, "includes", "helpers.js"),
      "module.exports = { format: (x) => 'formatted_' + x };",
    );

    for (let i = 0; i < MODULE_COUNT; i++) {
      fs.writeFileSync(
        path.join(tmpDir, `table_${i}.js`),
        `const { format } = require("./includes/helpers");
         module.exports = { name: format("table_${i}"), query: "SELECT ${i}" };`,
      );
    }

    const requireAll = (runner: VmRunner) => {
      for (let i = 0; i < MODULE_COUNT; i++) {
        runner.require(`./table_${i}`);
      }
    };

    requireAll(new VmRunner({ projectDir: tmpDir }));

    const coldRounds = 20;
    const coldStart = process.hrtime.bigint();
    for (let round = 0; round < coldRounds; round++) {
      requireAll(new VmRunner({ projectDir: tmpDir }));
    }
    const coldMs = Number(process.hrtime.bigint() - coldStart) / 1e6;
    const coldLoads = coldRounds * (MODULE_COUNT + 1);

    const cachedRunner = new VmRunner({ projectDir: tmpDir });
    requireAll(cachedRunner);
    const cachedRounds = 100;
    const cachedStart = process.hrtime.bigint();
    for (let round = 0; round < cachedRounds; round++) {
      requireAll(cachedRunner);
    }
    const cachedMs = Number(process.hrtime.bigint() - cachedStart) / 1e6;
    const cachedRequires = cachedRounds * MODULE_COUNT;

    const perSecond = (count: number, ms: number) => Math.round(count / (ms / 1000));
    // eslint-disable-next-line no-console
    console.log("VmRunner Benchmark Results:");
    // eslint-disable-next-line no-console
    console.log(
      `  Cold module loads (resolve + compile + evaluate): ${coldLoads} in ${coldMs.toFixed(2)} ms ` +
        `(${perSecond(coldLoads, coldMs)} ops/sec)`,
    );
    // eslint-disable-next-line no-console
    console.log(
      `  Cached requires: ${cachedRequires} in ${cachedMs.toFixed(2)} ms ` +
        `(${perSecond(cachedRequires, cachedMs)} ops/sec)`,
    );
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

runBenchmark();
