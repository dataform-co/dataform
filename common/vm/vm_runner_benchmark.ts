import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { VmRunner } from "df/common/vm/vm_runner";

function runBenchmark() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "vm-runner-benchmark-"));
  try {
    fs.mkdirSync(path.join(tmpDir, "includes"));
    fs.writeFileSync(
      path.join(tmpDir, "includes", "helpers.js"),
      "module.exports = { format: (x) => 'formatted_' + x };",
    );

    for (let i = 0; i < 50; i++) {
      fs.writeFileSync(
        path.join(tmpDir, `table_${i}.js`),
        `const { format } = require("./includes/helpers");
         module.exports = { name: format("table_${i}"), query: "SELECT ${i}" };`,
      );
    }

    const runner = new VmRunner({ projectDir: tmpDir });

    const iterations = 500;
    const start = process.hrtime.bigint();

    for (let iter = 0; iter < iterations; iter++) {
      const idx = iter % 50;
      runner.run(`require("./table_${idx}");`);
    }

    const end = process.hrtime.bigint();
    const durationMs = Number(end - start) / 1e6;
    const opsPerSec = Math.round(iterations / (durationMs / 1000));

    // eslint-disable-next-line no-console
    console.log("VmRunner Benchmark Results:");
    // eslint-disable-next-line no-console
    console.log(
      `  Evaluated ${iterations} module requires in ${durationMs.toFixed(2)} ms (${opsPerSec} ops/sec)`,
    );
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

runBenchmark();
