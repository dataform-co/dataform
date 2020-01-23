const Mocha = require("mocha");
const fs = require("fs");
function main(args) {
  const mocha = new Mocha({
    timeout: process.env["TEST_TIMEOUT"] * 1000,
    grep: process.env["TESTBRIDGE_TEST_ONLY"]
  });
  // Set the StackTraceLimit to infinity. This will make stack capturing slower, but more useful.
  // Since we are running tests having proper stack traces is very useful and should be always set to
  // the maximum (See: https://nodejs.org/api/errors.html#errors_error_stacktracelimit)
  Error.stackTraceLimit = Infinity;
  const manifest = require.resolve(args[0]);
  // Remove the manifest, some tested code may process the argv.
  process.argv.splice(2, 1)[0];
  fs.readFileSync(manifest, { encoding: "utf-8" })
    .split("\n")
    .filter(l => l.length > 0)
    // Filter here so that only files ending in `.spec.js` and `.test.js`
    // are added to jasmine as spec files. This is important as other
    // deps such as "@npm//typescript" if executed may cause the test to
    // fail or have unexpected side-effects. "@npm//typescript" would
    // try to execute tsc, print its help, and process.exit(1)
    .filter(f => f.endsWith(".test.js") || f.endsWith(".spec.js"))
    // Filter out files from node_modules.
    .filter(f => !f.includes("/node_modules/"))
    .forEach(f => mocha.addFile(f));
  // These exit codes are handled specially by Bazel:
  // https://github.com/bazelbuild/bazel/blob/486206012a664ecb20bdb196a681efc9a9825049/src/main/java/com/google/devtools/build/lib/util/ExitCode.java#L44
  const BAZEL_EXIT_TESTS_FAILED = 3;
  const BAZEL_EXIT_NO_TESTS_FOUND = 4;
  let hasTest = false;
  const runner = mocha.run(function(failures) {
    // Overwrite the process exit code based on test results.
    let exitCode = 0;
    if (failures) {
      exitCode = BAZEL_EXIT_TESTS_FAILED;
    } else if (!hasTest) {
      exitCode = BAZEL_EXIT_NO_TESTS_FOUND;
    }
    // This is not nice, we're exiting even though there may still be stuff in Node's event loop.
    // We should really just use process.exitCode(...) and allow Node to exit the process by itself,
    // but underlying code (e.g. PostGres connection libraries) leave stuff in the event loop for
    // a *really long time*.
    process.exit(exitCode);
  });
  runner.on("test", () => {
    hasTest = true;
  });
}
if (require.main === module) {
  process.chdir(process.env["TEST_SRCDIR"]);
  main(process.argv.slice(2));
}
