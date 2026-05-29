const cp = require("child_process");
const fs = require("fs");
const path = require("path");

const argv = require("minimist")(process.argv.slice(2));

const bazelBinDir = process.env.BAZEL_BINDIR;

function resolvePath(p) {
  if (p && bazelBinDir && p.startsWith(bazelBinDir)) {
    let relativePath = p.substring(bazelBinDir.length);
    if (relativePath.startsWith("/")) {
      relativePath = relativePath.substring(1);
    }
    return relativePath;
  }
  return p;
}

const jsOut = resolvePath(argv["js-out"]);
const esmJsOut = resolvePath(argv["esm-js-out"]);
const dtsOut = resolvePath(argv["dts-out"]);
const protoFiles = argv._.map(resolvePath);

if (!jsOut || !esmJsOut || !dtsOut || protoFiles.length === 0) {
    console.error("Usage: node compile_protos.js --js-out <path> --esm-js-out <path> --dts-out <path> <proto_files...>");
    process.exit(1);
}

const pbjsPath = require.resolve("protobufjs-cli/pbjs");
const pbtsPath = require.resolve("protobufjs-cli/pbts");

// 1. Run pbjs for CommonJS static-module programmatically in an isolated process
const pbjsJsScript = `
const pbjs = require("${pbjsPath.replace(/\\/g, '/')}");
const fs = require("fs");
pbjs.main(["--target", "static-module", "--wrap", "default", "--strict-long", ${protoFiles.map(f => `"${f}"`).join(", ")}], function(err, output) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    fs.writeFileSync("${jsOut}", output);
    process.exit(0);
});
`;

const pbjsJsRun = cp.spawnSync("node", ["-e", pbjsJsScript], { encoding: "utf-8" });
if (pbjsJsRun.status !== 0) {
    console.error("pbjs CommonJS failed:", pbjsJsRun.stderr || pbjsJsRun.stdout);
    process.exit(pbjsJsRun.status || 1);
}

// 2. Run pbjs for ES6 static-module programmatically in an isolated process
const pbjsEsmScript = `
const pbjs = require("${pbjsPath.replace(/\\/g, '/')}");
const fs = require("fs");
pbjs.main(["--target", "static-module", "--wrap", "es6", "--strict-long", ${protoFiles.map(f => `"${f}"`).join(", ")}], function(err, output) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    fs.writeFileSync("${esmJsOut}", output);
    process.exit(0);
});
`;

const pbjsEsmRun = cp.spawnSync("node", ["-e", pbjsEsmScript], { encoding: "utf-8" });
if (pbjsEsmRun.status !== 0) {
    console.error("pbjs ES6 failed:", pbjsEsmRun.stderr || pbjsEsmRun.stdout);
    process.exit(pbjsEsmRun.status || 1);
}

// 3. Run pbts programmatically in an isolated process
const pbtsScript = `
const pbts = require("${pbtsPath.replace(/\\/g, '/')}");
const fs = require("fs");
pbts.main(["${jsOut}"], function(err, dtsOutput) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    fs.writeFileSync("${dtsOut}", dtsOutput);
    process.exit(0);
});
`;

const pbtsRun = cp.spawnSync("node", ["-e", pbtsScript], { encoding: "utf-8" });
if (pbtsRun.status !== 0) {
    console.error("pbts failed:", pbtsRun.stderr || pbtsRun.stdout);
    process.exit(pbtsRun.status || 1);
}

console.log("Successfully generated proto JS, ESM, and typings via isolated child processes!");
