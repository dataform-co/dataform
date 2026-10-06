const fs = require("fs");
const path = require("path");
const { promisify } = require("util");

const argv = require("minimist")(process.argv.slice(2));
const pbjs = require("protobufjs-cli/pbjs");
const pbts = require("protobufjs-cli/pbts");

const pbjsMain = promisify(pbjs.main);
const pbtsMain = promisify(pbts.main);

const bazelBinDir = process.env.BAZEL_BINDIR;

function resolvePath(p) {
  if (!p || !bazelBinDir || path.isAbsolute(p)) return p;
  return path.relative(bazelBinDir, p);
}

const jsOut = resolvePath(argv["js-out"]);
const esmJsOut = resolvePath(argv["esm-js-out"]);
const dtsOut = resolvePath(argv["dts-out"]);
const protoFiles = argv._.map(resolvePath);

if (!jsOut || !esmJsOut || !dtsOut || protoFiles.length === 0) {
  console.error(
    "Usage: node compile_protos.js --js-out <path> --esm-js-out <path> --dts-out <path> <proto_files...>",
  );
  process.exit(1);
}

async function main() {
  const jsOutput = await pbjsMain([
    "--target",
    "static-module",
    "--wrap",
    "default",
    "--strict-long",
    ...protoFiles,
  ]);
  fs.writeFileSync(jsOut, jsOutput);

  const esmOutput = await pbjsMain([
    "--target",
    "static-module",
    "--wrap",
    "es6",
    "--strict-long",
    ...protoFiles,
  ]);
  fs.writeFileSync(esmJsOut, esmOutput);

  // Patch 'import Long = require("long");' to 'import Long from "long";'
  // to avoid syntax errors in older rollup-plugin-dts.
  const dtsOutput = await pbtsMain([jsOut]);
  const patchedDts = dtsOutput.replace(
    /import Long = require\("long"\);/g,
    'import Long from "long";',
  );
  fs.writeFileSync(dtsOut, patchedDts);
}

main().catch((err) => {
  console.error("Failed to compile protos:", err);
  process.exit(1);
});
