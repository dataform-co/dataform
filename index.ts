import * as fs from "fs";
import * as util from "util";
import * as yargs from "yargs";
import * as path from "path";
import { NodeVM } from "vm2";
import * as dft from "./dft";

const argv = yargs.option("project-dir", { describe: "Project directory", default: "." }).argv;

var projectDir = argv["project-dir"];

const readdir = util.promisify(fs.readdir);

const vm = new NodeVM({
  timeout: 1000,
  wrapper: "none",
  require: {
    context: "sandbox",
    root: "./",
    external: true,
    mock: {
      dft: dft
    }
  }
});

var walk = function(dir): string[] {
  var results = [];
  var list = fs.readdirSync(dir);
  list.forEach(function(file) {
    file = dir + "/" + file;
    var stat = fs.statSync(file);
    if (stat && stat.isDirectory()) {
      /* Recurse into a subdirectory */
      results = results.concat(walk(file));
    } else {
      /* Is a file */
      results.push(file);
    }
  });
  return results;
};

var allFiles = walk(projectDir).map(file => file.substr(projectDir.length + 1, file.length));
var allImports = allFiles.map(file => {
  return `require("./${file}");`;
}).join("\n");

var mainScript = `const dft = require("dft");\ndft.GLOBAL = new dft.Global();\n${allImports}\nreturn dft.GLOBAL;`;
console.log(mainScript);

var global = vm.run(mainScript, path.resolve(path.join(projectDir, "main.js")));

console.log(JSON.stringify(global));
