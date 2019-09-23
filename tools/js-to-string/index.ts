import fs from "fs";
import { promisify } from "util";
import yargs from "yargs";

const argv = yargs
  .option("input-path", { type: "string", required: true })
  .option("output-path", { type: "string", required: true }).argv;

async function run() {
  if (!argv["output-path"].endsWith(".js")) {
    throw new Error("Output file path must end in .js");
  }
  const inputFile = await promisify(fs.readFile)(argv["input-path"], "utf8");
  const inputFileStringified = JSON.stringify(inputFile);
  const newFile = `
module.exports = ${inputFileStringified};
  `;
  await Promise.all([
    promisify(fs.writeFile)(argv["output-path"], newFile),
    promisify(fs.writeFile)(argv["output-path"].replace(/\.js$/, ".d.ts"), `export default string;`)
  ]);
}

// tslint:disable-next-line: no-floating-promises
run();
