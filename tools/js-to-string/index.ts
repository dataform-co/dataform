import fs from "fs";
import { promisify } from "util";
import yargs from "yargs";

const argv = yargs
  .option("input-path", { type: "string", required: true })
  .option("output-path", { type: "string", required: true }).argv;

async function run() {
  const inputFile = await promisify(fs.readFile)(argv["input-path"], "utf8");
  const inputFileStringified = JSON.stringify(inputFile);
  const newFile = `
const contents = "${inputFileStringified}";
export default contents;
  `;
  await promisify(fs.writeFile)(argv["output-path"], newFile);
}


// tslint:disable-next-line: no-floating-promises
run();
