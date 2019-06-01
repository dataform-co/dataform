import * as fs from "fs";
import * as yargs from "yargs";

const argv = yargs.option("output", { required: true }).option("layers", { array: true }).argv;

let result = {};

argv.layers.forEach((layer: string) => {
  const inputFile = fs.readFileSync(layer, "utf8");
  const inputJson = JSON.parse(inputFile);
  result = { ...result, ...inputJson };
});

fs.writeFileSync(argv.output, JSON.stringify(result, null, 4));
