import * as fs from "fs";
import * as yargs from "yargs";

const argv = yargs
  .option("output-path", { required: true })
  .option("layer-paths", { array: true })
  .option("substitutions", { type: "string" }).argv;

const outputPath = argv.outputPath as string;
const layerPaths = argv.layerPaths as string[];
const substitutions = JSON.parse(argv.substitutions || "{}") as { [key: string]: string };

// Merge layers in the given order.
const result = layerPaths
  .map((layerPath: string) => JSON.parse(fs.readFileSync(layerPath, "utf8")))
  .reduce(
    (accumulatorJson: object, layerJson: object) => ({ ...accumulatorJson, ...layerJson }),
    {}
  );

let resultString = JSON.stringify(result, null, 4);

// Apply any plain string substitutions.
if (substitutions) {
  resultString = Object.keys(substitutions).reduce(
    (original, key) => original.split(key).join(substitutions[key]),
    resultString
  );
}

fs.writeFileSync(outputPath, resultString);
