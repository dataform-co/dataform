import * as fs from "fs";
import yargs from "yargs";

const argv = yargs
  .option("package-version", { required: true, type: "string" })
  .option("name", { required: true })
  .option("description", { required: true })
  .option("main", { required: false, type: "string" })
  .option("types", { required: false, type: "string" })
  .option("output-path", { required: true })
  .option("layer-paths", { required: true, array: true })
  .option("external-dependencies", {
    array: true
  }).argv;

const outputPath = argv.outputPath as string;
const layerPaths = argv.layerPaths as string[];
const externalDependencies = argv.externalDependencies as string[];

// Merge layers in the given order.
const result = layerPaths
  // tslint:disable-next-line: tsr-detect-non-literal-fs-filename
  .map((layerPath: string) => JSON.parse(fs.readFileSync(layerPath, "utf8")))
  .reduce(
    (accumulatorJson: object, layerJson: object) => ({ ...accumulatorJson, ...layerJson }),
    {}
  );

// Add overrides.
result.version = argv.packageVersion;
result.name = argv.name;
result.description = argv.description;
if (argv.main) {
  result.main = argv.main;
}
if (argv.types) {
  result.types = argv.types;
}

// Filter out dependencies.
result.dependencies = externalDependencies.reduce((acc, key) => {
  if (!result.dependencies[key]) {
    throw new Error("Dependency does not appear to be installed in root package.json: " + key);
  }
  return { ...acc, [key]: result.dependencies[key] };
}, {});

const resultString = JSON.stringify(result, null, 4);

// tslint:disable-next-line: tsr-detect-non-literal-fs-filename
fs.writeFileSync(outputPath, resultString);
