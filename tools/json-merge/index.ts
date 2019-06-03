import * as fs from "fs";
import * as yargs from "yargs";

const argv = yargs.option("output-path", { required: true }).option("layer-paths", { array: true })
  .argv;

const result = argv.layerPaths
  .map((layerPath: string) => JSON.parse(fs.readFileSync(layerPath, "utf8")))
  .reduce(
    (layerJson: object, accumulatorJson: object) => ({ ...accumulatorJson, ...layerJson }),
    {}
  );

fs.writeFileSync(argv.outputPath, JSON.stringify(result, null, 4));
