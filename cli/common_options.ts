import * as fs from "fs";
import * as path from "path";
import yargs from "yargs";

import { actuallyResolve, assertPathExists } from "df/cli/util";
import { INamedOption } from "df/cli/yargswrapper";

export const projectDirOption: INamedOption<yargs.PositionalOptions> = {
  name: "project-dir",
  option: {
    describe: "The Dataform project directory.",
    default: ".",
    coerce: actuallyResolve
  }
};

export const projectDirMustExistOption: INamedOption<yargs.PositionalOptions> = {
  ...projectDirOption,
  check: (argv: yargs.Arguments<any>) => {
    assertPathExists(argv[projectDirOption.name]);
    const dataformJsonPath = path.resolve(argv[projectDirOption.name], "dataform.json");
    const workflowSettingsYamlPath = path.resolve(
      argv[projectDirOption.name],
      "workflow_settings.yaml"
    );
    if (!fs.existsSync(dataformJsonPath) && !fs.existsSync(workflowSettingsYamlPath)) {
      throw new Error(
        `${
          argv[projectDirOption.name]
        } does not appear to be a dataform directory (missing workflow_settings.yaml file).`
      );
    }
  }
};

// Splits repeated and comma-separated values into a flat list, e.g.
// `--actions a,b --actions c` -> ["a", "b", "c"].
export const splitCommas = (raw: string[] | null) => raw.map(value => value.split(",")).flat();

export const actionsOption: INamedOption<yargs.Options> = {
  name: "actions",
  option: {
    describe: "A list of action names or patterns to run. Can include '*' wildcards.",
    type: "array",
    coerce: splitCommas
  }
};

