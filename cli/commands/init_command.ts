import yargs from "yargs";

import { init } from "df/cli/api";
import { projectDirOption } from "df/cli/common_options";
import { print, printInitResult } from "df/cli/console";
import { ProjectConfigOptions } from "df/cli/project_config_options";
import { promptForIcebergConfig } from "df/cli/util";
import { ICommand, INamedOption } from "df/cli/yargswrapper";
import { dataform } from "df/protos/ts";

export const icebergOption: INamedOption<yargs.Options> = {
  name: "iceberg",
  option: {
    describe: "Initialize the project with workflow-level Iceberg tables configuration.",
    type: "boolean",
    default: false
  }
};

export const initCommand: ICommand = {
  format:
    `init [${projectDirOption.name}] [${ProjectConfigOptions.defaultDatabase.name}]` +
    ` [${ProjectConfigOptions.defaultLocation.name}]`,
  description: "Create a new dataform project.",
  positionalOptions: [
    projectDirOption,
    {
      name: ProjectConfigOptions.defaultDatabase.name,
      option: {
        describe: "The default database to use, equivalent to Google Cloud Project ID."
      },
      check: (argv: yargs.Arguments<any>) => {
        if (!argv[ProjectConfigOptions.defaultDatabase.name]) {
          throw new Error(
            `The ${ProjectConfigOptions.defaultDatabase.name} positional argument is ` +
              `required. Use "dataform help init" for more info.`
          );
        }
      }
    },
    {
      name: ProjectConfigOptions.defaultLocation.name,
      option: {
        describe:
          "The default location to use. See " +
          "https://cloud.google.com/bigquery/docs/locations for supported values."
      },
      check: (argv: yargs.Arguments<any>) => {
        if (!argv[ProjectConfigOptions.defaultLocation.name]) {
          throw new Error(
            `The ${ProjectConfigOptions.defaultLocation.name} positional argument is ` +
              `required. Use "dataform help init" for more info.`
          );
        }
      }
    }
  ],
  options: [icebergOption],
  processFn: async argv => {
    const projectDir = argv[projectDirOption.name];
    const projectConfig: dataform.IProjectConfig = {
      defaultDatabase: argv[ProjectConfigOptions.defaultDatabase.name],
      defaultLocation: argv[ProjectConfigOptions.defaultLocation.name]
    };

    if (argv[icebergOption.name]) {
      const icebergConfig = promptForIcebergConfig();
      if (icebergConfig) {
        projectConfig.defaultIcebergConfig = icebergConfig;
      }
    }

    print("Writing project files...\n");

    const initResult = await init(projectDir, projectConfig);
    printInitResult(initResult);
    return 0;
  }
};
