import * as fs from "fs";
import * as path from "path";
import yargs from "yargs";

import { credentials } from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { prettyJsonStringify } from "df/cli/api/utils";
import { assertProjectDirExists, IProjectDirArgs, projectDirOption } from "df/cli/common_options";
import { print, printInitCredsResult, printSuccess } from "df/cli/console";
import { getBigQueryCredentials } from "df/cli/credentials";
import { ICommand, INamedOption } from "df/cli/yargswrapper";

export interface IInitCredsArgs extends IProjectDirArgs {
  testConnection: boolean;
}

const testConnectionOption: INamedOption<yargs.Options, IInitCredsArgs> = {
  name: "test-connection",
  option: {
    describe: "If true, a test query will be run using your final credentials.",
    type: "boolean",
    default: true
  }
};

export const initCredsCommand: ICommand<IInitCredsArgs> = {
  format: `init-creds [${projectDirOption.name}]`,
  description:
    `Create a ${credentials.CREDENTIALS_FILENAME} file for Dataform to use when ` +
    `accessing BigQuery.`,
  positionalOptions: [projectDirOption],
  options: [testConnectionOption],
  check: [assertProjectDirExists],
  processFn: async argv => {
    const finalCredentials = getBigQueryCredentials();
    if (argv.testConnection) {
      print("\nRunning connection test...");
      const dbadapter = new BigQueryDbAdapter(finalCredentials);
      const testResult = await credentials.test(dbadapter);
      switch (testResult.status) {
        case credentials.TestResultStatus.SUCCESSFUL: {
          printSuccess("\nCredentials test query completed successfully.\n");
          break;
        }
        case credentials.TestResultStatus.TIMED_OUT: {
          throw new Error("Credentials test connection timed out.");
        }
        case credentials.TestResultStatus.OTHER_ERROR: {
          throw new Error(
            `Credentials test query failed: ${testResult.error.stack || testResult.error.message}`
          );
        }
      }
    } else {
      print("\nCredentials test query was not run.\n");
    }
    const filePath = path.resolve(
      argv.projectDir,
      credentials.CREDENTIALS_FILENAME
    );
    fs.writeFileSync(filePath, prettyJsonStringify(finalCredentials));
    printInitCredsResult(filePath);
    return 0;
  }
};
