import * as fs from "fs";
import * as path from "path";
import yargs from "yargs";

import { credentials } from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { prettyJsonStringify } from "df/cli/api/utils";
import { projectDirMustExistOption } from "df/cli/common_options";
import { print, printInitCredsResult, printSuccess } from "df/cli/console";
import { getBigQueryCredentials } from "df/cli/credentials";
import { ICommand, INamedOption } from "df/cli/yargswrapper";

const testConnectionOptionName = "test-connection";

const testConnectionOption: INamedOption<yargs.Options> = {
  name: testConnectionOptionName,
  option: {
    describe: "If true, a test query will be run using your final credentials.",
    type: "boolean",
    default: true
  }
};

export const initCredsCommand: ICommand = {
  format: `init-creds [${projectDirMustExistOption.name}]`,
  description:
    `Create a ${credentials.CREDENTIALS_FILENAME} file for Dataform to use when ` +
    `accessing BigQuery.`,
  positionalOptions: [projectDirMustExistOption],
  options: [testConnectionOption],
  processFn: async argv => {
    const finalCredentials = getBigQueryCredentials();
    if (argv[testConnectionOptionName]) {
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
      argv[projectDirMustExistOption.name],
      credentials.CREDENTIALS_FILENAME
    );
    fs.writeFileSync(filePath, prettyJsonStringify(finalCredentials));
    printInitCredsResult(filePath);
    return 0;
  }
};
