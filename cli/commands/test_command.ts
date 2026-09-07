import { compile, credentials, test } from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { prettyJsonStringify } from "df/cli/api/utils";
import {
  credentialsOption,
  getCredentialsPath,
  jsonOutputOption,
  projectDirMustExistOption,
  projectDirOption,
  quietCompileOption,
  timeoutOption
} from "df/cli/common_options";
import {
  print,
  printCompiledGraphErrors,
  printError,
  printSuccess,
  printTestResult
} from "df/cli/console";
import { ProjectConfigOptions } from "df/cli/project_config_options";
import { compiledGraphHasErrors } from "df/cli/util";
import { ICommand } from "df/cli/yargswrapper";

export const testCommand: ICommand = {
  format: `test [${projectDirMustExistOption.name}]`,
  description: "Run the dataform project's unit tests.",
  positionalOptions: [projectDirMustExistOption],
  options: [
    credentialsOption,
    timeoutOption,
    jsonOutputOption,
    ...ProjectConfigOptions.allYargsOptions
  ],
  processFn: async argv => {
    if (!argv[jsonOutputOption.name]) {
      print("Compiling...\n");
    }
    const compiledGraph = await compile({
      projectDir: argv[projectDirMustExistOption.name],
      projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
      timeoutMillis: argv[timeoutOption.name] || undefined
    });
    if (compiledGraphHasErrors(compiledGraph)) {
      printCompiledGraphErrors(compiledGraph.graphErrors, argv[quietCompileOption.name]);
      return 1;
    }
    if (!argv[jsonOutputOption.name]) {
      printSuccess("Compiled successfully.\n");
    }
    const readCredentials = credentials.read(
      getCredentialsPath(argv[projectDirOption.name], argv[credentialsOption.name])
    );

    if (!compiledGraph.tests.length) {
      printError("No unit tests found.");
      return 1;
    }

    if (!argv[jsonOutputOption.name]) {
      print(`Running ${compiledGraph.tests.length} unit tests...\n`);
    }
    const dbadapter = new BigQueryDbAdapter(readCredentials);
    const testResults = await test(dbadapter, compiledGraph.tests);
    if (!argv[jsonOutputOption.name]) {
      testResults.forEach(testResult => printTestResult(testResult));
    } else {
      // Print all results as JSON if the option is set.
      print(prettyJsonStringify(testResults));
    }
    return testResults.every(testResult => testResult.successful) ? 0 : 1;
  }
};
