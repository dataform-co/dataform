import { compile, credentials, test } from "df/cli/api";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { prettyJsonStringify } from "df/cli/api/utils";
import {
  assertProjectDirExists,
  credentialsOption,
  ICredentialsArgs,
  IJsonOutputArgs,
  IProjectDirArgs,
  ITimeoutArgs,
  jsonOutputOption,
  projectDirOption,
  timeoutOption
} from "df/cli/common_options";
import {
  print,
  printCompiledGraphErrors,
  printError,
  printSuccess,
  printTestResult
} from "df/cli/console";
import { IProjectConfigArgs, ProjectConfigOptions } from "df/cli/project_config_options";
import { actuallyResolve, compiledGraphHasErrors } from "df/cli/util";
import { ICommand } from "df/cli/yargswrapper";

export interface ITestArgs
  extends IProjectDirArgs,
    ICredentialsArgs,
    ITimeoutArgs,
    IJsonOutputArgs,
    IProjectConfigArgs {}

export const testCommand: ICommand<ITestArgs> = {
  format: `test [${projectDirOption.name}]`,
  description: "Run the dataform project's unit tests.",
  positionalOptions: [projectDirOption],
  check: [assertProjectDirExists],
  options: [
    credentialsOption,
    timeoutOption,
    jsonOutputOption,
    ...ProjectConfigOptions.allYargsOptions
  ],
  processFn: async argv => {
    if (!argv.json) {
      print("Compiling...\n");
    }
    const compiledGraph = await compile({
      projectDir: argv.projectDir,
      projectConfigOverride: ProjectConfigOptions.constructProjectConfigOverride(argv),
      timeoutMillis: argv.timeout || undefined
    });
    if (compiledGraphHasErrors(compiledGraph)) {
      printCompiledGraphErrors(compiledGraph.graphErrors);
      return 1;
    }
    if (!argv.json) {
      printSuccess("Compiled successfully.\n");
    }
    const readCredentials = credentials.read(
      actuallyResolve(argv.projectDir, argv.credentials)
    );

    if (!compiledGraph.tests.length) {
      printError("No unit tests found.");
      return 1;
    }

    if (!argv.json) {
      print(`Running ${compiledGraph.tests.length} unit tests...\n`);
    }
    const dbadapter = new BigQueryDbAdapter(readCredentials);
    const testResults = await test(dbadapter, compiledGraph.tests);
    if (!argv.json) {
      testResults.forEach(testResult => printTestResult(testResult));
    } else {
      // Print all results as JSON if the option is set.
      print(prettyJsonStringify(testResults));
    }
    return testResults.every(testResult => testResult.successful) ? 0 : 1;
  }
};
