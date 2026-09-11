import * as fs from "fs";
import * as glob from "glob";
import * as path from "path";
import yargs from "yargs";

import {
  actionsOption,
  assertProjectDirExists,
  IActionsArgs,
  IProjectDirArgs,
  projectDirOption
} from "df/cli/common_options";
import { printError, printFormatFilesResult, printSuccess } from "df/cli/console";
import { ICommand, INamedOption } from "df/cli/yargswrapper";
import { formatFile } from "df/sqlx/format";

export interface IFormatArgs extends IProjectDirArgs, IActionsArgs {
  ignoreJsFiles: boolean;
  check: boolean;
}

const fmtIgnoreJsOption: INamedOption<yargs.Options, IFormatArgs> = {
  name: "ignore-js-files",
  option: {
    describe: "If set, the formatter will not consider javascript files (.js)",
    type: "boolean",
    default: false
  }
};

const checkOption: INamedOption<yargs.Options, IFormatArgs> = {
  name: "check",
  option: {
    describe: "Check if files are formatted correctly without modifying them.",
    type: "boolean",
    default: false
  }
};

export const formatCommand: ICommand<IFormatArgs> = {
  format: `format [${projectDirOption.name}]`,
  description: "Format the dataform project's files.",
  positionalOptions: [projectDirOption],
  options: [actionsOption, fmtIgnoreJsOption, checkOption],
  check: [assertProjectDirExists],
  processFn: async argv => {
    const extensions = argv.ignoreJsFiles ? "*.sqlx" : "*.{js,sqlx}";
    let actions = [`{definitions,includes}/**/${extensions}`];
    if (argv.actions && argv.actions.length > 0) {
      actions = argv.actions;
    }
    const filenames = actions
      .map((action: string) => glob.sync(action, { cwd: argv.projectDir }))
      .flat();

    const isCheckMode = argv.check;
    const results: Array<{
      filename: string;
      err?: Error;
      needsFormatting?: boolean;
    }> = await Promise.all(
      filenames.map(async (filename: string) => {
        try {
          const filePath = path.resolve(argv.projectDir, filename);
          if (isCheckMode) {
            // In check mode, we don't modify files, just check if they need formatting
            const fileContent = fs.readFileSync(filePath).toString();
            const formattedContent = await formatFile(filePath, {
              overwriteFile: false
            });
            return {
              filename,
              needsFormatting: fileContent !== formattedContent
            };
          } else {
            // Normal formatting mode
            await formatFile(filePath, {
              overwriteFile: true
            });
            return {
              filename
            };
          }
        } catch (e) {
          return {
            filename,
            err: e
          };
        }
      })
    );

    printFormatFilesResult(results);

    // Return error code if there are any formatting errors
    const failedFormatResults = results.filter(result => !!result.err);
    if (failedFormatResults.length > 0) {
      printError(`${failedFormatResults.length} file(s) failed to format.`);
      return 1;
    }

    // In check mode, return an error code if any files need formatting
    if (isCheckMode) {
      const filesNeedingFormatting = results.filter(result => result.needsFormatting);
      if (filesNeedingFormatting.length > 0) {
        printError(
          `${filesNeedingFormatting.length} file(s) would be reformatted. Run the format command without --check to update.`
        );
        return 1;
      }
      printSuccess("All files are formatted correctly!");
    }

    return 0;
  }
};
