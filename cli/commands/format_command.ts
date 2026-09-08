import * as fs from "fs";
import * as glob from "glob";
import * as path from "path";
import yargs from "yargs";

import { actionsOption, projectDirMustExistOption } from "df/cli/common_options";
import { printError, printFormatFilesResult, printSuccess } from "df/cli/console";
import { ICommand, INamedOption } from "df/cli/yargswrapper";
import { formatFile } from "df/sqlx/format";

const fmtIgnoreJsOption: INamedOption<yargs.Options> = {
  name: "ignore-js-files",
  option: {
    describe: "If set, the formatter will not consider javascript files (.js)",
    type: "boolean",
    default: false
  }
};

const checkOptionName = "check";

const checkOption: INamedOption<yargs.Options> = {
  name: checkOptionName,
  option: {
    describe: "Check if files are formatted correctly without modifying them.",
    type: "boolean",
    default: false
  }
};

export const formatCommand: ICommand = {
  format: `format [${projectDirMustExistOption.name}]`,
  description: "Format the dataform project's files.",
  positionalOptions: [projectDirMustExistOption],
  options: [actionsOption, fmtIgnoreJsOption, checkOption],
  processFn: async argv => {
    const extensions = argv[fmtIgnoreJsOption.name] ? "*.sqlx" : "*.{js,sqlx}";
    let actions = [`{definitions,includes}/**/${extensions}`];
    if (actionsOption.name in argv && argv[actionsOption.name].length > 0) {
      actions = argv[actionsOption.name];
    }
    const filenames = actions
      .map((action: string) => glob.sync(action, { cwd: argv[projectDirMustExistOption.name] }))
      .flat();

    const isCheckMode = argv[checkOptionName];
    const results: Array<{
      filename: string;
      err?: Error;
      needsFormatting?: boolean;
    }> = await Promise.all(
      filenames.map(async (filename: string) => {
        try {
          const filePath = path.resolve(argv[projectDirMustExistOption.name], filename);
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
