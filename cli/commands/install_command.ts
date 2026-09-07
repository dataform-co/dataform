import { install } from "df/cli/api";
import { projectDirMustExistOption } from "df/cli/common_options";
import { print, printSuccess } from "df/cli/console";
import { ICommand } from "df/cli/yargswrapper";

export const installCommand: ICommand = {
  format: `install [${projectDirMustExistOption.name}]`,
  description: "Install a project's NPM dependencies.",
  positionalOptions: [projectDirMustExistOption],
  options: [],
  processFn: async argv => {
    print("Installing NPM dependencies...\n");
    await install(argv[projectDirMustExistOption.name]);
    printSuccess("Project dependencies successfully installed.");
    return 0;
  }
};
