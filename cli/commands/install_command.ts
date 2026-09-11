import { install } from "df/cli/api";
import { assertProjectDirExists, projectDirOption } from "df/cli/common_options";
import { print, printSuccess } from "df/cli/console";
import { ICommand } from "df/cli/yargswrapper";

export const installCommand: ICommand = {
  format: `install [${projectDirOption.name}]`,
  description: "Install a project's NPM dependencies.",
  positionalOptions: [projectDirOption],
  options: [],
  check: [assertProjectDirExists],
  processFn: async argv => {
    print("Installing NPM dependencies...\n");
    await install(argv[projectDirOption.name]);
    printSuccess("Project dependencies successfully installed.");
    return 0;
  }
};
