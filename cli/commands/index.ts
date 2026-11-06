import { compileCommand } from "df/cli/commands/compile_command";
import { formatCommand } from "df/cli/commands/format_command";
import { initCommand } from "df/cli/commands/init_command";
import { initCredsCommand } from "df/cli/commands/init_creds_command";
import { installCommand } from "df/cli/commands/install_command";
import { runCommand } from "df/cli/commands/run_command";
import { testCommand } from "df/cli/commands/test_command";
import { ICommandBase } from "df/cli/yargswrapper";

export { compileCommand } from "df/cli/commands/compile_command";
export { formatCommand } from "df/cli/commands/format_command";
export { createHelpCommand } from "df/cli/commands/help_command";
export { initCommand } from "df/cli/commands/init_command";
export { initCredsCommand } from "df/cli/commands/init_creds_command";
export { installCommand } from "df/cli/commands/install_command";
export { runCommand } from "df/cli/commands/run_command";
export { testCommand } from "df/cli/commands/test_command";

// The CLI's commands, in the order they are listed in `dataform help` and cli/README.md.
// The `help` command is added separately by runCli().
export const CLI_COMMANDS: ICommandBase[] = [
  initCommand,
  installCommand,
  initCredsCommand,
  compileCommand,
  testCommand,
  runCommand,
  formatCommand,
];
