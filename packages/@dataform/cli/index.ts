import { runCli } from "df/cli";

export { compile, build, run } from "df/api";
import * as dbadapters from "df/api/dbadapters";
export { dbadapters };

if (require.main === module) {
    runCli();
}
