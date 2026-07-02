// tslint:disable tsr-detect-non-literal-fs-filename
import * as fs from "fs";
import * as path from "path";

export const DEFAULT_DATABASE = "dataform-open-source";
export const DEFAULT_LOCATION = "US";
export const DEFAULT_RESERVATION = "projects/dataform-open-source/locations/us/reservations/dataform-test";

const runfilesDir = process.env.RUNFILES;
let workspaceName = "df";
if (!fs.existsSync(path.resolve(runfilesDir, "df"))) {
  workspaceName = "_main";
}

export const CREDENTIALS_PATH = path.resolve(runfilesDir, workspaceName, "test_credentials/bigquery.json");

export const cliEntryPointPath = "cli/node_modules/@dataform/cli/bundle.js";
