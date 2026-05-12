// tslint:disable tsr-detect-non-literal-fs-filename
import * as path from "path";

export const DEFAULT_DATABASE = "dataform-open-source";
export const DEFAULT_LOCATION = "US";
export const DEFAULT_RESERVATION = "projects/dataform-open-source/locations/us/reservations/dataform-test";
export const CREDENTIALS_PATH = path.resolve(process.env.RUNFILES, "df/test_credentials/bigquery.json");

export const cliEntryPointPath = "cli/node_modules/@dataform/cli/bundle.js";
