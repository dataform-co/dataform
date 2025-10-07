import * as fs from "fs";
import * as path from "path";

import {
  interactiveQuestion,
  print,
  printError,
  printSuccess,
} from "df/cli/console";
import {validateConnectionFormat} from "df/core/utils"
import { dataform } from "df/protos/ts";
import untildify from "untildify";

export function actuallyResolve(...filePaths: string[]) {
  return path.resolve(...filePaths.map(filePath => untildify(filePath)));
}

export function assertPathExists(checkPath: string) {
  if (!fs.existsSync(checkPath)) {
    throw new Error(`${checkPath} does not exist!`);
  }
}

export function compiledGraphHasErrors(graph: dataform.ICompiledGraph) {
  return graph.graphErrors?.compilationErrors?.length > 0;
}

export function formatExecutionSuffix(jobIds: string[], bytesBilled: string[]): string {
  const jobMetadataParts: string[] = [];
  if (jobIds.length > 0) {
    jobMetadataParts.push(`\n \t jobId: ${jobIds.join(", ")}`);
  }
  if (bytesBilled.length > 0) {
    jobMetadataParts.push(`\n \t Bytes billed: ${bytesBilled.join(", ")}`);
  }
  return jobMetadataParts.length > 0 ? ` ${jobMetadataParts}` : "";
}

export function formatBytesInHumanReadableFormat(bytes: number): string {
  // we do not want to raise an error when bytes < 0
  // because it will fail Dataform run command when in fact the BQ job was executed.
  if (bytes <= 0) {return '0 B';}

  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  const k = 1024;

  // Find the appropriate unit level
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  const value = (bytes / Math.pow(k, i)).toFixed(2);

  return `${value} ${units[i]}`;
}

/**
 * Handles prompting and validation for defaultBucketName, defaultTableFolderRoot
 * and defaultTableFolderSubpath if the user provides the --iceberg flag when
 * running init.
 *
 * @returns Constructed DefaultIcebergConfig object, or undefined if no inputs
 * were provided.
 */
export function promptForIcebergConfig(): dataform.IDefaultIcebergConfig | undefined {
  print(ICEBERG_CONFIG_PROMPT_TEXT);
  print(ICEBERG_CONFIG_PROMPT_HINT);

  const tempIcebergConfig: dataform.IDefaultIcebergConfig = {};

  let bucketName: string;
  while (true) {
    print(ICEBERG_BUCKET_NAME_HINT);
    bucketName = interactiveQuestion(ICEBERG_BUCKET_NAME_PROMPT_QUESTION);
    try {
      if(bucketName) {
        validateIcebergConfigBucketName(bucketName);
        tempIcebergConfig.bucketName = bucketName;
      }
      break;
    } catch (e) {
      printError(`Validation Error: ${e.message}`);
      print("Please try again.");
    }
  }

  let tableFolderRoot: string;
  while (true) {
    print(ICEBERG_TABLE_FOLDER_ROOT_HINT);
    tableFolderRoot = interactiveQuestion(ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION);
    try {
      if (tableFolderRoot) {
        validateIcebergConfigTableFolderRoot(tableFolderRoot);
        tempIcebergConfig.tableFolderRoot = tableFolderRoot;
      }
      break;
    } catch (e) {
      printError(`Validation Error: ${e.message}`);
      print("Please try again.");
    }
  }

  let tableFolderSubpath: string;
  while (true) {
    print(ICEBERG_TABLE_FOLDER_ROOT_SUBPATH_HINT)
    tableFolderSubpath = interactiveQuestion(ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION);
    try {
      if (tableFolderSubpath) {
        validateIcebergConfigTableFolderSubpath(tableFolderSubpath);
        tempIcebergConfig.tableFolderSubpath = tableFolderSubpath;
      }
      break;
    } catch (e) {
      printError(`Validation Error: ${e.message}`);
      print("Please try again.");
    }
  }

  let connection: string;
  while (true) {
    print(ICEBERG_CONNECTION_HINT)
    connection = interactiveQuestion(ICEBERG_CONNECTION_QUESTION);
    try {
      if (connection) {
        validateConnectionFormat(connection);
        tempIcebergConfig.connection = connection;
      }
      break;
    } catch (e) {
      printError(`Validation Error: ${e.message}`);
      print("Please try again.");
    }
  }

  printSuccess(ICEBERG_CONFIG_COLLECTED_TEXT);

  // Only return the config object if at least one field was set.
  if (Object.keys(tempIcebergConfig).length > 0) {
    return tempIcebergConfig;
  }
  return undefined;
}

/**
 * Validates a string to ensure it is a valid name for a GCS bucket. Based on
 * https://cloud.google.com/storage/docs/buckets#naming.
 *
 * @param bucketName The name of the bucket to validate.
 * @throws {Error} If the bucket name does not meet the following criteria:
 *     -   Must be between 3 and 63 characters long.
 *     -   Must start and end with a letter or number.
 *     -   Can only contain lowercase letters, numbers, hyphens (-), underscores (_), and periods (.).
 *     -   Cannot contain the sequence "..".
 *     -   Cannot start with "goog".
 *     -   Cannot contain "--".
 *     -   Cannot contain "google" or close misspellings such as "g00gle".
 */
export function validateIcebergConfigBucketName(bucketName: string): void {
  if (bucketName.length < 3 || bucketName.length > 63) {
    throw new Error("Bucket name must be between 3 and 63 characters long.");
  }
  if (!/^[a-z0-9][a-z0-9-._]{1,220}[a-z0-9]$/.test(bucketName) || bucketName.includes("..")) {
    throw new Error(
      "Invalid bucket name. Must start and end with a letter or number, contain only lowercase letters, numbers, hyphens (-), underscores (_), and periods (.)."
    );
  }
  if (bucketName.startsWith("goog") || bucketName.includes("--")) {
    throw new Error("Bucket name cannot start with 'goog' or contain '--'.");
  }
  if(bucketName.includes("google") || bucketName.includes("g00gle")) {
    throw new Error("Bucket name cannot contain 'google' or close misspellings such as 'g00gle'.");
  }
}

/**
 * Validates a string to ensure it is a valid table folder root for an Iceberg
 * table.
 *
 * @param tableFolderRoot The root folder path to validate.
 * @throws {Error} If the root folder does not meet the following criteria:
 *     -   Must start and end with a letter or number.
 *     -   Can only contain letters, numbers, hyphens (-), underscores (_), and periods (.).
 *     -   Cannot contain the sequence "..".
 */
export function validateIcebergConfigTableFolderRoot(tableFolderRoot: string): void {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9-._]{1,220}[a-zA-Z0-9]$/.test(tableFolderRoot) || tableFolderRoot.includes("..")) {
    throw new Error(
      "Invalid input. Must start and end with a letter or number, contain only letters (a-z, A-Z), numbers, hyphens (-), underscores (_), and periods (.)."
    );
  }
}

/**
 * Validates a string to ensure it is a valid subpath within an Iceberg table folder.
 *
 * @param tableFolderSubpath The subpath to validate.
 * @throws {Error} If the subpath does not meet the following criteria:
 *     -   Must start and end with a letter or number.
 *     -   Can only contain letters, numbers, hyphens (-), underscores (_), periods (.), and forward slashes (/).
 *     -   Cannot contain the sequence "..".
 *     -   Cannot contain "./".
 *     -   Cannot contain "../".
 */
export function validateIcebergConfigTableFolderSubpath(tableFolderSubpath: string): void {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9-._/]{1,220}[a-zA-Z0-9]$/.test(tableFolderSubpath) || tableFolderSubpath.includes("..")) {
    throw new Error(
      "Invalid input. Must start and end with a letter or number, contain only letters (a-z, A-Z), numbers, hyphens (-), underscores (_), periods (.), and forward slashes (/). The sequence '..' is also disallowed."
    );
  }
  if (tableFolderSubpath.includes("./") || tableFolderSubpath.includes("../")) {
    throw new Error("Input cannot contain './' or '../'.");
  }
}

export const ICEBERG_BUCKET_NAME_HINT = "The bucket name must comply with https://cloud.google.com/storage/docs/buckets#naming. If you do not want to provide a workflow-level default bucket name, leave this input empty.\n";
export const ICEBERG_TABLE_FOLDER_ROOT_HINT = "Table folder root must start and end with a letter or a number. It can only contain letters, numbers, hyphens, underscores and periods. If you do not want to provide a workflow_level default table folder root, leave this input empty.\n"
export const ICEBERG_TABLE_FOLDER_ROOT_SUBPATH_HINT = "Table folder subpath must start and end with a letter or a number. It can only contain letters, numbers, hyphens, underscores, periods and forward slashes. If you do not want to provide a workflow_level default table folder subpath, leave this input empty.\n"
export const ICEBERG_CONFIG_PROMPT_HINT = "Set repository-level configuration for Iceberg bucket name, table folder root and table folder subpath. If you do not want to set a field, enter an empty string in response to the prompt.\n";
export const ICEBERG_CONFIG_PROMPT_TEXT = "\n--- Iceberg Configuration ---\n"
export const ICEBERG_CONFIG_COLLECTED_TEXT = "Iceberg configuration collected.\n";
export const ICEBERG_BUCKET_NAME_PROMPT_QUESTION = "Enter the default Iceberg bucket name:";
export const ICEBERG_TABLE_FOLDER_ROOT_PROMPT_QUESTION = "Enter the default Iceberg table folder root:";
export const ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_QUESTION = "Enter the default Iceberg table folder subpath:";
export const ICEBERG_CONNECTION_HINT = "The connection can have the form `{project}.{location}.{connection_id}` or `projects/{project}/locations/{location}/connections/{connection_id} or be set to DEFAULT. If you do not want to provide a workflow-level default connection, leave this input empty.\n";
export const ICEBERG_CONNECTION_QUESTION = "Enter the default connection:";
