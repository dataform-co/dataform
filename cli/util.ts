import * as fs from "fs";
import * as path from "path";

import {
  interactiveQuestion,
  print,
  printSuccess,
} from "df/cli/console";
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

export function promptForIcebergConfig(): dataform.IDefaultIcebergConfig | undefined {
  print(ICEBERG_CONFIG_PROMPT_TEXT);

  const tempIcebergConfig: dataform.IDefaultIcebergConfig = {};

  const bucketName = interactiveQuestion(ICEBERG_BUCKET_NAME_PROMPT_TEXT);
  if (bucketName !== "") {
    tempIcebergConfig.bucketName = bucketName;
  }

  const tableFolderRoot = interactiveQuestion(ICEBERG_TABLE_FOLDER_ROOT_PROMPT_TEXT);
  if (tableFolderRoot !== "") {
    tempIcebergConfig.tableFolderRoot = tableFolderRoot;
  }

  const tableFolderSubpath = interactiveQuestion(ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_TEXT);
  if (tableFolderSubpath !== "") {
    tempIcebergConfig.tableFolderSubpath = tableFolderSubpath;
  }

  printSuccess(ICEBERG_CONFIG_COLLECTED_TEXT);

  // Only return the config object if at least one field was set.
  if (Object.keys(tempIcebergConfig).length > 0) {
    return tempIcebergConfig;
  }
  return undefined;
}

export const ICEBERG_CONFIG_PROMPT_TEXT = "\nSet repository-level configuration for Iceberg bucket name, table folder root and table folder subpath. If you do not want to set a field, enter an empty string in response to the prompt.\n";
export const ICEBERG_CONFIG_COLLECTED_TEXT = "Default Iceberg configuration collected.\n";
const ICEBERG_BUCKET_NAME_PROMPT_TEXT = "Enter the default Iceberg bucket name:";
const ICEBERG_TABLE_FOLDER_ROOT_PROMPT_TEXT = "Enter the default Iceberg table folder root:";
const ICEBERG_TABLE_FOLDER_SUBPATH_PROMPT_TEXT = "Enter the default Iceberg table folder subpath:";
