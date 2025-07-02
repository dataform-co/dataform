import * as fs from "fs";
import * as path from "path";

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

