import { Session } from "df/core/session";
import { nativeRequire } from "df/core/utils";
import * as Path from "df/core/path";
import { IAction } from "../types";

export function transpileSql(action: IAction, session: Session, yamlPath?: string) {
  const sqlConfig = action.sql;
  if (!sqlConfig) {
    return;
  }
  const name = sqlConfig.name;
  const queryConfig = sqlConfig.query || {};
  const engineConfig = sqlConfig.engine || {};
  const dependsOn = sqlConfig.dependsOn || [];
  const executionTimeout = sqlConfig.executionTimeout;

  let queryContent = "";
  if (queryConfig.inline) {
    queryContent = queryConfig.inline;
  } else if (queryConfig.path) {
    queryContent = loadQueryContent(queryConfig.path, yamlPath);
  }

  // Strip trailing semicolon if present to prevent validation failure in Dataform
  // We use a regular expression to ignore trailing comments and whitespace when finding the semicolon.
  const trailingWhitespaceAndCommentsRegex = /(?:\s+|(?:--|#)[^\n]*\r?\n?|\/\*[\s\S]*?\*\/)*$/;
  while (true) {
    const match = queryContent.match(trailingWhitespaceAndCommentsRegex);
    if (!match) break;
    const suffix = match[0];
    const prefix = queryContent.substring(0, queryContent.length - suffix.length);
    if (prefix.endsWith(";")) {
      queryContent = prefix.slice(0, -1) + suffix;
      continue;
    }
    if (queryContent.trim().endsWith(";")) {
      const lastSemiColonIndex = queryContent.lastIndexOf(";");
      queryContent = queryContent.substring(0, lastSemiColonIndex) + queryContent.substring(lastSemiColonIndex + 1);
      continue;
    }
    break;
  }

  // Add metadata as comments to query content to "cover" all fields
  const metadataComments: string[] = [];
  if (executionTimeout) {
    metadataComments.push(`-- Execution Timeout: ${executionTimeout}`);
  }

  const config: any = { dependencies: dependsOn, tags: action.tags || [] };

  if (engineConfig.bigquery) {
    const bq = engineConfig.bigquery;
    if (bq.location) metadataComments.push(`-- BigQuery Location: ${bq.location}`);
    if (bq.impersonationChain && bq.impersonationChain.length > 0) metadataComments.push(`-- BigQuery Impersonation Chain: ${bq.impersonationChain.join(', ')}`);

    const destinationTable = bq.destinationTable;
    if (destinationTable) {
      const parts = destinationTable.split('.');
      config.type = "table";
      if (parts.length === 3) {
        config.database = parts[0];
        config.schema = parts[1];
      } else if (parts.length === 2) {
        config.schema = parts[0];
      }
      const tableName = parts[parts.length - 1];

      const finalQuery = metadataComments.length > 0 ? metadataComments.join('\n') + '\n' + queryContent : queryContent;
      session.publish(tableName, config).query(() => finalQuery);
      return;
    }
  } else if (engineConfig.dataprocServerless) {
    const ds = engineConfig.dataprocServerless;
    metadataComments.push(`-- Engine: Dataproc Serverless`);
    if (ds.location) metadataComments.push(`-- Location: ${ds.location}`);
    if (ds.resourceProfile) metadataComments.push(`-- Resource Profile: ${JSON.stringify(ds.resourceProfile)}`);
    if (ds.impersonationChain && ds.impersonationChain.length > 0) metadataComments.push(`-- Impersonation Chain: ${ds.impersonationChain.join(', ')}`);
  } else if (engineConfig.dataprocOnGce) {
    const dg = engineConfig.dataprocOnGce;
    metadataComments.push(`-- Engine: Dataproc on GCE`);
    if (dg.existingCluster) metadataComments.push(`-- Existing Cluster: ${JSON.stringify(dg.existingCluster)}`);
    if (dg.ephemeralCluster) metadataComments.push(`-- Ephemeral Cluster: ${JSON.stringify(dg.ephemeralCluster)}`);
  }

  // Fallback to operate if not published or if it's not BQ with destination table
  const finalQuery = metadataComments.length > 0 ? metadataComments.join('\n') + '\n' + queryContent : queryContent;

  session.operate(name, { tags: action.tags || [] } as any)
    .queries(finalQuery)
    .dependencies(dependsOn);
}

function loadQueryContent(queryPath: string, yamlPath?: string): string {
  // Normalize all input paths to use forward slashes
  const normalizedQuery = queryPath.replace(/\\/g, "/");
  const normalizedYaml = yamlPath ? yamlPath.replace(/\\/g, "/") : undefined;

  // Attempt 1: Resolve relative to the YAML file's directory
  let resolvedPath = normalizedQuery;
  if (normalizedYaml) {
    const lastSlash = normalizedYaml.lastIndexOf("/");
    const yamlDir = lastSlash !== -1 ? normalizedYaml.substring(0, lastSlash) : "";
    if (yamlDir) {
      resolvedPath = `${yamlDir}/${normalizedQuery}`;
    }
  }

  resolvedPath = Path.normalize(resolvedPath);

  try {
    const result = nativeRequire(resolvedPath);
    return result.query;
  } catch (err1) {
    // Attempt 2: Lookup normalized queryPath directly
    try {
      const normalizedQueryPath = Path.normalize(normalizedQuery);
      const result = nativeRequire(normalizedQueryPath);
      return result.query;
    } catch (err2) {
      // Attempt 3: Try prefixing with definitions/
      try {
        if (!normalizedQuery.startsWith("definitions/")) {
          const definitionsPath = Path.normalize(`definitions/${normalizedQuery}`);
          const result = nativeRequire(definitionsPath);
          return result.query;
        }
      } catch (err3) {
        // Ignore
      }
    }
  }

  throw new Error(
    `Failed to load query from path: "${queryPath}". File not found or failed to compile.\n` +
    `Attempted paths:\n` +
    `  - ${resolvedPath}\n` +
    `  - ${Path.normalize(normalizedQuery)}\n` +
    `  - definitions/${Path.normalize(normalizedQuery)}`
  );
}
