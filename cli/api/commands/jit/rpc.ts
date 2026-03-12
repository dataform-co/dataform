import { IDbAdapter, IDbClient } from "df/cli/api/dbadapters";
import { IBigQueryExecutionOptions } from "df/cli/api/dbadapters/bigquery";
import { Structs } from "df/common/protos/structs";
import { dataform, google } from "df/protos/ts";

export async function handleDbRequest(
  dbadapter: IDbAdapter,
  dbclient: IDbClient,
  method: string,
  request: Uint8Array,
  options?: IBigQueryExecutionOptions
): Promise<Uint8Array> {
  switch (method) {
    case "Execute":
      return await handleExecute(dbclient, request, options);
    case "ListTables":
      return await handleListTables(dbadapter, request);
    case "GetTable":
      return await handleGetTable(dbadapter, request);
    case "DeleteTable":
      return await handleDeleteTable(dbadapter, request, options?.dryRun);
    default:
      throw new Error(`Unrecognized RPC method: ${method}`);
  }
}

async function handleExecute(
  dbclient: IDbClient,
  request: Uint8Array,
  options?: IBigQueryExecutionOptions
): Promise<Uint8Array> {
  const executeRequest = dataform.ExecuteRequest.decode(request);
  const requestOptions = executeRequest.bigQueryOptions;

  const results = await dbclient.executeRaw(executeRequest.statement, {
    rowLimit: executeRequest.rowLimit ? executeRequest.rowLimit.toNumber() : undefined,
    bigquery: {
      labels: {
        ...(options?.labels || {}),
        ...(requestOptions?.labels || {})
      },
      location: requestOptions?.location || options?.location,
      jobPrefix: [options?.jobPrefix, requestOptions?.jobPrefix].filter(Boolean).join("-") || undefined,
      dryRun: !!(options?.dryRun || requestOptions?.dryRun)
    }
  });

  return dataform.ExecuteResponse.encode({
    rows: results.rawRows || []
  } as any).finish();
}

async function handleListTables(dbadapter: IDbAdapter, request: Uint8Array): Promise<Uint8Array> {
  const listTablesRequest = dataform.ListTablesRequest.decode(request);
  const targets = await dbadapter.tables();
  const tablesMetadata = await Promise.all(
    targets
      .filter(target => !listTablesRequest.schema || target.schema === listTablesRequest.schema)
      .map(target => dbadapter.table(target))
  );
  const listTablesResponse = dataform.ListTablesResponse.create({
    tables: tablesMetadata
  });
  return dataform.ListTablesResponse.encode(listTablesResponse).finish();
}

async function handleGetTable(dbadapter: IDbAdapter, request: Uint8Array): Promise<Uint8Array> {
  const getTableRequest = dataform.GetTableRequest.decode(request);
  const tableMetadata = await dbadapter.table(getTableRequest.target);
  if (!tableMetadata) {
    return dataform.TableMetadata.encode(dataform.TableMetadata.create({})).finish();
  }
  return dataform.TableMetadata.encode(tableMetadata).finish();
}

async function handleDeleteTable(
  dbadapter: IDbAdapter,
  request: Uint8Array,
  dryRun?: boolean
): Promise<Uint8Array> {
  const deleteTableRequest = dataform.DeleteTableRequest.decode(request);
  if (dryRun) {
    return new Uint8Array();
  }
  await dbadapter.deleteTable(deleteTableRequest.target);
  return new Uint8Array();
}
