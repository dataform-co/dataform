import { IDbAdapter, IDbClient } from "df/cli/api/dbadapters";
import { dataform, google } from "df/protos/ts";

export async function handleRpc(
  dbadapter: IDbAdapter,
  dbclient: IDbClient,
  method: string,
  request: Uint8Array,
  dryRun?: boolean
): Promise<Uint8Array> {
  switch (method) {
    case "Execute":
      return await handleExecute(dbclient, request, dryRun);
    case "ListTables":
      return await handleListTables(dbadapter, request);
    case "GetTable":
      return await handleGetTable(dbadapter, request);
    case "DeleteTable":
      return await handleDeleteTable(dbadapter, request, dryRun);
    default:
      throw new Error(`Unrecognized RPC method: ${method}`);
  }
}

async function handleExecute(
  dbclient: IDbClient,
  request: Uint8Array,
  dryRun?: boolean
): Promise<Uint8Array> {
  const executeRequest = dataform.ExecuteRequest.decode(request);
  const results = await dbclient.execute(executeRequest.statement, {
    bigquery: {
      labels: executeRequest.bigQueryOptions?.labels,
      location: executeRequest.bigQueryOptions?.location,
      jobPrefix: executeRequest.bigQueryOptions?.jobPrefix,
      dryRun: dryRun || executeRequest.bigQueryOptions?.dryRun
    }
  });

  const executeResponse = dataform.ExecuteResponse.create({
    rows: results.rows.map(mapRowToProto)
  });
  return dataform.ExecuteResponse.encode(executeResponse).finish();
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

function mapRowToProto(row: { [key: string]: any }): google.protobuf.IStruct {
  const fields: { [key: string]: google.protobuf.IValue } = {};
  for (const key of Object.keys(row)) {
    const val = row[key];
    if (typeof val === "number") {
      fields[key] = { numberValue: val };
    } else if (typeof val === "string") {
      fields[key] = { stringValue: val };
    } else if (typeof val === "boolean") {
      fields[key] = { boolValue: val };
    } else if (val === null || val === undefined) {
      fields[key] = { nullValue: 0 };
    } else {
      // For more complex types (Date, Buffer, nested Objects), stringify or structify.
      // This is a simplified mapper for IPC data transfer.
      fields[key] = { structValue: val };
    }
  }
  return { fields };
}
