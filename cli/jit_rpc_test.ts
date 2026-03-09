import { expect } from "chai";
import Long from "long";
import { anything, capture, instance, mock, verify, when } from "ts-mockito";

import { handleRpc } from "df/cli/api/commands/jit_rpc";
import { IDbAdapter, IDbClient } from "df/cli/api/dbadapters";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("jit_rpc", () => {
  test("Execute RPC maps to client.execute with all options", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT * FROM table";
    const executeRequest = dataform.ExecuteRequest.create({
      statement,
      rowLimit: Long.fromNumber(100),
      byteLimit: Long.fromNumber(1024),
      bigQueryOptions: {
        interactive: true,
        location: "US",
        labels: { key: "val" }
      }
    });
    const encodedRequest = dataform.ExecuteRequest.encode(executeRequest).finish();

    when(mockClient.execute(statement, anything())).thenResolve({
      rows: [{ num: 42, str: "val", bool: true, n: null }],
      metadata: { bigquery: { jobId: "job1" } }
    });

    const response = await handleRpc(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    const fields = decoded.rows[0].fields;
    expect(fields.num.numberValue).equals(42);
    expect(fields.str.stringValue).equals("val");
    expect(fields.bool.boolValue).equals(true);
    expect(fields.n.nullValue).equals(0);

    verify(mockClient.execute(statement, anything())).once();
  });

  test("DeleteTable RPC calls adapter and returns empty buffer", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const target = { database: "db", schema: "sch", name: "tab" };
    const request = dataform.DeleteTableRequest.create({ target });
    const encodedRequest = dataform.DeleteTableRequest.encode(request).finish();

    const response = await handleRpc(instance(mockAdapter), instance(mockClient), "DeleteTable", encodedRequest);

    verify(mockAdapter.deleteTable(anything())).once();
    const capturedTarget = capture(mockAdapter.deleteTable).last()[0];
    expect(capturedTarget.name).equals("tab");
    expect(response.length).equals(0);
  });

  test("Execute RPC handles null and empty result sets", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT null as n";
    const encodedRequest = dataform.ExecuteRequest.encode(dataform.ExecuteRequest.create({ statement })).finish();

    // Test with a null value
    when(mockClient.execute(statement, anything())).thenResolve({
      rows: [{ n: null }],
      metadata: {}
    });

    const response = await handleRpc(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    expect(decoded.rows[0].fields.n.nullValue).equals(0); // Protobuf NullValue.NULL_VALUE is 0

    // Test with empty rows
    when(mockClient.execute(statement, anything())).thenResolve({
      rows: [],
      metadata: {}
    });

    const responseEmpty = await handleRpc(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decodedEmpty = dataform.ExecuteResponse.decode(responseEmpty);
    expect(decodedEmpty.rows.length).equals(0);
  });

  test("ListTables RPC returns tables from adapter", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const request = dataform.ListTablesRequest.create({ database: "db", schema: "sch" });
    const encodedRequest = dataform.ListTablesRequest.encode(request).finish();

    const target1 = { database: "db", schema: "sch", name: "table1" };
    when(mockAdapter.tables()).thenResolve([target1]);
    when(mockAdapter.table(anything())).thenResolve({ target: target1 } as any);

    const response = await handleRpc(instance(mockAdapter), instance(mockClient), "ListTables", encodedRequest);
    const decoded = dataform.ListTablesResponse.decode(response);

    expect(decoded.tables.length).equals(1);
    expect(decoded.tables[0].target.name).equals("table1");
  });

  test("GetTable RPC returns metadata from adapter", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const target = { database: "db", schema: "sch", name: "tab" };
    const request = dataform.GetTableRequest.create({ target });
    const encodedRequest = dataform.GetTableRequest.encode(request).finish();

    when(mockAdapter.table(anything())).thenResolve({ target } as any);

    const response = await handleRpc(instance(mockAdapter), instance(mockClient), "GetTable", encodedRequest);
    const decoded = dataform.TableMetadata.decode(response);

    expect(decoded.target.name).equals("tab");
  });

  test("GetTable RPC returns empty metadata when table not found", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const target = { database: "db", schema: "sch", name: "missing" };
    const request = dataform.GetTableRequest.create({ target });
    const encodedRequest = dataform.GetTableRequest.encode(request).finish();

    // Adapter returns null for missing table
    when(mockAdapter.table(anything())).thenResolve(null);

    const response = await handleRpc(instance(mockAdapter), instance(mockClient), "GetTable", encodedRequest);
    const decoded = dataform.TableMetadata.decode(response);

    // Proto instances for empty messages have null target in protobufjs decoded objects
    expect(decoded.target).to.equal(null);
  });

  test("ListTables RPC filters by schema", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const request = dataform.ListTablesRequest.create({ database: "db", schema: "sch1" });
    const encodedRequest = dataform.ListTablesRequest.encode(request).finish();

    const target1 = { database: "db", schema: "sch1", name: "table1" };
    const target2 = { database: "db", schema: "sch2", name: "table2" };

    when(mockAdapter.tables()).thenResolve([target1, target2]);
    when(mockAdapter.table(target1)).thenResolve({ target: target1 } as any);
    when(mockAdapter.table(target2)).thenResolve({ target: target2 } as any);

    const response = await handleRpc(instance(mockAdapter), instance(mockClient), "ListTables", encodedRequest);
    const decoded = dataform.ListTablesResponse.decode(response);

    // Should only contain target1
    expect(decoded.tables.length).equals(1);
    expect(decoded.tables[0].target.schema).equals("sch1");
  });

  test("DeleteTable RPC respects global dryRun flag", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const request = dataform.DeleteTableRequest.create({
      target: { database: "db", schema: "sch", name: "tab" }
    });
    const encodedRequest = dataform.DeleteTableRequest.encode(request).finish();

    // Call with dryRun = true
    await handleRpc(instance(mockAdapter), instance(mockClient), "DeleteTable", encodedRequest, true);

    // Verify that the adapter method was NOT called
    verify(mockAdapter.deleteTable(anything())).never();
  });

  test("Execute RPC respects global dryRun flag", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT 1";
    const encodedRequest = dataform.ExecuteRequest.encode(dataform.ExecuteRequest.create({ statement })).finish();

    when(mockClient.execute(anything(), anything())).thenResolve({ rows: [], metadata: {} });

    // Call with dryRun = true
    await handleRpc(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest, true);

    // Verify that the execution options had dryRun: true
    const capturedOptions = capture(mockClient.execute).last()[1];
    expect(capturedOptions.bigquery.dryRun).to.equal(true);
  });

  test("Throws error for unrecognized RPC method", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    try {
      await handleRpc(instance(mockAdapter), instance(mockClient), "UnknownMethod", new Uint8Array());
      expect.fail("Should have thrown");
    } catch (e) {
      expect(e.message).to.contain("Unrecognized RPC method");
    }
  });
});
