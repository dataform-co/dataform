import { expect } from "chai";
import Long from "long";
import { anything, capture, instance, mock, verify, when } from "ts-mockito";

import { handleDbRequest } from "df/cli/api/commands/jit/rpc";
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
        labels: { key: "val" },
        jobPrefix: "prefix",
        dryRun: true
      }
    });
    const encodedRequest = dataform.ExecuteRequest.encode(executeRequest).finish();

    // Real raw BigQuery f/v format
    const rawRows = [
      {
        f: [
          { v: "42" },
          { v: "val" },
          { v: "true" },
          { v: null }
        ]
      }
    ];

    const schema = [
      { name: "num", primitive: dataform.Field.Primitive.INTEGER },
      { name: "str", primitive: dataform.Field.Primitive.STRING },
      { name: "bool", primitive: dataform.Field.Primitive.BOOLEAN },
      { name: "n", primitive: dataform.Field.Primitive.STRING }
    ];
    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: rawRows,
      schema,
      metadata: { bigquery: { jobId: "job1" } }
    });

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    const row = decoded.rows[0];
    const fList = row.fields.f.listValue.values;
    expect(fList[0].structValue.fields.v.stringValue).equals("42");
    expect(fList[1].structValue.fields.v.stringValue).equals("val");
    expect(fList[2].structValue.fields.v.stringValue).equals("true");
    expect(fList[3].structValue.fields.v.nullValue).equals(0);

    expect(decoded.schemaFields.length).equals(4);
    expect(decoded.schemaFields[0].name).equals("num");
    expect(decoded.schemaFields[1].name).equals("str");

    verify(mockClient.executeRaw(statement, anything())).once();
    const capturedArgs = capture(mockClient.executeRaw).last();
    expect(capturedArgs[0]).to.equal(statement);
    const capturedOptions = capturedArgs[1];
    expect(capturedOptions.bigquery.location).equals("US");
    expect(capturedOptions.bigquery.labels).deep.equals({ key: "val" });
    expect(capturedOptions.bigquery.jobPrefix).equals("prefix");
    expect(capturedOptions.bigquery.dryRun).equals(true);
  });

  test("DeleteTable RPC calls adapter and returns empty buffer", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const target = { database: "db", schema: "sch", name: "tab" };
    const request = dataform.DeleteTableRequest.create({ target });
    const encodedRequest = dataform.DeleteTableRequest.encode(request).finish();

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "DeleteTable", encodedRequest);

    verify(mockAdapter.deleteTable(anything())).once();
    const capturedTarget = capture(mockAdapter.deleteTable).last()[0];
    expect(dataform.Target.create(capturedTarget)).deep.equals(dataform.Target.create(target));
    expect(response.length).equals(0);
  });

  test("Execute RPC handles null and empty result sets", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT null as n";
    const encodedRequest = dataform.ExecuteRequest.encode(dataform.ExecuteRequest.create({ statement })).finish();

    // Test with a null value
    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: [
        {
          f: [{ v: null }]
        }
      ],
      schema: [{ name: "n", primitive: dataform.Field.Primitive.STRING }],
      metadata: {}
    });

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    const fListNull = decoded.rows[0].fields.f.listValue.values;
    expect(fListNull[0].structValue.fields.v.nullValue).equals(0); // Protobuf NullValue.NULL_VALUE is 0

    verify(mockClient.executeRaw(statement, anything())).once();
    const capturedArgs1 = capture(mockClient.executeRaw).last();
    expect(capturedArgs1[0]).to.equal(statement);
    expect(capturedArgs1[1].bigquery.dryRun).equals(undefined);

    // Test with empty rows
    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: [],
      metadata: {}
    });

    const responseEmpty = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decodedEmpty = dataform.ExecuteResponse.decode(responseEmpty);
    expect(decodedEmpty.rows.length).equals(0);

    verify(mockClient.executeRaw(statement, anything())).twice();
    const capturedArgs2 = capture(mockClient.executeRaw).last();
    expect(capturedArgs2[0]).to.equal(statement);
    expect(capturedArgs2[1].bigquery.dryRun).equals(undefined);
  });

  test("ListTables RPC returns tables from adapter", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const request = dataform.ListTablesRequest.create({ database: "db", schema: "sch" });
    const encodedRequest = dataform.ListTablesRequest.encode(request).finish();

    const target1 = { database: "db", schema: "sch", name: "table1" };
    const metadata1 = { target: target1, type: dataform.TableMetadata.Type.TABLE } as any;
    when(mockAdapter.tables("db", "sch")).thenResolve([metadata1]);

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "ListTables", encodedRequest);
    const decoded = dataform.ListTablesResponse.decode(response);

    expect(decoded.tables.length).equals(1);
    expect(decoded.tables[0].target.name).equals("table1");

    verify(mockAdapter.tables("db", "sch")).once();
    verify(mockAdapter.table(anything())).never();
  });

  test("ListTables RPC throws error when database is missing", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    // Request without database
    const request = dataform.ListTablesRequest.create({ schema: "sch" });
    const encodedRequest = dataform.ListTablesRequest.encode(request).finish();

    try {
      await handleDbRequest(instance(mockAdapter), instance(mockClient), "ListTables", encodedRequest);
      expect.fail("Should have thrown an error");
    } catch (e) {
      expect(e.message).to.equal("ListTablesRequest.database must be supplied");
    }

    verify(mockAdapter.tables(anything(), anything())).never();
  });

  test("GetTable RPC returns metadata from adapter", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const target = { database: "db", schema: "sch", name: "tab" };
    const request = dataform.GetTableRequest.create({ target });
    const encodedRequest = dataform.GetTableRequest.encode(request).finish();

    when(mockAdapter.table(anything())).thenResolve({ target } as any);

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "GetTable", encodedRequest);
    const decoded = dataform.TableMetadata.decode(response);

    expect(decoded.target.name).equals("tab");
    verify(mockAdapter.table(anything())).once();
    const capturedTarget = capture(mockAdapter.table).last()[0];
    expect(dataform.Target.create(capturedTarget)).deep.equals(dataform.Target.create(target));
  });

  test("GetTable RPC throws error when table not found", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const target = { database: "db", schema: "sch", name: "missing" };
    const request = dataform.GetTableRequest.create({ target });
    const encodedRequest = dataform.GetTableRequest.encode(request).finish();

    // Adapter returns null for missing table
    when(mockAdapter.table(anything())).thenResolve(null);

    try {
      await handleDbRequest(instance(mockAdapter), instance(mockClient), "GetTable", encodedRequest);
      expect.fail("Should have thrown an error");
    } catch (e) {
      expect(e.message).to.contain("Table not found");
      expect(e.message).to.contain("missing");
    }

    verify(mockAdapter.table(anything())).once();
  });

  test("DeleteTable RPC respects global dryRun flag", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const request = dataform.DeleteTableRequest.create({
      target: { database: "db", schema: "sch", name: "tab" }
    });
    const encodedRequest = dataform.DeleteTableRequest.encode(request).finish();

    // Call with dryRun = true
    await handleDbRequest(instance(mockAdapter), instance(mockClient), "DeleteTable", encodedRequest, { dryRun: true });

    // Verify that the adapter method was NOT called
    verify(mockAdapter.deleteTable(anything())).never();
  });

  test("Execute RPC respects global dryRun flag", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT 1";
    const encodedRequest = dataform.ExecuteRequest.encode(dataform.ExecuteRequest.create({ statement })).finish();

    when(mockClient.executeRaw(anything(), anything())).thenResolve({ rows: [], metadata: {} });

    // Call with dryRun = true
    await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest, { dryRun: true });

    verify(mockClient.executeRaw(statement, anything())).once();
    const capturedArgs = capture(mockClient.executeRaw).last();
    expect(capturedArgs[0]).to.equal(statement);
    expect(capturedArgs[1].bigquery.dryRun).to.equal(true);
  });

  test("Throws error for unrecognized RPC method", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    try {
      await handleDbRequest(instance(mockAdapter), instance(mockClient), "UnknownMethod", new Uint8Array());
      expect.fail("Should have thrown");
    } catch (e) {
      expect(e.message).to.contain("Unrecognized RPC method");
    }
  });

  test("Execute RPC merges global BigQuery options with request-specific options", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();
    const statement = "SELECT 1";
    const executeRequest = dataform.ExecuteRequest.create({
      statement,
      bigQueryOptions: {
        location: "EU",
        labels: { request_label: "request_val" },
        jobPrefix: "request-prefix",
        dryRun: true
      }
    });
    const encodedRequest = dataform.ExecuteRequest.encode(executeRequest).finish();

    const globalOptions = {
      labels: { global_label: "global_val" },
      location: "US", // Request should override this to EU
      jobPrefix: "global-prefix"
    };

    when(mockClient.executeRaw(statement, anything())).thenResolve({ rows: [], metadata: {} });

    await handleDbRequest(
      instance(mockAdapter),
      instance(mockClient),
      "Execute",
      encodedRequest,
      globalOptions
    );

    verify(mockClient.executeRaw(statement, anything())).once();
    const capturedOptions = capture(mockClient.executeRaw).last()[1];

    // We expect both labels to be present
    expect(capturedOptions.bigquery.labels).deep.equals({
      global_label: "global_val",
      request_label: "request_val"
    });
    // We expect request location to override global location
    expect(capturedOptions.bigquery.location).equals("EU");
    // We expect job prefixes to be merged
    expect(capturedOptions.bigquery.jobPrefix).equals("global-prefix-request-prefix");
  });

  test("Execute RPC label merging: both global and request labels", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();
    const statement = "SELECT 1";
    const encodedRequest = dataform.ExecuteRequest.encode({
      statement,
      bigQueryOptions: { labels: { request_label: "request_val" } }
    }).finish();

    when(mockClient.executeRaw(statement, anything())).thenResolve({ rows: [], metadata: {} });

    await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest, {
      labels: {
        global_label: "global_val",
        request_label: "global_override_attempt"
      }
    });

    const capturedOptions = capture(mockClient.executeRaw).last()[1];
    expect(capturedOptions.bigquery.labels).deep.equals({
      global_label: "global_val",
      request_label: "request_val"
    });
  });

  test("Execute RPC label merging: undefined global, defined request", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();
    const statement = "SELECT 1";
    const encodedRequest = dataform.ExecuteRequest.encode({
      statement,
      bigQueryOptions: { labels: { request_label: "request_val" } }
    }).finish();

    when(mockClient.executeRaw(statement, anything())).thenResolve({ rows: [], metadata: {} });

    // Global options have no labels
    await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest, {
      location: "US"
    });

    const capturedOptions = capture(mockClient.executeRaw).last()[1];
    expect(capturedOptions.bigquery.labels).deep.equals({
      request_label: "request_val"
    });
  });

  test("Execute RPC label merging: empty global, defined request", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();
    const statement = "SELECT 1";
    const encodedRequest = dataform.ExecuteRequest.encode({
      statement,
      bigQueryOptions: { labels: { request_label: "request_val" } }
    }).finish();

    when(mockClient.executeRaw(statement, anything())).thenResolve({ rows: [], metadata: {} });

    // Global options have empty labels object
    await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest, {
      labels: {}
    });

    const capturedOptions = capture(mockClient.executeRaw).last()[1];
    expect(capturedOptions.bigquery.labels).deep.equals({
      request_label: "request_val"
    });
  });

  test("Execute RPC label merging: defined global, undefined request", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();
    const statement = "SELECT 1";
    // Request has no labels
    const encodedRequest = dataform.ExecuteRequest.encode({
      statement,
      bigQueryOptions: { location: "US" }
    }).finish();

    when(mockClient.executeRaw(statement, anything())).thenResolve({ rows: [], metadata: {} });

    await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest, {
      labels: { global_label: "global_val" }
    });

    const capturedOptions = capture(mockClient.executeRaw).last()[1];
    expect(capturedOptions.bigquery.labels).deep.equals({
      global_label: "global_val"
    });
  });

  test("Execute RPC label merging: defined global, empty request", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();
    const statement = "SELECT 1";
    // Request has empty labels
    const encodedRequest = dataform.ExecuteRequest.encode({
      statement,
      bigQueryOptions: { labels: {} }
    }).finish();

    when(mockClient.executeRaw(statement, anything())).thenResolve({ rows: [], metadata: {} });

    await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest, {
      labels: { global_label: "global_val" }
    });

    const capturedOptions = capture(mockClient.executeRaw).last()[1];
    expect(capturedOptions.bigquery.labels).deep.equals({
      global_label: "global_val"
    });
  });

  test("Execute RPC handles raw BigQuery f,v format results", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT * FROM table";
    const encodedRequest = dataform.ExecuteRequest.encode(dataform.ExecuteRequest.create({ statement })).finish();

    // Real raw BigQuery f/v format
    const rawRows = [
      {
        f: [
          { v: "42" }
        ]
      }
    ];

    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: rawRows,
      schema: [{ name: "id", primitive: dataform.Field.Primitive.STRING }],
      metadata: { bigquery: { jobId: "job1" } }
    });

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    const row = decoded.rows[0];
    expect(row.fields.f).to.not.equal(undefined);
    const fList = row.fields.f.listValue.values;
    expect(fList[0].structValue.fields.v.stringValue).equals("42");
    expect(decoded.schemaFields.length).equals(1);
    expect(decoded.schemaFields[0].name).equals("id");
  });

  test("Execute RPC preserves complex nested BigQuery f,v format", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT complex_struct FROM table";
    const encodedRequest = dataform.ExecuteRequest.encode(dataform.ExecuteRequest.create({ statement })).finish();

    // Real raw BigQuery complex nested f/v format
    const rawRows = [
      {
        f: [
          {
            v: {
              f: [
                { v: "nested_val" },
                { v: "123" }
              ]
            }
          }
        ]
      }
    ];

    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: rawRows,
      schema: [{ name: "complex_struct", primitive: dataform.Field.Primitive.STRING }],
      metadata: { bigquery: { jobId: "job1" } }
    });

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    const row = decoded.rows[0];
    const nestedStruct = row.fields.f.listValue.values[0].structValue.fields.v.structValue;
    const nestedFList = nestedStruct.fields.f.listValue.values;
    expect(nestedFList[0].structValue.fields.v.stringValue).equals("nested_val");
    expect(decoded.schemaFields.length).equals(1);
    expect(decoded.schemaFields[0].name).equals("complex_struct");
  });
});
