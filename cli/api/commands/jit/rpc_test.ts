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

    // Data in the format google.protobuf.IStruct expects
    const rawRows = [
      {
        fields: {
          num: { numberValue: 42 },
          str: { stringValue: "val" },
          bool: { boolValue: true },
          n: { nullValue: 0 }
        }
      }
    ];

    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: [],
      rawRows,
      metadata: { bigquery: { jobId: "job1" } }
    });

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    const fields = decoded.rows[0].fields;
    expect(fields.num.numberValue).equals(42);
    expect(fields.str.stringValue).equals("val");
    expect(fields.bool.boolValue).equals(true);
    expect(fields.n.nullValue).equals(0);

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
      rows: [],
      rawRows: [
        {
          fields: {
            n: { nullValue: 0 }
          }
        }
      ],
      metadata: {}
    });

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    expect(decoded.rows[0].fields.n.nullValue).equals(0); // Protobuf NullValue.NULL_VALUE is 0

    verify(mockClient.executeRaw(statement, anything())).once();
    const capturedArgs1 = capture(mockClient.executeRaw).last();
    expect(capturedArgs1[0]).to.equal(statement);
    expect(capturedArgs1[1].bigquery.dryRun).to.equal(false);

    // Test with empty rows
    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: [],
      rawRows: [],
      metadata: {}
    });

    const responseEmpty = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decodedEmpty = dataform.ExecuteResponse.decode(responseEmpty);
    expect(decodedEmpty.rows.length).equals(0);

    verify(mockClient.executeRaw(statement, anything())).twice();
    const capturedArgs2 = capture(mockClient.executeRaw).last();
    expect(capturedArgs2[0]).to.equal(statement);
    expect(capturedArgs2[1].bigquery.dryRun).to.equal(false);
  });

  test("ListTables RPC returns tables from adapter", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const request = dataform.ListTablesRequest.create({ database: "db", schema: "sch" });
    const encodedRequest = dataform.ListTablesRequest.encode(request).finish();

    const target1 = { database: "db", schema: "sch", name: "table1" };
    when(mockAdapter.tables()).thenResolve([target1]);
    when(mockAdapter.table(anything())).thenResolve({ target: target1 } as any);

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "ListTables", encodedRequest);
    const decoded = dataform.ListTablesResponse.decode(response);

    expect(decoded.tables.length).equals(1);
    expect(decoded.tables[0].target.name).equals("table1");

    verify(mockAdapter.tables()).once();
    verify(mockAdapter.table(anything())).once();
    const capturedTarget = capture(mockAdapter.table).last()[0];
    expect(dataform.Target.create(capturedTarget)).deep.equals(dataform.Target.create(target1));
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

  test("GetTable RPC returns empty metadata when table not found", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const target = { database: "db", schema: "sch", name: "missing" };
    const request = dataform.GetTableRequest.create({ target });
    const encodedRequest = dataform.GetTableRequest.encode(request).finish();

    // Adapter returns null for missing table
    when(mockAdapter.table(anything())).thenResolve(null);

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "GetTable", encodedRequest);
    const decoded = dataform.TableMetadata.decode(response);

    // Proto instances for empty messages have null target in protobufjs decoded objects
    expect(decoded.target).to.equal(null);

    verify(mockAdapter.table(anything())).once();
    const capturedTarget = capture(mockAdapter.table).last()[0];
    expect(dataform.Target.create(capturedTarget)).deep.equals(dataform.Target.create(target));
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

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "ListTables", encodedRequest);
    const decoded = dataform.ListTablesResponse.decode(response);

    // Should only contain target1
    expect(decoded.tables.length).equals(1);
    expect(decoded.tables[0].target.schema).equals("sch1");

    verify(mockAdapter.tables()).once();
    verify(mockAdapter.table(anything())).once();
    const capturedTarget = capture(mockAdapter.table).last()[0];
    expect(dataform.Target.create(capturedTarget)).deep.equals(dataform.Target.create(target1));
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

    when(mockClient.executeRaw(anything(), anything())).thenResolve({ rows: [], rawRows: [], metadata: {} });

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
        labels: { request_label: "request_val" },
        jobPrefix: "request-prefix"
      }
    });
    const encodedRequest = dataform.ExecuteRequest.encode(executeRequest).finish();

    const globalOptions = {
      labels: { global_label: "global_val" },
      location: "EU",
      jobPrefix: "global-prefix"
    };

    when(mockClient.executeRaw(statement, anything())).thenResolve({ rows: [], rawRows: [], metadata: {} });

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
    // We expect global location to be applied
    expect(capturedOptions.bigquery.location).equals("EU");
    // We expect job prefixes to be merged
    expect(capturedOptions.bigquery.jobPrefix).equals("global-prefix-request-prefix");
  });

  test("Execute RPC handles raw BigQuery f,v format results", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT * FROM table";
    const encodedRequest = dataform.ExecuteRequest.encode(dataform.ExecuteRequest.create({ statement })).finish();

    // Data in the format google.protobuf.IStruct expects for raw "f,v"
    const rawRows = [
      {
        fields: {
          f: {
            listValue: {
              values: [
                { structValue: { fields: { v: { stringValue: "42" } } } }
              ]
            }
          }
        }
      }
    ];

    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: [],
      rawRows,
      metadata: { bigquery: { jobId: "job1" } }
    });

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    expect(decoded.rows[0].fields.f).to.not.equal(undefined);
    const fList = decoded.rows[0].fields.f.listValue.values;
    expect(fList[0].structValue.fields.v.stringValue).equals("42");
  });

  test("Execute RPC preserves complex nested BigQuery f,v format", async () => {
    const mockAdapter = mock<IDbAdapter>();
    const mockClient = mock<IDbClient>();

    const statement = "SELECT complex_struct FROM table";
    const encodedRequest = dataform.ExecuteRequest.encode(dataform.ExecuteRequest.create({ statement })).finish();

    // Data in the format google.protobuf.IStruct expects for complex nested "f,v"
    const rawRows = [
      {
        fields: {
          f: {
            listValue: {
              values: [
                {
                  structValue: {
                    fields: {
                      v: {
                        structValue: {
                          fields: {
                            f: {
                              listValue: {
                                values: [
                                  { structValue: { fields: { v: { stringValue: "nested_val" } } } },
                                  { structValue: { fields: { v: { stringValue: "123" } } } }
                                ]
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              ]
            }
          }
        }
      }
    ];

    when(mockClient.executeRaw(statement, anything())).thenResolve({
      rows: [],
      rawRows,
      metadata: { bigquery: { jobId: "job1" } }
    });

    const response = await handleDbRequest(instance(mockAdapter), instance(mockClient), "Execute", encodedRequest);
    const decoded = dataform.ExecuteResponse.decode(response);

    expect(decoded.rows.length).equals(1);
    const row = decoded.rows[0];
    const nestedStruct = row.fields.f.listValue.values[0].structValue.fields.v.structValue;
    const nestedFList = nestedStruct.fields.f.listValue.values;
    expect(nestedFList[0].structValue.fields.v.stringValue).equals("nested_val");
  });
});
