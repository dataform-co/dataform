import { Dataset, Table } from "@google-cloud/bigquery";
import { expect } from "chai";
import { anything, instance, mock, verify, when } from "ts-mockito";

import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("BigQueryDbAdapter", () => {
  test("tables() with schema filters correctly", async () => {
    const mockBigQuery = mock<any>();
    const mockDataset = mock<Dataset>();
    const mockTable = mock<Table>();

    const tableName = "table1";
    const schemaName = "schema1";
    const projectId = "project1";

    const credentials = dataform.BigQuery.create({ projectId, location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, { bigqueryClient: instance(mockBigQuery) });

    when(mockBigQuery.dataset(schemaName)).thenReturn(instance(mockDataset));
    // getTables returns an array where the first element is an array of tables.
    // Each table object needs an 'id' property.
    when(mockDataset.getTables()).thenReturn(Promise.resolve([[{ id: tableName }]] as any));
    when(mockDataset.table(tableName)).thenReturn(instance(mockTable));
    when(mockTable.getMetadata()).thenReturn(
      Promise.resolve([
        {
          type: "TABLE",
          tableReference: { projectId, datasetId: schemaName, tableId: tableName },
          schema: { fields: [{ name: "col1", type: "STRING", mode: "NULLABLE" }] },
          lastModifiedTime: "123456789"
        }
      ] as any)
    );

    const result = await adapter.tables(projectId, schemaName);

    expect(result.length).to.equal(1);
    expect(result[0].target.database).to.equal(projectId);
    expect(result[0].target.schema).to.equal(schemaName);
    expect(result[0].target.name).to.equal(tableName);
    expect(result[0].fields.length).to.equal(1);
    expect(result[0].fields[0].name).to.equal("col1");
  });

  test("tables() without schema lists all datasets and tables", async () => {
    const mockBigQuery = mock<any>();
    const mockDataset = mock<Dataset>();
    const mockTable = mock<Table>();
    const schemaName = "schema1";
    const tableName = "table1";
    const projectId = "project";

    const credentials = dataform.BigQuery.create({ projectId, location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, { bigqueryClient: instance(mockBigQuery) });

    when(mockBigQuery.dataset(schemaName)).thenReturn(instance(mockDataset));
    when(mockDataset.getTables()).thenReturn(Promise.resolve([[{ id: tableName }]] as any));
    when(mockDataset.table(tableName)).thenReturn(instance(mockTable));
    when(mockTable.getMetadata()).thenReturn(
      Promise.resolve([
        {
          type: "TABLE",
          tableReference: { projectId, datasetId: schemaName, tableId: tableName },
          schema: { fields: [{ name: "col1", type: "STRING" }] },
          lastModifiedTime: "123456789"
        }
      ] as any)
    );

    when(mockBigQuery.getDatasets()).thenReturn(Promise.resolve([[{ id: schemaName }]] as any));

    const result = await adapter.tables(projectId);

    expect(result.length).to.equal(1);
    expect(result[0].target.database).to.equal(projectId);
    expect(result[0].target.schema).to.equal(schemaName);
    expect(result[0].target.name).to.equal(tableName);
  });

  test("setMetadata handles action without columns", async () => {
    // Partial mock for BigQuery client to avoid real network calls
    const mockBigQuery: any = {
      dataset: () => ({
        table: () => ({
          getMetadata: () => Promise.resolve([{ schema: { fields: [] } }]),
          setMetadata: (metadata: any) => {
            expect(metadata.description).to.equal("test");
            return Promise.resolve([]);
          }
        })
      })
    };

    const credentials = dataform.BigQuery.create({ projectId: "p", location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, { concurrencyLimit: 1 });
    (adapter as any).getClient = () => mockBigQuery;

    const action = dataform.ExecutionAction.create({
      target: { database: "db", schema: "sch", name: "tab" },
      actionDescriptor: { description: "test" }
      // columns is missing/null in this action
    });

    // This should not throw "cannot read property 'find' of undefined"
    await adapter.setMetadata(action);
  });

  test("setMetadata correctly maps column descriptions", async () => {
    const mockBigQuery: any = {
      dataset: () => ({
        table: () => ({
          getMetadata: () => Promise.resolve([{
            schema: {
              fields: [{ name: "id", type: "INTEGER" }]
            }
          }]),
          setMetadata: (metadata: any) => {
            expect(metadata.schema[0].description).to.equal("id desc");
            return Promise.resolve([]);
          }
        })
      })
    };

    const credentials = dataform.BigQuery.create({ projectId: "p", location: "US" });
    const adapter = new BigQueryDbAdapter(credentials, { concurrencyLimit: 1 });
    (adapter as any).getClient = () => mockBigQuery;

    const action = dataform.ExecutionAction.create({
      target: { database: "db", schema: "sch", name: "tab" },
      actionDescriptor: {
        columns: [{ path: ["id"], description: "id desc" }]
      }
    });

    await adapter.setMetadata(action);
  });
});
