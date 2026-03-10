import { expect } from "chai";

import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("BigQueryDbAdapter", () => {
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

    const adapter = new BigQueryDbAdapter({ projectId: "p" }, { concurrencyLimit: 1 });
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

    const adapter = new BigQueryDbAdapter({ projectId: "p" }, { concurrencyLimit: 1 });
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
