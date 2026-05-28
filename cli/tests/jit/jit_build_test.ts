import { expect } from "chai";

import { Builder } from "df/cli/api/commands/build";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("build", () => {
  test("jit_code is preserved in ExecutionAction", () => {
    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig: { warehouse: "bigquery" },
      tables: [
        {
          target: { database: "db", schema: "schema", name: "table" },
          jitCode: "console.log('jit table')",
          enumType: dataform.TableType.TABLE
        }
      ],
      operations: [
        {
          target: { database: "db", schema: "schema", name: "operation" },
          jitCode: "console.log('jit operation')",
          queries: []
        }
      ]
    });

    const builder = new Builder(compiledGraph, {}, { tables: [] });
    const executionGraph = builder.build();

    const tableAction = executionGraph.actions.find(
      (a: dataform.IExecutionAction) => a.target.name === "table"
    );
    expect(tableAction.jitCode).equals("console.log('jit table')");

    const operationAction = executionGraph.actions.find(
      (a: dataform.IExecutionAction) => a.target.name === "operation"
    );
    expect(operationAction.jitCode).equals("console.log('jit operation')");
  });
});
