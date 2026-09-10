import { dataform } from "df/protos/ts";

// c +-> b +-> a
//       ^
//       d
// Made with asciiflow.com
export const TEST_GRAPH: dataform.ICompiledGraph = dataform.CompiledGraph.create({
  projectConfig: { warehouse: "bigquery" },
  tables: [
    {
      type: "table",
      target: {
        schema: "schema",
        name: "a"
      },
      query: "query",
      dependencyTargets: [{ schema: "schema", name: "b" }]
    },
    {
      type: "table",
      target: {
        schema: "schema",
        name: "b"
      },
      query: "query",
      dependencyTargets: [{ schema: "schema", name: "c" }],
      disabled: true
    },
    {
      type: "table",
      target: {
        schema: "schema",
        name: "c"
      },
      query: "query"
    }
  ],
  assertions: [
    {
      target: {
        schema: "schema",
        name: "d"
      },
      parentAction: {
        schema: "schema",
        name: "b"
      }
    }
  ]
});
