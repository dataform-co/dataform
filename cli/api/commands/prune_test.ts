import { expect } from "chai";

import { prune } from "df/cli/api/commands/prune";
import { dataform } from "df/protos/ts";
import { asPlainObject, suite, test } from "df/testing";

const TABLE_TARGET: dataform.ITarget = { database: "p", schema: "d", name: "books" };
const GRAPH_TARGET: dataform.ITarget = { database: "p", schema: "d", name: "LibraryGraph" };

const TABLE_ACTION: dataform.ITable = {
  type: "table",
  enumType: dataform.TableType.TABLE,
  target: TABLE_TARGET,
  canonicalTarget: TABLE_TARGET,
  query: "select 1 as id",
  tags: ["daily"],
  dependencyTargets: []
};

const GRAPH_ACTION: dataform.IPropertyGraph = {
  target: GRAPH_TARGET,
  canonicalTarget: GRAPH_TARGET,
  entities: [
    {
      name: "Book",
      dataSource: TABLE_TARGET,
      keys: ["id"]
    }
  ],
  relationships: [],
  dependencyTargets: [TABLE_TARGET],
  graphBody: "NODE TABLES (`p.d.books` KEY (id))"
};

const TAGGED_GRAPH_TARGET: dataform.ITarget = { database: "p", schema: "d", name: "NightlyGraph" };

const TAGGED_GRAPH_ACTION: dataform.IPropertyGraph = {
  target: TAGGED_GRAPH_TARGET,
  canonicalTarget: TAGGED_GRAPH_TARGET,
  entities: [
    {
      name: "Book",
      dataSource: TABLE_TARGET,
      keys: ["id"]
    }
  ],
  relationships: [],
  dependencyTargets: [TABLE_TARGET],
  graphBody: "NODE TABLES (`p.d.books` KEY (id))",
  tags: ["nightly"]
};

function makeCompiledGraph(): dataform.ICompiledGraph {
  return {
    tables: [TABLE_ACTION],
    operations: [],
    assertions: [],
    propertyGraphs: [GRAPH_ACTION],
    targets: [TABLE_TARGET, GRAPH_TARGET]
  };
}

suite("prune", () => {
  test("no selectors returns all actions including property graphs", () => {
    expect(asPlainObject(prune(makeCompiledGraph(), {}))).deep.equals(
      asPlainObject({
        tables: [TABLE_ACTION],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [TABLE_TARGET, GRAPH_TARGET]
      })
    );
  });

  test("--actions=<graph_name> selects only the graph", () => {
    expect(
      asPlainObject(prune(makeCompiledGraph(), { actions: ["LibraryGraph"] }))
    ).deep.equals(
      asPlainObject({
        tables: [],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [GRAPH_TARGET]
      })
    );
  });

  test("--actions=<graph_name> with includeDependencies pulls in the ref target", () => {
    expect(
      asPlainObject(
        prune(makeCompiledGraph(), {
          actions: ["LibraryGraph"],
          includeDependencies: true
        })
      )
    ).deep.equals(
      asPlainObject({
        tables: [TABLE_ACTION],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [TABLE_TARGET, GRAPH_TARGET]
      })
    );
  });

  test("--actions=<table_name> with includeDependents pulls in the graph", () => {
    expect(
      asPlainObject(
        prune(makeCompiledGraph(), {
          actions: ["books"],
          includeDependents: true
        })
      )
    ).deep.equals(
      asPlainObject({
        tables: [TABLE_ACTION],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [TABLE_TARGET, GRAPH_TARGET]
      })
    );
  });

  test("--tags matches tagged tables and skips untagged property graphs", () => {
    expect(asPlainObject(prune(makeCompiledGraph(), { tags: ["daily"] }))).deep.equals(
      asPlainObject({
        tables: [TABLE_ACTION],
        operations: [],
        assertions: [],
        propertyGraphs: [],
        targets: [TABLE_TARGET]
      })
    );
  });

  test("--tags matches tagged property graphs", () => {
    const compiledGraph: dataform.ICompiledGraph = {
      tables: [TABLE_ACTION],
      operations: [],
      assertions: [],
      propertyGraphs: [GRAPH_ACTION, TAGGED_GRAPH_ACTION],
      targets: [TABLE_TARGET, GRAPH_TARGET, TAGGED_GRAPH_TARGET]
    };
    expect(asPlainObject(prune(compiledGraph, { tags: ["nightly"] }))).deep.equals(
      asPlainObject({
        tables: [],
        operations: [],
        assertions: [],
        propertyGraphs: [TAGGED_GRAPH_ACTION],
        targets: [TAGGED_GRAPH_TARGET]
      })
    );
  });

  test("--actions=<pattern> matches graph by readable target string", () => {
    expect(
      asPlainObject(prune(makeCompiledGraph(), { actions: ["p.d.LibraryGraph"] }))
    ).deep.equals(
      asPlainObject({
        tables: [],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [GRAPH_TARGET]
      })
    );
  });
});
