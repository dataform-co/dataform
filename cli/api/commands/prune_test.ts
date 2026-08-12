import { expect } from "chai";

import { prune } from "df/cli/api/commands/prune";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

function makeCompiledGraph(): dataform.ICompiledGraph {
  const tableTarget: dataform.ITarget = {
    database: "p",
    schema: "d",
    name: "books"
  };
  const graphTarget: dataform.ITarget = {
    database: "p",
    schema: "d",
    name: "LibraryGraph"
  };
  return {
    tables: [
      {
        type: "table",
        enumType: dataform.TableType.TABLE,
        target: tableTarget,
        canonicalTarget: tableTarget,
        query: "select 1 as id",
        tags: ["daily"],
        dependencyTargets: []
      }
    ],
    operations: [],
    assertions: [],
    propertyGraphs: [
      {
        target: graphTarget,
        canonicalTarget: graphTarget,
        entities: [
          {
            name: "Book",
            dataSource: tableTarget,
            keys: ["id"]
          }
        ],
        relationships: [],
        dependencyTargets: [tableTarget],
        graphBody: "NODE TABLES (`p.d.books` KEY (id))"
      }
    ],
    targets: [tableTarget, graphTarget]
  };
}

suite("prune", () => {
  test("no selectors returns all actions including property graphs", () => {
    const compiled = makeCompiledGraph();
    const pruned = prune(compiled, {});
    expect(pruned.tables).to.have.lengthOf(1);
    expect(pruned.propertyGraphs).to.have.lengthOf(1);
    expect(pruned.propertyGraphs[0].target.name).equals("LibraryGraph");
  });

  test("--actions=<graph_name> selects only the graph", () => {
    const compiled = makeCompiledGraph();
    const pruned = prune(compiled, { actions: ["LibraryGraph"] });
    expect(pruned.tables).to.have.lengthOf(0);
    expect(pruned.propertyGraphs).to.have.lengthOf(1);
    expect(pruned.propertyGraphs[0].target.name).equals("LibraryGraph");
    expect(pruned.targets.map(t => t.name)).deep.equals(["LibraryGraph"]);
  });

  test("--actions=<graph_name> with includeDependencies pulls in the ref target", () => {
    const compiled = makeCompiledGraph();
    const pruned = prune(compiled, {
      actions: ["LibraryGraph"],
      includeDependencies: true
    });
    expect(pruned.tables).to.have.lengthOf(1);
    expect(pruned.propertyGraphs).to.have.lengthOf(1);
    expect(pruned.tables[0].target.name).equals("books");
  });

  test("--actions=<table_name> with includeDependents pulls in the graph", () => {
    const compiled = makeCompiledGraph();
    const pruned = prune(compiled, {
      actions: ["books"],
      includeDependents: true
    });
    expect(pruned.tables).to.have.lengthOf(1);
    expect(pruned.propertyGraphs).to.have.lengthOf(1);
  });

  test("--tags matches tagged tables but never property graphs", () => {
    const compiled = makeCompiledGraph();
    const pruned = prune(compiled, { tags: ["daily"] });
    expect(pruned.tables).to.have.lengthOf(1);
    expect(pruned.propertyGraphs).to.have.lengthOf(0);
  });

  test("--actions=<pattern> matches graph by readable target string", () => {
    const compiled = makeCompiledGraph();
    const pruned = prune(compiled, { actions: ["p.d.LibraryGraph"] });
    expect(pruned.tables).to.have.lengthOf(0);
    expect(pruned.propertyGraphs).to.have.lengthOf(1);
  });
});
