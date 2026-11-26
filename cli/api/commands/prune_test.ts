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
  dependencyTargets: [],
};

const GRAPH_ACTION: dataform.IPropertyGraph = {
  target: GRAPH_TARGET,
  canonicalTarget: GRAPH_TARGET,
  nodes: [
    {
      name: "Book",
      dataSource: TABLE_TARGET,
      keys: ["id"],
    },
  ],
  edges: [],
  dependencyTargets: [TABLE_TARGET],
  graphBody: "NODE TABLES (`p.d.books` KEY (id))",
};

const TAGGED_GRAPH_TARGET: dataform.ITarget = { database: "p", schema: "d", name: "NightlyGraph" };

const TAGGED_GRAPH_ACTION: dataform.IPropertyGraph = {
  target: TAGGED_GRAPH_TARGET,
  canonicalTarget: TAGGED_GRAPH_TARGET,
  nodes: [
    {
      name: "Book",
      dataSource: TABLE_TARGET,
      keys: ["id"],
    },
  ],
  edges: [],
  dependencyTargets: [TABLE_TARGET],
  graphBody: "NODE TABLES (`p.d.books` KEY (id))",
  tags: ["nightly"],
};

function makeCompiledGraph(): dataform.ICompiledGraph {
  return {
    tables: [TABLE_ACTION],
    operations: [],
    assertions: [],
    propertyGraphs: [GRAPH_ACTION],
    targets: [TABLE_TARGET, GRAPH_TARGET],
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
        targets: [TABLE_TARGET, GRAPH_TARGET],
      }),
    );
  });

  test("--actions=<graph_name> selects only the graph", () => {
    expect(asPlainObject(prune(makeCompiledGraph(), { actions: ["LibraryGraph"] }))).deep.equals(
      asPlainObject({
        tables: [],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [GRAPH_TARGET],
      }),
    );
  });

  test("--actions=<graph_name> with includeDependencies pulls in the ref target", () => {
    expect(
      asPlainObject(
        prune(makeCompiledGraph(), {
          actions: ["LibraryGraph"],
          includeDependencies: true,
        }),
      ),
    ).deep.equals(
      asPlainObject({
        tables: [TABLE_ACTION],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [TABLE_TARGET, GRAPH_TARGET],
      }),
    );
  });

  test("--actions=<table_name> with includeDependents pulls in the graph", () => {
    expect(
      asPlainObject(
        prune(makeCompiledGraph(), {
          actions: ["books"],
          includeDependents: true,
        }),
      ),
    ).deep.equals(
      asPlainObject({
        tables: [TABLE_ACTION],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [TABLE_TARGET, GRAPH_TARGET],
      }),
    );
  });

  test("--tags matches tagged tables and skips untagged property graphs", () => {
    expect(asPlainObject(prune(makeCompiledGraph(), { tags: ["daily"] }))).deep.equals(
      asPlainObject({
        tables: [TABLE_ACTION],
        operations: [],
        assertions: [],
        propertyGraphs: [],
        targets: [TABLE_TARGET],
      }),
    );
  });

  test("--tags matches tagged property graphs", () => {
    const compiledGraph: dataform.ICompiledGraph = {
      tables: [TABLE_ACTION],
      operations: [],
      assertions: [],
      propertyGraphs: [GRAPH_ACTION, TAGGED_GRAPH_ACTION],
      targets: [TABLE_TARGET, GRAPH_TARGET, TAGGED_GRAPH_TARGET],
    };
    expect(asPlainObject(prune(compiledGraph, { tags: ["nightly"] }))).deep.equals(
      asPlainObject({
        tables: [],
        operations: [],
        assertions: [],
        propertyGraphs: [TAGGED_GRAPH_ACTION],
        targets: [TAGGED_GRAPH_TARGET],
      }),
    );
  });

  test("--actions=<pattern> matches graph by readable target string", () => {
    expect(
      asPlainObject(prune(makeCompiledGraph(), { actions: ["p.d.LibraryGraph"] })),
    ).deep.equals(
      asPlainObject({
        tables: [],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION],
        targets: [GRAPH_TARGET],
      }),
    );
  });

  test("--actions=<wildcard> matches property graphs and keeps targets in sync", () => {
    const compiledGraph: dataform.ICompiledGraph = {
      tables: [TABLE_ACTION],
      operations: [],
      assertions: [],
      propertyGraphs: [GRAPH_ACTION, TAGGED_GRAPH_ACTION],
      targets: [TABLE_TARGET, GRAPH_TARGET, TAGGED_GRAPH_TARGET],
    };
    expect(asPlainObject(prune(compiledGraph, { actions: ["*Graph"] }))).deep.equals(
      asPlainObject({
        tables: [],
        operations: [],
        assertions: [],
        propertyGraphs: [GRAPH_ACTION, TAGGED_GRAPH_ACTION],
        targets: [GRAPH_TARGET, TAGGED_GRAPH_TARGET],
      }),
    );
  });

  suite("targets colliding under targetAsReadableString", () => {
    const DEP_TABLE_TARGET: dataform.ITarget = { database: "proj", schema: "b", name: "dep_table" };
    const DEP_ASSERT_TARGET: dataform.ITarget = {
      database: "proj",
      schema: "b",
      name: "dep_assert",
    };
    const COLLIDING_TABLE_TARGET: dataform.ITarget = { database: "proj", schema: "b", name: "c" };
    const COLLIDING_ASSERT_TARGET: dataform.ITarget = { schema: "proj", name: "b.c" };
    const AFTER_TABLE_TARGET: dataform.ITarget = {
      database: "proj",
      schema: "b",
      name: "after_table",
    };
    const AFTER_ASSERT_TARGET: dataform.ITarget = {
      database: "proj",
      schema: "b",
      name: "after_assert",
    };

    const DEP_TABLE: dataform.ITable = {
      type: "view",
      enumType: dataform.TableType.VIEW,
      target: DEP_TABLE_TARGET,
      canonicalTarget: DEP_TABLE_TARGET,
      query: "select 1",
      dependencyTargets: [],
    };
    const DEP_ASSERT: dataform.ITable = {
      type: "view",
      enumType: dataform.TableType.VIEW,
      target: DEP_ASSERT_TARGET,
      canonicalTarget: DEP_ASSERT_TARGET,
      query: "select 2",
      dependencyTargets: [],
    };
    const COLLIDING_TABLE: dataform.ITable = {
      type: "view",
      enumType: dataform.TableType.VIEW,
      target: COLLIDING_TABLE_TARGET,
      canonicalTarget: COLLIDING_TABLE_TARGET,
      query: "select 3",
      tags: ["tag_tbl"],
      dependencyTargets: [DEP_TABLE_TARGET],
    };
    const COLLIDING_ASSERT: dataform.IAssertion = {
      target: COLLIDING_ASSERT_TARGET,
      canonicalTarget: COLLIDING_ASSERT_TARGET,
      query: "select 4",
      tags: ["tag_ass"],
      dependencyTargets: [DEP_ASSERT_TARGET],
    };
    const AFTER_TABLE: dataform.ITable = {
      type: "view",
      enumType: dataform.TableType.VIEW,
      target: AFTER_TABLE_TARGET,
      canonicalTarget: AFTER_TABLE_TARGET,
      query: "select 5",
      dependencyTargets: [COLLIDING_TABLE_TARGET],
    };
    const AFTER_ASSERT: dataform.ITable = {
      type: "view",
      enumType: dataform.TableType.VIEW,
      target: AFTER_ASSERT_TARGET,
      canonicalTarget: AFTER_ASSERT_TARGET,
      query: "select 6",
      dependencyTargets: [COLLIDING_ASSERT_TARGET],
    };

    function makeCollidingGraph(): dataform.ICompiledGraph {
      return {
        tables: [DEP_TABLE, DEP_ASSERT, COLLIDING_TABLE, AFTER_TABLE, AFTER_ASSERT],
        operations: [],
        assertions: [COLLIDING_ASSERT],
        propertyGraphs: [],
        targets: [
          DEP_TABLE_TARGET,
          DEP_ASSERT_TARGET,
          COLLIDING_TABLE_TARGET,
          COLLIDING_ASSERT_TARGET,
          AFTER_TABLE_TARGET,
          AFTER_ASSERT_TARGET,
        ],
      };
    }

    test("unqualified --actions throws ambiguous error when another action shares its short name", () => {
      const otherC: dataform.ITable = {
        type: "view",
        enumType: dataform.TableType.VIEW,
        target: { schema: "proj.b", name: "c" },
        canonicalTarget: { schema: "proj.b", name: "c" },
        query: "select 7",
        dependencyTargets: [],
      };
      const graph: dataform.ICompiledGraph = {
        tables: [COLLIDING_TABLE, otherC],
        operations: [],
        assertions: [],
        propertyGraphs: [],
        targets: [COLLIDING_TABLE_TARGET, otherC.target],
      };
      expect(() => prune(graph, { actions: ["c"] })).to.throw(
        "Ambiguous Action name: c. Did you mean one of: proj.b.c, proj.b.c.",
      );
    });

    test("dotted --actions selects all matching actions and walks all of their dependencies", () => {
      expect(
        asPlainObject(
          prune(makeCollidingGraph(), {
            actions: ["proj.b.c"],
            includeDependencies: true,
          }),
        ),
      ).deep.equals(
        asPlainObject({
          tables: [DEP_TABLE, DEP_ASSERT, COLLIDING_TABLE],
          operations: [],
          assertions: [COLLIDING_ASSERT],
          propertyGraphs: [],
          targets: [
            DEP_TABLE_TARGET,
            DEP_ASSERT_TARGET,
            COLLIDING_TABLE_TARGET,
            COLLIDING_ASSERT_TARGET,
          ],
        }),
      );
    });

    test("dotted --actions with includeDependents walks dependents of all matching actions", () => {
      expect(
        asPlainObject(
          prune(makeCollidingGraph(), {
            actions: ["proj.b.c"],
            includeDependents: true,
          }),
        ),
      ).deep.equals(
        asPlainObject({
          tables: [COLLIDING_TABLE, AFTER_TABLE, AFTER_ASSERT],
          operations: [],
          assertions: [COLLIDING_ASSERT],
          propertyGraphs: [],
          targets: [
            COLLIDING_TABLE_TARGET,
            COLLIDING_ASSERT_TARGET,
            AFTER_TABLE_TARGET,
            AFTER_ASSERT_TARGET,
          ],
        }),
      );
    });

    test("--tags selects only the tagged colliding action and walks only its dependencies", () => {
      expect(
        asPlainObject(
          prune(makeCollidingGraph(), {
            tags: ["tag_tbl"],
            includeDependencies: true,
          }),
        ),
      ).deep.equals(
        asPlainObject({
          tables: [DEP_TABLE, COLLIDING_TABLE],
          operations: [],
          assertions: [],
          propertyGraphs: [],
          targets: [DEP_TABLE_TARGET, COLLIDING_TABLE_TARGET],
        }),
      );
    });

    test("--tags with includeDependents walks only dependents of the tagged colliding action", () => {
      expect(
        asPlainObject(
          prune(makeCollidingGraph(), {
            tags: ["tag_tbl"],
            includeDependents: true,
          }),
        ),
      ).deep.equals(
        asPlainObject({
          tables: [COLLIDING_TABLE, AFTER_TABLE],
          operations: [],
          assertions: [],
          propertyGraphs: [],
          targets: [COLLIDING_TABLE_TARGET, AFTER_TABLE_TARGET],
        }),
      );
    });

    test("unqualified wildcard matches actual target.name without ambiguity and skips dotted names", () => {
      const otherC: dataform.ITable = {
        type: "view",
        enumType: dataform.TableType.VIEW,
        target: { schema: "proj.b", name: "c" },
        canonicalTarget: { schema: "proj.b", name: "c" },
        query: "select 7",
        dependencyTargets: [],
      };
      const graph: dataform.ICompiledGraph = {
        tables: [COLLIDING_TABLE, otherC],
        operations: [],
        assertions: [COLLIDING_ASSERT],
        propertyGraphs: [],
        targets: [COLLIDING_TABLE_TARGET, otherC.target, COLLIDING_ASSERT_TARGET],
      };

      // "c*" matches target.name === "c" on both tables without throwing ambiguity, and does not
      // match COLLIDING_ASSERT whose target.name is "b.c" (even though its readable string ends in ".c").
      expect(asPlainObject(prune(graph, { actions: ["c*"] }))).deep.equals(
        asPlainObject({
          tables: [COLLIDING_TABLE, otherC],
          operations: [],
          assertions: [],
          propertyGraphs: [],
          targets: [COLLIDING_TABLE_TARGET, otherC.target],
        }),
      );

      // "*c" matches all three because "c" and "b.c" both end in "c".
      expect(asPlainObject(prune(graph, { actions: ["*c"] }))).deep.equals(
        asPlainObject({
          tables: [COLLIDING_TABLE, otherC],
          operations: [],
          assertions: [COLLIDING_ASSERT],
          propertyGraphs: [],
          targets: [COLLIDING_TABLE_TARGET, otherC.target, COLLIDING_ASSERT_TARGET],
        }),
      );
    });

    test("qualified wildcard selects all colliding targets and walks all of their dependencies and dependents", () => {
      expect(
        asPlainObject(
          prune(makeCollidingGraph(), {
            actions: ["*.b.c"],
            includeDependencies: true,
          }),
        ),
      ).deep.equals(
        asPlainObject({
          tables: [DEP_TABLE, DEP_ASSERT, COLLIDING_TABLE],
          operations: [],
          assertions: [COLLIDING_ASSERT],
          propertyGraphs: [],
          targets: [
            DEP_TABLE_TARGET,
            DEP_ASSERT_TARGET,
            COLLIDING_TABLE_TARGET,
            COLLIDING_ASSERT_TARGET,
          ],
        }),
      );

      expect(
        asPlainObject(
          prune(makeCollidingGraph(), {
            actions: ["proj.**c"],
            includeDependents: true,
          }),
        ),
      ).deep.equals(
        asPlainObject({
          tables: [COLLIDING_TABLE, AFTER_TABLE, AFTER_ASSERT],
          operations: [],
          assertions: [COLLIDING_ASSERT],
          propertyGraphs: [],
          targets: [
            COLLIDING_TABLE_TARGET,
            COLLIDING_ASSERT_TARGET,
            AFTER_TABLE_TARGET,
            AFTER_ASSERT_TARGET,
          ],
        }),
      );
    });

    test("overlapping wildcard, exact and tag selectors do not duplicate actions or targets", () => {
      expect(
        asPlainObject(
          prune(makeCollidingGraph(), {
            actions: ["*", "proj.b.c", "*table"],
            tags: ["tag_tbl", "tag_ass"],
          }),
        ),
      ).deep.equals(asPlainObject(makeCollidingGraph()));
    });

    test("literal '*' in a target name does not wildcard-expand during dependency or dependent walks", () => {
      const starTarget: dataform.ITarget = { schema: "s", name: "op*" };
      const unrelatedTarget: dataform.ITarget = { schema: "s", name: "op_unrelated" };
      const consumerTarget: dataform.ITarget = { schema: "s", name: "consumer" };
      const starOp: dataform.IOperation = {
        target: starTarget,
        canonicalTarget: starTarget,
        queries: ["select 1"],
        dependencyTargets: [],
      };
      const unrelatedOp: dataform.IOperation = {
        target: unrelatedTarget,
        canonicalTarget: unrelatedTarget,
        queries: ["select 2"],
        dependencyTargets: [],
      };
      const consumerOp: dataform.IOperation = {
        target: consumerTarget,
        canonicalTarget: consumerTarget,
        queries: ["select 3"],
        dependencyTargets: [starTarget],
      };
      const graph: dataform.ICompiledGraph = {
        tables: [],
        operations: [starOp, unrelatedOp, consumerOp],
        assertions: [],
        propertyGraphs: [],
        targets: [starTarget, unrelatedTarget, consumerTarget],
      };

      expect(
        asPlainObject(prune(graph, { actions: ["consumer"], includeDependencies: true })),
      ).deep.equals(
        asPlainObject({
          tables: [],
          operations: [starOp, consumerOp],
          assertions: [],
          propertyGraphs: [],
          targets: [starTarget, consumerTarget],
        }),
      );

      expect(
        asPlainObject(prune(graph, { actions: ["op_unrelated"], includeDependents: true })),
      ).deep.equals(
        asPlainObject({
          tables: [],
          operations: [unrelatedOp],
          assertions: [],
          propertyGraphs: [],
          targets: [unrelatedTarget],
        }),
      );
    });
  });
});
