import { expect } from "chai";
import * as path from "path";

import { Session } from "df/core/session";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

// TODO(ekrekr): migrate the tests in this file to core/main_test.ts.

class TestConfigs {
  public static bigquery: dataform.IProjectConfig = {
    warehouse: "bigquery",
    defaultSchema: "schema",
    defaultLocation: "US"
  };

  public static bigqueryWithDefaultDatabase: dataform.IProjectConfig = {
    ...TestConfigs.bigquery,
    defaultDatabase: "default-database"
  };

  public static bigqueryWithSchemaSuffix: dataform.IProjectConfig = {
    ...TestConfigs.bigquery,
    schemaSuffix: "suffix"
  };

  public static bigqueryWithDefaultDatabaseAndSuffix: dataform.IProjectConfig = {
    ...TestConfigs.bigqueryWithDefaultDatabase,
    databaseSuffix: "suffix"
  };

  public static bigqueryWithTablePrefix: dataform.IProjectConfig = {
    ...TestConfigs.bigquery,
    tablePrefix: "prefix"
  };
}

suite("@dataform/core", () => {
  suite("publish", () => {
    [TestConfigs.bigquery, TestConfigs.bigqueryWithTablePrefix].forEach(testConfig => {
      test(`config with prefix "${testConfig.tablePrefix}"`, () => {
        const tableWithPrefix = (table: string) =>
          testConfig.tablePrefix ? `${testConfig.tablePrefix}_${table}` : table;
        const session = new Session(path.dirname(__filename), testConfig);
        session
          .publish("example", {
            type: "table",
            dependencies: [],
            description: "this is a table",
            columns: {
              test: "test description",
              test2: {
                description: "test2 description",
                tags: ["tag1", "tag2"]
              }
            }
          })
          .query(_ => "select 1 as test")
          .preOps(_ => ["pre_op"])
          .postOps(_ => ["post_op"]);
        session
          .publish("example", {
            type: "table",
            schema: "schema2",
            dependencies: [{ schema: "schema", name: "example" }],
            description: "test description"
          })
          .query(_ => "select 1 as test")
          .preOps(_ => ["pre_op"])
          .postOps(_ => ["post_op"]);
        session
          .publish("my_table", {
            type: "table",
            schema: "test_schema"
          })
          .query(_ => "SELECT 1 as one");

        const compiledGraph = session.compile();

        expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);

        const t = compiledGraph.tables.find(
          table => targetAsReadableString(table.target) === `schema.${tableWithPrefix("example")}`
        );
        expect(t.type).equals("table");
        expect(t.enumType).equals(dataform.TableType.TABLE);
        expect(t.actionDescriptor).eql({
          description: "this is a table",
          columns: [
            dataform.ColumnDescriptor.create({
              description: "test description",
              path: ["test"]
            }),
            dataform.ColumnDescriptor.create({
              description: "test2 description",
              path: ["test2"],
              tags: ["tag1", "tag2"]
            })
          ]
        });
        expect(t.preOps).deep.equals(["pre_op"]);
        expect(t.postOps).deep.equals(["post_op"]);

        const t2 = compiledGraph.tables.find(
          table => targetAsReadableString(table.target) === `schema2.${tableWithPrefix("example")}`
        );
        expect(t2.type).equals("table");
        expect(t2.enumType).equals(dataform.TableType.TABLE);
        expect(t2.actionDescriptor).eql({
          description: "test description"
        });
        expect(t2.preOps).deep.equals(["pre_op"]);
        expect(t2.postOps).deep.equals(["post_op"]);
        expect(t2.dependencyTargets.map(dependency => targetAsReadableString(dependency))).includes(
          `schema.${tableWithPrefix("example")}`
        );
        expect(dataform.Target.create(t2.canonicalTarget).toJSON()).deep.equals({
          name: "example",
          schema: "schema2"
        });

        const t3 = compiledGraph.tables.find(
          table =>
            targetAsReadableString(table.target) === `test_schema.${tableWithPrefix("my_table")}`
        );
        expect((t3.target.name = `${tableWithPrefix("my_table")}`));
        expect((t3.target.schema = "test_schema"));
        expect(t3.type).equals("table");
        expect(t3.enumType).equals(dataform.TableType.TABLE);
      });
    });

    [TestConfigs.bigquery, TestConfigs.bigqueryWithSchemaSuffix].forEach(testConfig => {
      test(`config with suffix "${testConfig.schemaSuffix}"`, () => {
        const schemaWithSuffix = (schema: string) =>
          testConfig.schemaSuffix ? `${schema}_${testConfig.schemaSuffix}` : schema;
        const session = new Session(path.dirname(__filename), testConfig);
        session
          .publish("example", {
            type: "table",
            dependencies: [],
            description: "this is a table",
            columns: {
              test: "test description"
            }
          })
          .query(_ => "select 1 as test")
          .preOps(_ => ["pre_op"])
          .postOps(_ => ["post_op"]);
        session
          .publish("example", {
            type: "table",
            schema: "schema2",
            dependencies: [{ schema: "schema", name: "example" }],
            description: "test description"
          })
          .query(_ => "select 1 as test")
          .preOps(_ => ["pre_op"])
          .postOps(_ => ["post_op"]);
        session
          .publish("my_table", {
            type: "table",
            schema: "test_schema"
          })
          .query(_ => "SELECT 1 as one");

        const compiledGraph = session.compile();

        expect(compiledGraph.graphErrors.compilationErrors).to.eql([]);

        const t = compiledGraph.tables.find(
          table => targetAsReadableString(table.target) === `${schemaWithSuffix("schema")}.example`
        );
        expect(t.type).equals("table");
        expect(t.enumType).equals(dataform.TableType.TABLE);
        expect(t.actionDescriptor).eql({
          description: "this is a table",
          columns: [
            dataform.ColumnDescriptor.create({
              description: "test description",
              path: ["test"]
            })
          ]
        });
        expect(t.preOps).deep.equals(["pre_op"]);
        expect(t.postOps).deep.equals(["post_op"]);

        const t2 = compiledGraph.tables.find(
          table => targetAsReadableString(table.target) === `${schemaWithSuffix("schema2")}.example`
        );
        expect(t2.type).equals("table");
        expect(t2.enumType).equals(dataform.TableType.TABLE);
        expect(t.actionDescriptor).eql({
          description: "this is a table",
          columns: [
            dataform.ColumnDescriptor.create({
              description: "test description",
              path: ["test"]
            })
          ]
        });
        expect(t2.preOps).deep.equals(["pre_op"]);
        expect(t2.postOps).deep.equals(["post_op"]);
        expect(t2.dependencyTargets.map(dependency => targetAsReadableString(dependency))).includes(
          `${schemaWithSuffix("schema")}.example`
        );

        const t3 = compiledGraph.tables.find(
          table =>
            targetAsReadableString(table.target) === `${schemaWithSuffix("test_schema")}.my_table`
        );
        expect((t3.target.name = "my_table"));
        expect((t3.target.schema = schemaWithSuffix("test_schema")));
        expect(t3.type).equals("table");
        expect(t3.enumType).equals(dataform.TableType.TABLE);
      });
    });

    test("incremental table", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session
        .publish("incremental", {
          type: "incremental"
        })
        .query(ctx => `select ${ctx.incremental()} as incremental`);
      const graph = session.compile();

      expect(graph.toJSON().tables).deep.members([
        {
          target: {
            name: "incremental",
            schema: TestConfigs.bigquery.defaultSchema
          },
          canonicalTarget: {
            name: "incremental",
            schema: TestConfigs.bigquery.defaultSchema
          },
          query: "select false as incremental",
          incrementalQuery: "select true as incremental",
          disabled: false,
          fileName: path.basename(__filename),
          type: "incremental",
          enumType: "INCREMENTAL"
        }
      ]);
    });

    test("canonical targets", () => {
      const originalConfig = {
        warehouse: "bigquery",
        defaultSchema: "schema",
        defaultDatabase: "database",
        schemaSuffix: "dev",
        tablePrefix: "dev"
      };
      const overrideConfig = {
        ...originalConfig,
        defaultSchema: "otherschema",
        defaultDatabase: "otherdatabase"
      };
      const session = new Session(path.dirname(__filename), overrideConfig, originalConfig);
      session.publish("dataset");
      session.assert("assertion");
      session.declare({ name: "declaration" });
      session.operate("operation");

      const graph = session.compile();
      expect(
        [
          ...graph.tables,
          ...graph.assertions,
          ...graph.declarations,
          ...graph.operations
        ].map(action => dataform.Target.create(action.canonicalTarget).toJSON())
      ).deep.equals([
        {
          database: "database",
          name: "dataset",
          schema: "schema"
        },
        {
          database: "database",
          name: "assertion",
          schema: "schema"
        },
        {
          database: "database",
          name: "declaration",
          schema: "schema"
        },
        {
          database: "database",
          name: "operation",
          schema: "schema"
        }
      ]);
    });

    test("non-unique canonical targets fails", () => {
      const originalConfig = {
        warehouse: "bigquery",
        defaultSchema: "schema",
        defaultDatabase: "database",
        defaultLocation: "US"
      };
      const overrideConfig = { ...originalConfig, defaultSchema: "otherschema" };
      const session = new Session(path.dirname(__filename), overrideConfig, originalConfig);
      session
        .publish("view", {
          type: "view"
        })
        .query("query");
      session.publish("view", {
        type: "view",
        schema: "schema"
      });
      const graph = session.compile();
      expect(graph.graphErrors.compilationErrors.map(error => error.message)).deep.equals(
        Array(2).fill(
          'Duplicate canonical target detected. Canonical targets must be unique across tables, declarations, assertions, and operations:\n"{"schema":"schema","name":"view","database":"database"}"'
        )
      );
    });

    test("validation_type_incremental", () => {
      const sessionSuccess = new Session(path.dirname(__filename), TestConfigs.bigquery);
      sessionSuccess
        .publish("exampleSuccess1", {
          type: "incremental"
        })
        .where("test1");
      sessionSuccess
        .publish("exampleSuccess2", ctx => `select field as 1`)
        .where("test2")
        .type("incremental");
      const cgSuccess = sessionSuccess.compile();
      expect(cgSuccess.graphErrors.compilationErrors).deep.equals([]);
    });

    test("validation_type", () => {
      const sessionSuccess = new Session(path.dirname(__filename), TestConfigs.bigquery);
      sessionSuccess.publish("exampleSuccess1", { type: "table" });
      sessionSuccess.publish("exampleSuccess2", { type: "view" });
      sessionSuccess.publish("exampleSuccess3", { type: "incremental" }).where("test");
      const cgSuccess = sessionSuccess.compile();
      expect(cgSuccess.graphErrors.compilationErrors).deep.equals([]);

      const sessionFail = new Session(path.dirname(__filename), TestConfigs.bigquery);
      sessionFail.publish("exampleFail", JSON.parse('{"type": "ta ble"}'));
      const cgFail = sessionFail.compile();

      expect(cgFail.toJSON().graphErrors.compilationErrors).deep.equals([
        {
          fileName: "core.spec.js",
          actionName: "schema.exampleFail",
          actionTarget: { schema: "schema", name: "exampleFail" },
          message:
            'Wrong type of table detected. Should only use predefined types: "table" | "view" | "incremental"'
        }
      ]);

      const err = cgFail.graphErrors.compilationErrors.find(
        e => e.actionName === "schema.exampleFail"
      );
      expect(err)
        .to.have.property("message")
        .that.matches(/Wrong type of table/);
    });

    test("validation_bigquery_fail", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("example_materialized_view", {
        type: "table",
        materialized: true
      });

      const expectedResults = [
        {
          name: "schema.example_materialized_view",
          message: "The 'materialized' option is only valid for BigQuery views"
        }
      ];

      const graph = session.compile();

      expect(
        graph.graphErrors.compilationErrors.map(compilationError => ({
          name: compilationError.actionName,
          message: compilationError.message
        }))
      ).to.have.deep.members(expectedResults);
    });

    test("validation_bigquery_fail", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("example_partitionBy_view_fail", {
        type: "view",
        bigquery: {
          partitionBy: "some_partition"
        }
      });
      session.publish("example_clusterBy_view_fail", {
        type: "view",
        bigquery: {
          clusterBy: ["some_cluster"]
        }
      });
      session.publish("example_expiring_view_fail", {
        type: "view",
        bigquery: {
          partitionExpirationDays: 7
        }
      });
      session.publish("example_require_partition_filter_view_fail", {
        type: "view",
        bigquery: {
          requirePartitionFilter: true
        }
      });
      session.publish("example_expiring_materialized_view_fail", {
        type: "view",
        materialized: true,
        bigquery: {
          partitionBy: "some_partition",
          clusterBy: ["some_cluster"],
          partitionExpirationDays: 7
        }
      });
      session.publish("example_require_partition_filter_materialized_view_fail", {
        type: "view",
        materialized: true,
        bigquery: {
          partitionBy: "some_partition",
          clusterBy: ["some_cluster"],
          requirePartitionFilter: true
        }
      });
      session.publish("example_materialize_table_fail", {
        type: "table",
        materialized: true
      });
      session.publish("example_expiring_non_partitioned_fail", {
        type: "table",
        bigquery: {
          partitionExpirationDays: 7
        }
      });
      session.publish("example_duplicate_partition_expiration_days_fail", {
        type: "table",
        bigquery: {
          partitionBy: "partition",
          partitionExpirationDays: 1,
          additionalOptions: {
            partition_expiration_days: "7"
          }
        }
      });
      session.publish("example_duplicate_require_partition_filter_fail", {
        type: "table",
        bigquery: {
          partitionBy: "partition",
          requirePartitionFilter: true,
          additionalOptions: {
            require_partition_filter: "false"
          }
        }
      });

      const graph = session.compile();

      expect(
        graph.graphErrors.compilationErrors.map(({ message, actionName }) => ({
          message,
          actionName
        }))
      ).has.deep.members([
        {
          actionName: "schema.example_partitionBy_view_fail",
          message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`
        },
        {
          actionName: "schema.example_clusterBy_view_fail",
          message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`
        },
        {
          actionName: "schema.example_expiring_view_fail",
          message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`
        },
        {
          actionName: "schema.example_require_partition_filter_view_fail",
          message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`
        },
        {
          actionName: "schema.example_expiring_materialized_view_fail",
          message: `requirePartitionFilter/partitionExpirationDays are not valid for BigQuery materialized views`
        },
        {
          actionName: "schema.example_require_partition_filter_materialized_view_fail",
          message: `requirePartitionFilter/partitionExpirationDays are not valid for BigQuery materialized views`
        },
        {
          actionName: "schema.example_materialize_table_fail",
          message: "The 'materialized' option is only valid for BigQuery views"
        },
        {
          actionName: "schema.example_expiring_non_partitioned_fail",
          message:
            "requirePartitionFilter/partitionExpirationDays are not valid for non partitioned BigQuery tables"
        },
        {
          actionName: "schema.example_duplicate_partition_expiration_days_fail",
          message: "partitionExpirationDays has been declared twice"
        },
        {
          actionName: "schema.example_duplicate_require_partition_filter_fail",
          message: "requirePartitionFilter has been declared twice"
        }
      ]);
    });

    test("validation_bigquery_pass", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("example_partitionBy_view_fail", {
        type: "table",
        bigquery: {
          partitionBy: "some_partition",
          clusterBy: ["some_column", "some_other_column"],
          partitionExpirationDays: 7,
          requirePartitionFilter: false
        }
      });
      session.publish("example_materialized_view", {
        type: "view",
        materialized: true
      });
      session.publish("example_additional_options", {
        type: "table",
        bigquery: {
          additionalOptions: {
            friendlyName: "name"
          }
        }
      });

      const graph = session.compile();

      expect(graph.tables[0].bigquery).to.deep.equals(
        dataform.BigQueryOptions.create({
          clusterBy: ["some_column", "some_other_column"],
          partitionBy: "some_partition",
          partitionExpirationDays: 7,
          requirePartitionFilter: false
        })
      );
      expect(graph.tables[1].materialized).to.equals(true);
      expect(graph.tables[2].bigquery).to.deep.equals(
        dataform.BigQueryOptions.create({
          additionalOptions: {
            friendlyName: "name"
          }
        })
      );
      expect(graph.graphErrors.compilationErrors).to.deep.equals([]);
    });

    [
      TestConfigs.bigquery,
      TestConfigs.bigqueryWithSchemaSuffix,
      TestConfigs.bigqueryWithTablePrefix
    ].forEach(testConfig => {
      test(`ref with prefix "${testConfig.tablePrefix}" and suffix "${testConfig.schemaSuffix}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        const suffix = testConfig.schemaSuffix ? `_${testConfig.schemaSuffix}` : "";
        const prefix = testConfig.tablePrefix ? `${testConfig.tablePrefix}_` : "";

        session.publish(`a`, _ => "select 1 as test");
        session.publish(`b`, ctx => `select * from ${ctx.ref("a")}`);
        session.publish(`c`, ctx => `select * from ${ctx.ref(undefined)}`);
        session.publish(`d`, ctx => `select * from ${ctx.ref({ schema: "schema", name: "a" })}`);
        session.publish(`g`, ctx => `select * from ${ctx.ref("schema", "a")}`);
        session.publish(`h`, ctx => `select * from ${ctx.ref(["schema", "a"])}`);
        session
          .publish("e", {
            schema: "foo"
          })
          .query(_ => "select 1 as test");
        session.publish("f", ctx => `select * from ${ctx.ref("e")}`);

        const graph = session.compile();

        const tableNames = graph.tables.map(table => targetAsReadableString(table.target));

        const baseEqlArray = [
          "schema.a",
          "schema.b",
          "schema.c",
          "schema.d",
          "schema.g",
          "schema.h",
          "foo.e",
          "schema.f"
        ];

        expect(tableNames).eql(
          baseEqlArray.map(item => {
            if (testConfig.tablePrefix) {
              const separatedItems = item.split(".");
              separatedItems[1] = `${prefix}${separatedItems[1]}`;
              return separatedItems.join(".");
            }

            if (testConfig.schemaSuffix) {
              const separatedItems = item.split(".");
              separatedItems[0] = `${separatedItems[0]}${suffix}`;
              return separatedItems.join(".");
            }

            return item;
          })
        );

        expect(
          graph.tables
            .find(table => targetAsReadableString(table.target) === `schema${suffix}.${prefix}b`)
            .dependencyTargets.map(dependency => targetAsReadableString(dependency))
        ).eql([`schema${suffix}.${prefix}a`]);
        expect(
          graph.tables
            .find(table => targetAsReadableString(table.target) === `schema${suffix}.${prefix}d`)
            .dependencyTargets.map(dependency => targetAsReadableString(dependency))
        ).eql([`schema${suffix}.${prefix}a`]);
        expect(
          graph.tables
            .find(table => targetAsReadableString(table.target) === `schema${suffix}.${prefix}g`)
            .dependencyTargets.map(dependency => targetAsReadableString(dependency))
        ).eql([`schema${suffix}.${prefix}a`]);
        expect(
          graph.tables
            .find(table => targetAsReadableString(table.target) === `schema${suffix}.${prefix}h`)
            .dependencyTargets.map(dependency => targetAsReadableString(dependency))
        ).eql([`schema${suffix}.${prefix}a`]);
        expect(
          graph.tables
            .find(table => targetAsReadableString(table.target) === `schema${suffix}.${prefix}f`)
            .dependencyTargets.map(dependency => targetAsReadableString(dependency))
        ).eql([`foo${suffix}.${prefix}e`]);

        const errors = graph.graphErrors.compilationErrors.map(item => item.message);
        expect(errors).includes("Action name is not specified");
        expect(graph.graphErrors.compilationErrors.length).eql(1);
      });
    });

    [
      { testConfig: TestConfigs.bigquery, target: "schema" },
      { testConfig: TestConfigs.bigqueryWithSchemaSuffix, target: "schema_suffix" }
    ].forEach(({ testConfig, target }) => {
      test(`schema/suffix: "${target}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.publish("test", { type: "table" }).query(ctx => ctx.schema());

        const graph = session.compile();

        const testTable = graph.tables.find(
          table => targetAsReadableString(table.target) === `${target}.test`
        );

        expect(testTable.query).deep.equals(target);
      });
    });

    [
      { testConfig: TestConfigs.bigquery, target: "schema.test", name: "test" },
      {
        testConfig: TestConfigs.bigqueryWithTablePrefix,
        target: "schema.prefix_test",
        name: "prefix_test"
      }
    ].forEach(({ testConfig, target, name }) => {
      test(`name/prefix: "${target}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.publish("test", { type: "table" }).query(ctx => ctx.name());

        const graph = session.compile();

        const testTable = graph.tables.find(
          table => targetAsReadableString(table.target) === target
        );

        expect(testTable.query).deep.equals(name);
      });
    });

    [
      {
        testConfig: TestConfigs.bigqueryWithDefaultDatabase,
        target: "default-database.schema.test",
        database: "default-database"
      },
      {
        testConfig: TestConfigs.bigqueryWithDefaultDatabaseAndSuffix,
        target: "default-database_suffix.schema.test",
        database: "default-database_suffix"
      }
    ].forEach(({ testConfig, target, database }) => {
      test(`database/suffix: "${target}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.publish("test", { type: "table" }).query(ctx => ctx.database());

        const graph = session.compile();

        const testTable = graph.tables.find(
          table => targetAsReadableString(table.target) === target
        );

        expect(testTable.query).deep.equals(database);
      });
    });

    test(`database fails when undefined`, () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("test", { type: "table" }).query(ctx => ctx.database());

      const graph = session.compile();

      const testTable = graph.tables.find(
        table => targetAsReadableString(table.target) === "schema.test"
      );

      expect(graph.graphErrors.compilationErrors[0].message).deep.equals(
        "Warehouse does not support multiple databases"
      );
      expect(testTable.query).deep.equals("");
    });
  });

  suite("operate", () => {
    test("ref", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.operate("operate-1", () => `select 1 as sample`).hasOutput(true);
      session.operate("operate-2", ctx => `select * from ${ctx.ref("operate-1")}`).hasOutput(true);

      const graph = session.compile();

      expect(graph.graphErrors.compilationErrors).deep.equals([]);
      expect(graph)
        .to.have.property("operations")
        .to.be.an("array")
        .to.have.lengthOf(2);

      expect(targetAsReadableString(graph.operations[0].target)).equals("schema.operate-1");
      expect(
        graph.operations[0].dependencyTargets.map(dependency => targetAsReadableString(dependency))
      ).deep.equals([]);
      expect(graph.operations[0].queries).deep.equals(["select 1 as sample"]);

      expect(targetAsReadableString(graph.operations[1].target)).equals("schema.operate-2");
      expect(
        graph.operations[1].dependencyTargets.map(dependency => targetAsReadableString(dependency))
      ).deep.equals(["schema.operate-1"]);
      expect(graph.operations[1].queries).deep.equals(["select * from `schema.operate-1`"]);
    });

    [
      { testConfig: TestConfigs.bigquery, finalizedSchema: "schema" },
      {
        testConfig: TestConfigs.bigqueryWithSchemaSuffix,
        finalizedSchema: "schema_suffix"
      }
    ].forEach(({ testConfig, finalizedSchema }) => {
      test(`schema with suffix: "${finalizedSchema}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.operate("operate-1", ctx => ctx.schema()).hasOutput(true);

        const graph = session.compile();

        expect(graph.operations[0].queries).deep.equals([finalizedSchema]);
      });
    });

    [
      { testConfig: TestConfigs.bigquery, finalizedName: "operate-1" },
      {
        testConfig: TestConfigs.bigqueryWithTablePrefix,
        finalizedName: "prefix_operate-1"
      }
    ].forEach(({ testConfig, finalizedName }) => {
      test(`name with prefix: "${finalizedName}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.operate("operate-1", ctx => ctx.name()).hasOutput(true);

        const graph = session.compile();

        expect(graph.operations[0].queries).deep.equals([finalizedName]);
      });
    });

    [
      {
        testConfig: TestConfigs.bigqueryWithDefaultDatabase,
        finalizedDatabase: "default-database"
      },
      {
        testConfig: TestConfigs.bigqueryWithDefaultDatabaseAndSuffix,
        finalizedDatabase: "default-database_suffix"
      }
    ].forEach(({ testConfig, finalizedDatabase }) => {
      test(`database with suffix: "${finalizedDatabase}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.operate("operate-1", ctx => ctx.database()).hasOutput(true);

        const graph = session.compile();

        expect(graph.operations[0].queries).deep.equals([finalizedDatabase]);
      });
    });

    test(`database fails when undefined`, () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);

      session.operate("operate-1", ctx => ctx.database()).hasOutput(true);

      const graph = session.compile();

      expect(graph.graphErrors.compilationErrors[0].message).deep.equals(
        "Warehouse does not support multiple databases"
      );
      expect(JSON.stringify(graph.operations[0].queries)).deep.equals('[""]');
    });
  });

  suite("graph", () => {
    test("circular_dependencies", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("a").dependencies("b");
      session.publish("b").dependencies("a");
      const cGraph = session.compile();
      expect(
        cGraph.graphErrors.compilationErrors.filter(item =>
          item.message.match(/Circular dependency/)
        ).length
      ).greaterThan(0);
    });

    test("missing_dependency", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("a", ctx => `select * from ${ctx.ref("b")}`);
      const cGraph = session.compile();
      expect(
        cGraph.graphErrors.compilationErrors.filter(item =>
          item.message.match(/Missing dependency/)
        ).length
      ).greaterThan(0);
    });

    test("duplicate_action_names", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("a").dependencies("b");
      session.publish("b");
      session.publish("a");
      const cGraph = session.compile();
      expect(
        cGraph.graphErrors.compilationErrors.filter(item =>
          item.message.match(/Duplicate action name/)
        ).length
      ).equals(2);
    });

    test("duplicate actions in compiled graph", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("a");
      session.publish("a");
      session.publish("b"); // unique action
      session.publish("c");

      session.operate("a");
      session.operate("d"); // unique action
      session.operate("e"); // unique action

      session.declare({ name: "a" });
      session.declare({ name: "f" }); // unique action
      session.declare({ name: "g" });

      session.assert("c");
      session.assert("g");

      const cGraph = session.compile();

      expect(
        [].concat(cGraph.tables, cGraph.assertions, cGraph.operations, cGraph.declarations).length
      ).equals(4);
    });

    test("same action names in different schemas (ambiguity)", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("a", { schema: "foo" });
      session.publish("a", { schema: "bar" });
      session.publish("b", { schema: "foo" }).dependencies("a");
      const cGraph = session.compile();
      expect(
        cGraph.graphErrors.compilationErrors.filter(item =>
          item.message.match(
            /Ambiguous Action name: {\"name\":\"a\"}. Did you mean one of: foo.a, bar.a./
          )
        ).length
      ).greaterThan(0);
    });

    test("same action name in same schema", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("a", { schema: "schema2" }).dependencies("b");
      session.publish("a", { schema: "schema2" });
      session.publish("b");
      const cGraph = session.compile();
      expect(
        cGraph.graphErrors.compilationErrors.filter(item =>
          item.message.match(/Duplicate action name detected. Names within a schema must be unique/)
        ).length
      ).equals(2);
    });

    test("same action names in different schemas", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("b");
      session.publish("a", { schema: "schema1" }).dependencies("b");
      session.publish("a", { schema: "schema2" });
      const cGraph = session.compile();
      expect(cGraph.graphErrors.compilationErrors).deep.equals([]);
    });

    test("semi-colons at the end of files throw", () => {
      // If this didn't happen, then the generated SQL could be incorrect
      // because of being broken up by semi-colons.
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("a", "select 1 as x;\n");
      session.assert("b", "select 1 as x;");
      const graph = session.compile();
      expect(graph.graphErrors.compilationErrors.map(error => error.message)).deep.equals([
        "Semi-colons are not allowed at the end of SQL statements.",
        "Semi-colons are not allowed at the end of SQL statements."
      ]);
    });

    test("defaultLocation must be set in BigQuery", () => {
      const session = new Session(path.dirname(__filename), {
        warehouse: "bigquery",
        defaultSchema: "schema"
      });
      const graph = session.compile();
      expect(graph.graphErrors.compilationErrors.map(error => error.message)).deep.equals([
        "A defaultLocation is required for BigQuery. This can be configured in workflow_settings.yaml."
      ]);
    });
  });
});
