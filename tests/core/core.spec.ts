import { expect } from "chai";
import * as fs from "fs-extra";
import * as path from "path";

import * as compilers from "df/core/compilers";
import { Session } from "df/core/session";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { asPlainObject } from "df/tests/utils";

class TestConfigs {
  public static redshift: dataform.IProjectConfig = {
    warehouse: "redshift",
    defaultSchema: "schema"
  };

  public static redshiftWithSuffix: dataform.IProjectConfig = {
    ...TestConfigs.redshift,
    schemaSuffix: "suffix"
  };

  public static redshiftWithPrefix: dataform.IProjectConfig = {
    ...TestConfigs.redshift,
    tablePrefix: "prefix"
  };

  public static bigquery: dataform.IProjectConfig = {
    warehouse: "bigquery",
    defaultSchema: "schema",
    defaultLocation: "US"
  };

  public static bigqueryWithDatabase: dataform.IProjectConfig = {
    ...TestConfigs.bigquery,
    defaultDatabase: "test-db"
  };

  public static bigqueryWithDatabaseAndSuffix: dataform.IProjectConfig = {
    ...TestConfigs.bigqueryWithDatabase,
    databaseSuffix: "suffix"
  };

  public static snowflake: dataform.IProjectConfig = {
    warehouse: "snowflake",
    defaultSchema: "schema"
  };
}

suite("@dataform/core", () => {
  suite("publish", () => {
    [TestConfigs.redshift, TestConfigs.redshiftWithPrefix].forEach(testConfig => {
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

    [TestConfigs.redshift, TestConfigs.redshiftWithSuffix].forEach(testConfig => {
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
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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
            schema: TestConfigs.redshift.defaultSchema
          },
          canonicalTarget: {
            name: "incremental",
            schema: TestConfigs.redshift.defaultSchema
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
      const sessionSuccess = new Session(path.dirname(__filename), TestConfigs.redshift);
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
      const sessionSuccess = new Session(path.dirname(__filename), TestConfigs.redshift);
      sessionSuccess.publish("exampleSuccess1", { type: "table" });
      sessionSuccess.publish("exampleSuccess2", { type: "view" });
      sessionSuccess.publish("exampleSuccess3", { type: "incremental" }).where("test");
      const cgSuccess = sessionSuccess.compile();
      expect(cgSuccess.graphErrors.compilationErrors).deep.equals([]);

      const sessionFail = new Session(path.dirname(__filename), TestConfigs.redshift);
      sessionFail.publish("exampleFail", JSON.parse('{"type": "ta ble"}'));
      const cgFail = sessionFail.compile();

      expect(cgFail.toJSON().graphErrors.compilationErrors).deep.equals([
        {
          fileName: "core.spec.js",
          actionName: "schema.exampleFail",
          actionTarget: { schema: "schema", name: "exampleFail" },
          message:
            'Wrong type of table detected. Should only use predefined types: "table" | "view" | "incremental" | "inline"'
        }
      ]);

      const err = cgFail.graphErrors.compilationErrors.find(
        e => e.actionName === "schema.exampleFail"
      );
      expect(err)
        .to.have.property("message")
        .that.matches(/Wrong type of table/);
    });

    test("validation_redshift_success", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("example_without_dist", {
        redshift: {
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        }
      });
      session.publish("example_without_sort", {
        redshift: {
          distKey: "column1",
          distStyle: "even"
        }
      });

      const graph = session.compile();

      expect(graph)
        .to.have.property("tables")
        .to.be.an("array")
        .to.have.lengthOf(2);

      expect(graph.graphErrors.compilationErrors).deep.equals([]);
    });

    test("validation_redshift_fail", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("example_absent_distKey", {
        redshift: {
          distStyle: "even",
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        }
      });
      session.publish("example_absent_distStyle", {
        redshift: {
          distKey: "column1",
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        }
      });
      session.publish("example_wrong_distStyle", {
        redshift: {
          distKey: "column1",
          distStyle: "wrong_even",
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        }
      });
      session.publish("example_absent_sortKeys", {
        redshift: {
          distKey: "column1",
          distStyle: "even",
          sortStyle: "compound"
        }
      });
      session.publish("example_empty_sortKeys", {
        redshift: {
          distKey: "column1",
          distStyle: "even",
          sortKeys: [],
          sortStyle: "compound"
        }
      });
      session.publish("example_absent_sortStyle", {
        redshift: {
          distKey: "column1",
          distStyle: "even",
          sortKeys: ["column1", "column2"]
        }
      });
      session.publish("example_wrong_sortStyle", {
        redshift: {
          distKey: "column1",
          distStyle: "even",
          sortKeys: ["column1", "column2"],
          sortStyle: "wrong_sortStyle"
        }
      });
      session.publish("example_materialized_view", {
        type: "view",
        materialized: true
      });

      const expectedResults = [
        { name: "schema.example_absent_distKey", message: `Property "distKey" is not defined` },
        { name: "schema.example_absent_distStyle", message: `Property "distStyle" is not defined` },
        {
          name: "schema.example_wrong_distStyle",
          message: `Wrong value of "distStyle" property. Should only use predefined values: "even" | "key" | "all"`
        },
        { name: "schema.example_absent_sortKeys", message: `Property "sortKeys" is not defined` },
        { name: "schema.example_empty_sortKeys", message: `Property "sortKeys" is not defined` },
        { name: "schema.example_absent_sortStyle", message: `Property "sortStyle" is not defined` },
        {
          name: "schema.example_wrong_sortStyle",
          message: `Wrong value of "sortStyle" property. Should only use predefined values: "compound" | "interleaved"`
        },
        {
          name: "schema.example_materialized_view",
          message: "The 'materialized' option is only valid for Snowflake and BigQuery views"
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
          message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views; they are only valid for tables`
        },
        {
          actionName: "schema.example_clusterBy_view_fail",
          message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views; they are only valid for tables`
        },
        {
          actionName: "schema.example_expiring_view_fail",
          message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views; they are only valid for tables`
        },
        {
          actionName: "schema.example_materialize_table_fail",
          message: "The 'materialized' option is only valid for Snowflake and BigQuery views"
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

    test("validation_snowflake_fail", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.snowflake);
      session.publish("example_secure_table_fail", {
        type: "table",
        snowflake: {
          secure: true
        }
      });
      session.publish("example_transient_view_fail", {
        type: "view",
        snowflake: {
          transient: true
        }
      });
      session.publish("example_cluster_by_view_fail", {
        type: "view",
        snowflake: {
          clusterBy: ["a"]
        }
      });
      session.publish("example_materialize_table_fail", {
        type: "table",
        materialized: true
      });

      const graph = session.compile();

      expect(
        graph.graphErrors.compilationErrors.map(({ message, actionName }) => ({
          message,
          actionName
        }))
      ).has.deep.members([
        {
          actionName: "SCHEMA.EXAMPLE_SECURE_TABLE_FAIL",
          message: "The 'secure' option is only valid for Snowflake views"
        },
        {
          actionName: "SCHEMA.EXAMPLE_TRANSIENT_VIEW_FAIL",
          message: "The 'transient' option is only valid for Snowflake tables"
        },
        {
          actionName: "SCHEMA.EXAMPLE_CLUSTER_BY_VIEW_FAIL",
          message: "The 'clusterBy' option is only valid for Snowflake tables"
        },
        {
          actionName: "SCHEMA.EXAMPLE_MATERIALIZE_TABLE_FAIL",
          message: "The 'materialized' option is only valid for Snowflake and BigQuery views"
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

    test("validation_type_inline", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("a", { type: "table" }).query(_ => "select 1 as test");
      session
        .publish("b", {
          type: "inline",
          redshift: {
            distKey: "column1",
            distStyle: "even",
            sortKeys: ["column1", "column2"],
            sortStyle: "compound"
          },
          columns: { test: "test description b" },
          disabled: true
        })
        .preOps(_ => ["pre_op_b"])
        .postOps(_ => ["post_op_b"])
        .where("test_where")
        .query(ctx => `select * from ${ctx.ref("a")}`);
      session
        .publish("c", {
          type: "table",
          columns: { test: "test description c" }
        })
        .preOps(_ => ["pre_op_c"])
        .postOps(_ => ["post_op_c"])
        .query(ctx => `select * from ${ctx.ref("b")}`);

      const graph = session.compile();

      const tableA = graph.tables.find(
        table => targetAsReadableString(table.target) === "schema.a"
      );
      expect(tableA.type).equals("table");
      expect(tableA.enumType).equals(dataform.TableType.TABLE);
      expect(
        tableA.dependencyTargets.map(dependency => targetAsReadableString(dependency))
      ).deep.equals([]);
      expect(tableA.query).equals("select 1 as test");

      const tableB = graph.tables.find(
        table => targetAsReadableString(table.target) === "schema.b"
      );
      expect(tableB.type).equals("inline");
      expect(tableB.enumType).equals(dataform.TableType.INLINE);
      expect(
        tableB.dependencyTargets.map(dependency => targetAsReadableString(dependency))
      ).includes("schema.a");
      expect(tableB.actionDescriptor).eql({
        columns: [
          dataform.ColumnDescriptor.create({
            description: "test description b",
            path: ["test"]
          })
        ]
      });
      expect(tableB.preOps).deep.equals(["pre_op_b"]);
      expect(tableB.postOps).deep.equals(["post_op_b"]);
      expect(asPlainObject(tableB.redshift)).deep.equals(
        asPlainObject({
          distKey: "column1",
          distStyle: "even",
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        })
      );
      expect(tableB.disabled).equals(true);
      expect(tableB.where).equals("test_where");
      expect(tableB.query).equals('select * from "schema"."a"');

      const tableC = graph.tables.find(
        table => targetAsReadableString(table.target) === "schema.c"
      );
      expect(tableC.type).equals("table");
      expect(tableC.enumType).equals(dataform.TableType.TABLE);
      expect(
        tableC.dependencyTargets.map(dependency => targetAsReadableString(dependency))
      ).includes("schema.a");
      expect(tableC.actionDescriptor).eql({
        columns: [
          dataform.ColumnDescriptor.create({
            description: "test description c",
            path: ["test"]
          })
        ]
      });
      expect(tableC.preOps).deep.equals(["pre_op_c"]);
      expect(tableC.postOps).deep.equals(["post_op_c"]);
      expect(tableC.redshift).equals(null);
      expect(tableC.disabled).equals(false);
      expect(tableC.where).equals("");
      expect(tableC.query).equals('select * from (select * from "schema"."a")');

      const errors = graph.graphErrors.compilationErrors
        .filter(item => item.actionName === "schema.b")
        .map(item => item.message);

      expect(errors).that.matches(/Unused property was detected: "preOps"/);
      expect(errors).that.matches(/Unused property was detected: "postOps"/);
      expect(errors).that.matches(/Unused property was detected: "redshift"/);
      expect(errors).that.matches(/Unused property was detected: "disabled"/);
      expect(errors).that.matches(/Unused property was detected: "where"/);
    });

    test("validation_navigator_descriptors", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session
        .publish("a", {
          type: "table",
          columns: {
            colly: {
              displayName: "colly display name",
              description: "colly description",
              dimension: "timestamp",
              aggregator: "distinct",
              expression: "1"
            }
          }
        })
        .query(_ => "select 1 as test");

      const graph = session.compile();
      expect(graph.graphErrors.compilationErrors).deep.equals([]);

      const schema = graph.tables.find(
        table => targetAsReadableString(table.target) === "schema.a"
      );

      const collyColumn = schema.actionDescriptor.columns.find(
        column => column.displayName === "colly display name"
      );
      expect(collyColumn).to.eql(
        dataform.ColumnDescriptor.create({
          path: ["colly"],
          displayName: "colly display name",
          description: "colly description",
          dimensionType: dataform.ColumnDescriptor.DimensionType.TIMESTAMP,
          aggregation: dataform.ColumnDescriptor.Aggregation.DISTINCT,
          expression: "1"
        })
      );
    });

    [TestConfigs.redshift, TestConfigs.redshiftWithPrefix, TestConfigs.redshiftWithSuffix].forEach(
      testConfig => {
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
      }
    );

    [
      { testConfig: TestConfigs.redshift, target: "schema" },
      { testConfig: TestConfigs.redshiftWithSuffix, target: "schema_suffix" }
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
      { testConfig: TestConfigs.redshift, target: "schema.test", name: "test" },
      {
        testConfig: TestConfigs.redshiftWithPrefix,
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
        testConfig: TestConfigs.bigqueryWithDatabase,
        target: "test-db.schema.test",
        database: "test-db"
      },
      {
        testConfig: TestConfigs.bigqueryWithDatabaseAndSuffix,
        target: "test-db_suffix.schema.test",
        database: "test-db_suffix"
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
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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

  suite("resolve", () => {
    [TestConfigs.redshift, TestConfigs.redshiftWithPrefix, TestConfigs.redshiftWithSuffix].forEach(
      testConfig => {
        test(`resolve with prefix "${testConfig.tablePrefix}" and suffix "${testConfig.schemaSuffix}"`, () => {
          const session = new Session(path.dirname(__filename), testConfig);
          session.compile();
          const suffix = testConfig.schemaSuffix ? `_${testConfig.schemaSuffix}` : "";
          const prefix = testConfig.tablePrefix ? `${testConfig.tablePrefix}_` : "";

          const resolvedRef = session.resolve("e");
          expect(resolvedRef).to.equal(`"schema${suffix}"."${prefix}e"`);
        });
      }
    );

    test("throws error for unknown action with .sql file compilation unsupported", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery, undefined, false);
      const graph = session.compile();
      expect(session.resolve("whatever")).to.equal("");
      expect(graph.graphErrors.compilationErrors[0].message).deep.equals(
        'Could not resolve "whatever"'
      );
    });

    test("throws error for unknown action in unknown schema with .sql file compilation unsupported", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery, undefined, false);
      const graph = session.compile();
      expect(session.resolve("unknown_schema", "whatever")).to.equal("");
      expect(graph.graphErrors.compilationErrors[0].message).deep.equals(
        'Could not resolve {"schema":"unknown_schema","name":"whatever"}'
      );
    });
  });

  suite("operate", () => {
    test("ref", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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
      expect(graph.operations[1].queries).deep.equals(['select * from "schema"."operate-1"']);
    });

    [
      { testConfig: TestConfigs.redshift, finalizedSchema: "schema" },
      { testConfig: TestConfigs.redshiftWithSuffix, finalizedSchema: "schema_suffix" }
    ].forEach(({ testConfig, finalizedSchema }) => {
      test(`schema with suffix: "${finalizedSchema}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.operate("operate-1", ctx => ctx.schema()).hasOutput(true);

        const graph = session.compile();

        expect(graph.operations[0].queries).deep.equals([finalizedSchema]);
      });
    });

    [
      { testConfig: TestConfigs.redshift, finalizedName: "operate-1" },
      { testConfig: TestConfigs.redshiftWithPrefix, finalizedName: "prefix_operate-1" }
    ].forEach(({ testConfig, finalizedName }) => {
      test(`name with prefix: "${finalizedName}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.operate("operate-1", ctx => ctx.name()).hasOutput(true);

        const graph = session.compile();

        expect(graph.operations[0].queries).deep.equals([finalizedName]);
      });
    });

    [
      { testConfig: TestConfigs.bigqueryWithDatabase, finalizedDatabase: "test-db" },
      { testConfig: TestConfigs.bigqueryWithDatabaseAndSuffix, finalizedDatabase: "test-db_suffix" }
    ].forEach(({ testConfig, finalizedDatabase }) => {
      test(`database with suffix: "${finalizedDatabase}"`, () => {
        const session = new Session(path.dirname(__filename), testConfig);
        session.operate("operate-1", ctx => ctx.database()).hasOutput(true);

        const graph = session.compile();

        expect(graph.operations[0].queries).deep.equals([finalizedDatabase]);
      });
    });

    test(`database fails when undefined`, () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);

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
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("a", ctx => `select * from ${ctx.ref("b")}`);
      const cGraph = session.compile();
      expect(
        cGraph.graphErrors.compilationErrors.filter(item =>
          item.message.match(/Missing dependency/)
        ).length
      ).greaterThan(0);
    });

    test("duplicate_action_names", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("b");
      session.publish("a", { schema: "schema1" }).dependencies("b");
      session.publish("a", { schema: "schema2" });
      const cGraph = session.compile();
      expect(cGraph.graphErrors.compilationErrors).deep.equals([]);
    });

    test("semi-colons at the end of files throw", () => {
      // If this didn't happen, then the generated SQL could be incorrect
      // because of being broken up by semi-colons.
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
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
        "A defaultLocation is required for BigQuery. This can be configured in dataform.json."
      ]);
    });

    test("variables defined in dataform.json must be strings", () => {
      const sessionFail = new Session(path.dirname(__filename), {
        warehouse: "bigquery",
        defaultSchema: "schema",
        defaultLocation: "location",
        vars: {
          int_var: 1,
          str_var: "str"
        }
      } as any);

      expect(() => {
        sessionFail.compile();
      }).to.throw("Custom variables defined in dataform.json can only be strings.");

      const sessionSuccess = new Session(path.dirname(__filename), {
        warehouse: "bigquery",
        defaultSchema: "schema",
        defaultLocation: "location",
        vars: {
          str_var1: "str1",
          str_var2: "str2"
        }
      } as any);

      const graph = sessionSuccess.compile();
      expect(graph.graphErrors.compilationErrors).to.eql([]);
    });
  });

  suite("compilers", () => {
    test("extract_blocks", () => {
      const TEST_SQL_FILE = `
        /*js
        var a = 1;
        */
        /*js
        var c = 3;
        */
        /*
        normal_multiline_comment
        */
        -- --js var x = 1200;
        --js var b = 2;
        -- normal_single_line_comment

        -- /*js
        -- var y = 234; // some js comment
        -- */

        select 1 as test from \`x\`
        `;
      const EXPECTED_JS = `var a = 1;\nvar c = 3;\nvar b = 2;`.trim();
      const EXPECTED_SQL = `
        /*
        normal_multiline_comment
        */
        -- --js var x = 1200;

        -- normal_single_line_comment

        -- /*js
        -- var y = 234; // some js comment
        -- */

        select 1 as test from \\\`x\\\``.trim();

      const { sql, js } = compilers.extractJsBlocks(TEST_SQL_FILE);
      expect(sql).equals(EXPECTED_SQL);
      expect(js).equals(EXPECTED_JS);
    });

    test("basic syntax", async () => {
      expect(
        compilers.compile(
          `
select * from \${ref('dab')}
`,
          "file.sqlx"
        )
      ).eql(await fs.readFile("tests/core/basic-syntax.js.test", "utf8"));
    });

    test("backticks are escaped", async () => {
      expect(
        compilers.compile(
          `
select
  "\`",
  """\`"",
from \`location\`
`,
          "file.sqlx"
        )
      ).eql(await fs.readFile("tests/core/backticks-are-escaped.js.test", "utf8"));
    });

    test("backslashes act literally", async () => {
      expect(
        compilers.compile(
          `
select
  regexp_extract('01a_data_engine', '^(\\d{2}\\w)'),
  regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)'),
  regexp_extract('\\\\', ''),
  regexp_extract("", r"[0-9]\\"*"),
  """\\ \\? \\\\""",
pre_operations {
  select
    regexp_extract('01a_data_engine', '^(\\d{2}\\w)'),
    regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)'),
    regexp_extract('\\\\', ''),
    regexp_extract("", r"[0-9]\\"*"),
}
`,
          "file.sqlx"
        )
      ).eql(await fs.readFile("tests/core/backslashes-act-literally.js.test", "utf8"));
    });
    test("strings act literally", async () => {
      expect(
        compilers.compile(
          `
select
  """
  triple
  quotes
  """,
  "asd\\"123'def",
  'asd\\'123"def',

post_operations {
  select
    """
    triple
    quotes
    """,
    "asd\\"123'def",
    'asd\\'123"def',
}
`,
          "file.sqlx"
        )
      ).eql(await fs.readFile("tests/core/strings-act-literally.js.test", "utf8"));
    });
    test("JS placeholders inside SQL strings", async () => {
      expect(
        compilers.compile(
          `
select '\${\`bar\`}'
`,
          "file.sqlx"
        )
      ).eql(
        await fs.readFile("tests/core/js-placeholder-strings-inside-sql-strings.js.test", "utf8")
      );
    });
  });

  suite("assert", () => {
    [
      { testConfig: TestConfigs.redshift, assertion: "schema" },
      { testConfig: TestConfigs.redshiftWithSuffix, assertion: "schema_suffix" }
    ].forEach(({ testConfig, assertion }) => {
      test(`schema: ${assertion}`, () => {
        const session = new Session(path.dirname(__filename), testConfig);

        session.assert("schema-assertion", ctx => ctx.schema());

        const graph = session.compile();

        expect(JSON.stringify(graph.assertions[0].query)).to.deep.equal(`"${assertion}"`);
      });
    });

    [
      { testConfig: TestConfigs.redshift, finalizedName: "name" },
      { testConfig: TestConfigs.redshiftWithPrefix, finalizedName: "prefix_name" }
    ].forEach(({ testConfig, finalizedName }) => {
      test(`name: ${finalizedName}`, () => {
        const session = new Session(path.dirname(__filename), testConfig);

        session.assert("name", ctx => ctx.name());

        const graph = session.compile();

        expect(JSON.stringify(graph.assertions[0].query)).to.deep.equal(`"${finalizedName}"`);
      });
    });

    [
      { testConfig: TestConfigs.bigqueryWithDatabase, finalizedDatabase: "test-db" },
      { testConfig: TestConfigs.bigqueryWithDatabaseAndSuffix, finalizedDatabase: "test-db_suffix" }
    ].forEach(({ testConfig, finalizedDatabase }) => {
      test(`database: ${finalizedDatabase}`, () => {
        const session = new Session(path.dirname(__filename), {
          ...testConfig,
          defaultDatabase: "test-db"
        });

        session.assert("database", ctx => ctx.database());

        const graph = session.compile();

        expect(JSON.stringify(graph.assertions[0].query)).to.deep.equal(`"${finalizedDatabase}"`);
      });
    });

    test(`database fails when undefined`, () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);

      session.assert("database", ctx => ctx.database());

      const graph = session.compile();

      expect(graph.graphErrors.compilationErrors[0].message).deep.equals(
        "Warehouse does not support multiple databases"
      );
      expect(JSON.stringify(graph.assertions[0].query)).to.deep.equal('""');
    });
  });
});
