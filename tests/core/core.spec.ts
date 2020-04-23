import * as compilers from "@dataform/core/compilers";
import { Session } from "@dataform/core/session";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { suite, test } from "@dataform/testing";
import { fail } from "assert";
import { expect } from "chai";
import { asPlainObject } from "df/tests/utils";
import * as fs from "fs-extra";
import * as path from "path";

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
          table => table.name === `schema.${tableWithPrefix("example")}`
        );
        expect(t.type).equals("table");
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
          table => table.name === `schema2.${tableWithPrefix("example")}`
        );
        expect(t2.type).equals("table");
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
        expect(t2.dependencies).includes(`schema.${tableWithPrefix("example")}`);

        const t3 = compiledGraph.tables.find(
          table => table.name === `test_schema.${tableWithPrefix("my_table")}`
        );
        expect((t3.target.name = `${tableWithPrefix("my_table")}`));
        expect((t3.target.schema = "test_schema"));
        expect(t3.type).equals("table");
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
          table => table.name === `${schemaWithSuffix("schema")}.example`
        );
        expect(t.type).equals("table");
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
          table => table.name === `${schemaWithSuffix("schema2")}.example`
        );
        expect(t2.type).equals("table");
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
        expect(t2.dependencies).includes(`${schemaWithSuffix("schema")}.example`);

        const t3 = compiledGraph.tables.find(
          table => table.name === `${schemaWithSuffix("test_schema")}.my_table`
        );
        expect((t3.target.name = "my_table"));
        expect((t3.target.schema = schemaWithSuffix("test_schema")));
        expect(t3.type).equals("table");
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
          query: "select false as incremental",
          incrementalQuery: "select true as incremental",
          disabled: false,
          fileName: path.basename(__filename),
          name: "schema.incremental",
          type: "incremental"
        }
      ]);
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
      const cgSuccessErrors = utils.validate(cgSuccess);

      expect(cgSuccessErrors.validationErrors).deep.equals([]);
    });

    test("validation_type", () => {
      const sessionSuccess = new Session(path.dirname(__filename), TestConfigs.redshift);
      sessionSuccess.publish("exampleSuccess1", { type: "table" });
      sessionSuccess.publish("exampleSuccess2", { type: "view" });
      sessionSuccess.publish("exampleSuccess3", { type: "incremental" }).where("test");
      const cgSuccess = sessionSuccess.compile();
      const cgSuccessErrors = utils.validate(cgSuccess);

      expect(cgSuccessErrors.validationErrors).deep.equals([]);

      const sessionFail = new Session(path.dirname(__filename), TestConfigs.redshift);
      sessionFail.publish("exampleFail", JSON.parse('{"type": "ta ble"}'));
      const cgFail = sessionFail.compile();
      const cgFailErrors = utils.validate(cgFail);

      expect(cgFailErrors.validationErrors).deep.equals([
        dataform.ValidationError.create({
          actionName: "schema.exampleFail",
          message:
            'Wrong type of table detected. Should only use predefined types: "table" | "view" | "incremental" | "inline"'
        })
      ]);

      const err = cgFailErrors.validationErrors.find(e => e.actionName === "schema.exampleFail");
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
      const gErrors = utils.validate(graph);

      expect(graph)
        .to.have.property("tables")
        .to.be.an("array")
        .to.have.lengthOf(2);

      expect(gErrors.validationErrors).deep.equals([]);
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
        }
      ];

      const graph = session.compile();
      const gErrors = utils.validate(graph);

      expect(
        gErrors.validationErrors.map(validationError => ({
          name: validationError.actionName,
          message: validationError.message
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
      session.publish("example_clusterBy_but_no_partitionBy_fail", {
        type: "table",
        bigquery: {
          clusterBy: ["some_column", "some_other_column"]
        }
      });

      const graph = session.compile();
      const gErrors = utils.validate(graph);

      expect(
        gErrors.validationErrors
          .filter(item => item.actionName === "schema.example_partitionBy_view_fail")
          .map(item => item.message)
      ).to.deep.equals([
        `partitionBy/clusterBy are not valid for BigQuery views; they are only valid for tables`
      ]);

      expect(
        gErrors.validationErrors
          .filter(item => item.actionName === "schema.example_clusterBy_but_no_partitionBy_fail")
          .map(item => item.message)
      ).to.deep.equals([`clusterBy is not valid without partitionBy`]);
    });

    test("validation_bigquery_pass", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
      session.publish("example_partitionBy_view_fail", {
        type: "table",
        bigquery: {
          partitionBy: "some_partition",
          clusterBy: ["some_column", "some_other_column"]
        }
      });

      const graph = session.compile();
      const gValid = utils.validate(graph);

      expect(graph.tables[0].bigquery).to.deep.equals(
        dataform.BigQueryOptions.create({
          clusterBy: ["some_column", "some_other_column"],
          partitionBy: "some_partition"
        })
      );
      expect(gValid).to.deep.equals(
        dataform.GraphErrors.create({
          compilationErrors: [],
          validationErrors: []
        })
      );
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
      const graphErrors = utils.validate(graph);

      const tableA = graph.tables.find(item => item.name === "schema.a");
      expect(tableA.type).equals("table");
      expect(tableA.dependencies).deep.equals([]);
      expect(tableA.query).equals("select 1 as test");

      const tableB = graph.tables.find(item => item.name === "schema.b");
      expect(tableB.type).equals("inline");
      expect(tableB.dependencies).includes("schema.a");
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

      const tableC = graph.tables.find(item => item.name === "schema.c");
      expect(tableC.type).equals("table");
      expect(tableC.dependencies).includes("schema.a");
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

      const errors = graphErrors.validationErrors
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
            dimension_column: {
              displayName: "Dimension",
              dimension: "timestamp"
            },
            aggregator_column: {
              description: "Aggregator description",
              displayName: "Aggregator",
              aggregator: "distinct"
            }
          }
        })
        .query(_ => "select 1 as test");

      const graph = session.compile();
      const graphErrors = utils.validate(graph);
      expect(graphErrors.compilationErrors).deep.equals([]);
      expect(graphErrors.validationErrors).deep.equals([]);

      const schema = graph.tables.find(table => table.name === "schema.a");

      const dimensionColumn = schema.actionDescriptor.columns.find(
        column => column.displayName === "Dimension"
      );
      expect(dimensionColumn).to.eql(
        dataform.ColumnDescriptor.create({
          dimensionType: dataform.ColumnDescriptor.DimensionType.TIMESTAMP,
          displayName: "Dimension",
          path: ["dimension_column"]
        })
      );

      const aggregationColumn = schema.actionDescriptor.columns.find(
        column => column.displayName === "Aggregator"
      );
      expect(aggregationColumn).to.eql(
        dataform.ColumnDescriptor.create({
          description: "Aggregator description",
          aggregation: dataform.ColumnDescriptor.Aggregation.DISTINCT,
          displayName: "Aggregator",
          path: ["aggregator_column"]
        })
      );
    });

    test("does not allow extra column descriptors", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      try {
        session
          .publish("a", {
            type: "table",
            columns: {
              dimension_column: {
                unknownProperty: "unknown"
              } as any
            }
          })
          .query(_ => "select 1 as test");
      } catch (e) {
        return;
      }
      fail();
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
          const graphErrors = utils.validate(graph);

          const tableNames = graph.tables.map(item => item.name);

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
            graph.tables.find(table => table.name === `schema${suffix}.${prefix}b`).dependencies
          ).eql([`schema${suffix}.${prefix}a`]);
          expect(
            graph.tables.find(table => table.name === `schema${suffix}.${prefix}d`).dependencies
          ).eql([`schema${suffix}.${prefix}a`]);
          expect(
            graph.tables.find(table => table.name === `schema${suffix}.${prefix}g`).dependencies
          ).eql([`schema${suffix}.${prefix}a`]);
          expect(
            graph.tables.find(table => table.name === `schema${suffix}.${prefix}h`).dependencies
          ).eql([`schema${suffix}.${prefix}a`]);
          expect(
            graph.tables.find(table => table.name === `schema${suffix}.${prefix}f`).dependencies
          ).eql([`foo${suffix}.${prefix}e`]);

          const errors = graphErrors.compilationErrors.map(item => item.message);
          expect(errors).includes("Action name is not specified");
          expect(graphErrors.compilationErrors.length).eql(1);
          expect(graphErrors.validationErrors.length).eql(0);
        });
      }
    );
  });

  suite("resolve", () => {
    [TestConfigs.redshift, TestConfigs.redshiftWithPrefix, TestConfigs.redshiftWithSuffix].forEach(
      testConfig => {
        test(`resolve with prefix "${testConfig.tablePrefix}" and suffix "${testConfig.schemaSuffix}"`, () => {
          const session = new Session(path.dirname(__filename), testConfig);
          const suffix = testConfig.schemaSuffix ? `_${testConfig.schemaSuffix}` : "";
          const prefix = testConfig.tablePrefix ? `${testConfig.tablePrefix}_` : "";

          const resolvedRef = session.resolve("e");
          expect(resolvedRef).to.equal(`"schema${suffix}"."${prefix}e"`);
        });
      }
    );
  });

  suite("operate", () => {
    test("ref", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.operate("operate-1", () => `select 1 as sample`).hasOutput(true);
      session.operate("operate-2", ctx => `select * from ${ctx.ref("operate-1")}`).hasOutput(true);

      const graph = session.compile();
      const gErrors = utils.validate(graph);

      expect(gErrors.compilationErrors).deep.equals([]);
      expect(gErrors.validationErrors).deep.equals([]);
      expect(graph)
        .to.have.property("operations")
        .to.be.an("array")
        .to.have.lengthOf(2);

      expect(graph.operations[0].name).equals("schema.operate-1");
      expect(graph.operations[0].dependencies).deep.equals([]);
      expect(graph.operations[0].queries).deep.equals(["select 1 as sample"]);

      expect(graph.operations[1].name).equals("schema.operate-2");
      expect(graph.operations[1].dependencies).deep.equals(["schema.operate-1"]);
      expect(graph.operations[1].queries).deep.equals(['select * from "schema"."operate-1"']);
    });
  });

  suite("graph", () => {
    test("circular_dependencies", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("a").dependencies("b");
      session.publish("b").dependencies("a");
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(
        gErrors.compilationErrors.filter(item => item.message.match(/Circular dependency/)).length
      ).greaterThan(0);
    });

    test("missing_dependency", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("a", ctx => `select * from ${ctx.ref("b")}`);
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(
        gErrors.compilationErrors.filter(item => item.message.match(/Missing dependency/)).length
      ).greaterThan(0);
    });

    test("duplicate_action_names", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("a").dependencies("b");
      session.publish("b");
      session.publish("a");
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);

      expect(
        gErrors.compilationErrors.filter(item => item.message.match(/Duplicate action name/)).length
      ).greaterThan(0);
    });

    test("same action names in different schemas (ambiguity)", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("a", { schema: "foo" });
      session.publish("a", { schema: "bar" });
      session.publish("b", { schema: "foo" }).dependencies("a");
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(
        gErrors.compilationErrors.filter(item =>
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
      const gErrors = utils.validate(cGraph);
      expect(
        gErrors.compilationErrors.filter(item =>
          item.message.match(/Duplicate action name detected. Names within a schema must be unique/)
        ).length
      ).greaterThan(0);
    });

    test("same action names in different schemas", () => {
      const session = new Session(path.dirname(__filename), TestConfigs.redshift);
      session.publish("b");
      session.publish("a", { schema: "schema1" }).dependencies("b");
      session.publish("a", { schema: "schema2" });
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(gErrors.compilationErrors).deep.equals([]);
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
    test("backslashes act literally", async () => {
      expect(
        compilers.compile(
          `
select
  regexp_extract('01a_data_engine', '^(\\d{2}\\w)'),
  regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)'),
  regexp_extract('\\\\', ''),
  regexp_extract("", r"[0-9]\\"*"),

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
  "asd\\"123'def",
  'asd\\'123"def',

post_operations {
  select
    "asd\\"123'def",
    'asd\\'123"def',
}
`,
          "file.sqlx"
        )
      ).eql(await fs.readFile("tests/core/strings-act-literally.js.test", "utf8"));
    });
  });
});
