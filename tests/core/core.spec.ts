import * as compilers from "@dataform/core/compilers";
import { Session } from "@dataform/core/session";
import { Table } from "@dataform/core/table";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { expect } from "chai";
import { asPlainObject } from "df/tests/utils";
import * as path from "path";

class TEST_CONFIG {
  public static REDSHIFT: dataform.IProjectConfig = {
    warehouse: "redshift",
    defaultSchema: "schema"
  };

  public static REDSHIFT_WITH_SUFFIX: dataform.IProjectConfig = {
    warehouse: "redshift",
    defaultSchema: "schema",
    schemaSuffix: "suffix"
  };

  public static BIGQUERY: dataform.IProjectConfig = {
    warehouse: "bigquery",
    defaultSchema: "schema"
  };
}

describe("@dataform/core", () => {
  describe("publish", () => {
    [TEST_CONFIG.REDSHIFT, TEST_CONFIG.REDSHIFT_WITH_SUFFIX].forEach(testConfig => {
      it(`config with suffix "${testConfig.schemaSuffix}"`, () => {
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

    it("incremental table", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session
        .publish("incremental", {
          type: "incremental"
        })
        .query(ctx => `select ${ctx.isIncremental()} as incremental`);
      const graph = session.compile();

      expect(graph.toJSON().tables).deep.equals([
        {
          target: {
            name: "incremental",
            schema: TEST_CONFIG.REDSHIFT.defaultSchema
          },
          query: "select false as incremental",
          incrementalQuery: "select true as incremental",
          disabled: false,
          fileName: "",
          name: "schema.incremental",
          type: "incremental"
        }
      ]);
    });

    it("config_context", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      const t = session
        .publish(
          "example",
          ctx => `
          ${ctx.type("table")}
          ${ctx.preOps(["pre_op"])}
          ${ctx.postOps(["post_op"])}
        `
        )
        .compile();

      expect(t.name).equals("schema.example");
      expect(t.type).equals("table");
      expect(t.preOps).deep.equals(["pre_op"]);
      expect(t.postOps).deep.equals(["post_op"]);
    });

    it("validation_type_incremental", () => {
      const sessionSuccess = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      sessionSuccess
        .publish("exampleSuccess1", {
          type: "incremental"
        })
        .where("test1");
      sessionSuccess.publish(
        "exampleSuccess2",
        ctx => `
        ${ctx.where("test2")}
        ${ctx.type("incremental")}
        select field as 1
      `
      );
      sessionSuccess.publish(
        "exampleSuccess3",
        ctx => `
        ${ctx.type("incremental")}
        ${ctx.where("test2")}
        select field as 1
      `
      );
      const cgSuccess = sessionSuccess.compile();
      const cgSuccessErrors = utils.validate(cgSuccess);

      expect(cgSuccessErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;
    });

    it("validation_type", () => {
      const sessionSuccess = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      sessionSuccess.publish("exampleSuccess1", { type: "table" });
      sessionSuccess.publish("exampleSuccess2", { type: "view" });
      sessionSuccess.publish("exampleSuccess3", { type: "incremental" }).where("test");
      const cgSuccess = sessionSuccess.compile();
      const cgSuccessErrors = utils.validate(cgSuccess);

      expect(cgSuccessErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;

      const sessionFail = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      sessionFail.publish("exampleFail", JSON.parse('{"type": "ta ble"}'));
      const cgFail = sessionFail.compile();
      const cgFailErrors = utils.validate(cgFail);

      expect(cgFailErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.not.empty;

      const err = cgFailErrors.validationErrors.find(e => e.actionName === "schema.exampleFail");
      expect(err)
        .to.have.property("message")
        .that.matches(/Wrong type of table/);
    });

    it("validation_redshift_success", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
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

      expect(gErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;
    });

    it("validation_redshift_fail", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
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

    it("validation_bigquery_fail", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.BIGQUERY);
      session.publish("example_partitionBy_view_fail", {
        type: "view",
        bigquery: {
          partitionBy: "some_partition"
        }
      });

      const expectedResults = [
        {
          name: "schema.example_partitionBy_view_fail",
          message: `partitionBy is not valid for BigQuery views; it is only valid for tables`
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

    it("validation_type_inline", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
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
      expect(tableA).to.exist;
      expect(tableA.type).equals("table");
      expect(tableA.dependencies).to.be.an("array").that.is.empty;
      expect(tableA.query).equals("select 1 as test");

      const tableB = graph.tables.find(item => item.name === "schema.b");
      expect(tableB).to.exist;
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
      expect(tableB.disabled).to.be.true;
      expect(tableB.where).equals("test_where");
      expect(tableB.query).equals('select * from "schema"."a"');

      const tableC = graph.tables.find(item => item.name === "schema.c");
      expect(tableC).to.exist;
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
      expect(tableC.redshift).to.not.exist;
      expect(tableC.disabled).to.be.false;
      expect(tableC.where).equals("");
      expect(tableC.query).equals('select * from (select * from "schema"."a")');

      expect(graphErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.not.empty;

      const errors = graphErrors.validationErrors
        .filter(item => item.actionName === "schema.b")
        .map(item => item.message);

      expect(errors).that.matches(/Unused property was detected: "preOps"/);
      expect(errors).that.matches(/Unused property was detected: "postOps"/);
      expect(errors).that.matches(/Unused property was detected: "redshift"/);
      expect(errors).that.matches(/Unused property was detected: "disabled"/);
      expect(errors).that.matches(/Unused property was detected: "where"/);
    });

    it("ref", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session.publish("a", _ => "select 1 as test");
      session.publish("b", ctx => `select * from ${ctx.ref("a")}`);
      session.publish("c", ctx => `select * from ${ctx.ref(undefined)}`);
      session.publish("d", ctx => `select * from ${ctx.ref({ schema: "schema", name: "a" })}`);
      session
        .publish("e", {
          schema: "foo"
        })
        .query(_ => "select 1 as test");
      session.publish("f", ctx => `select * from ${ctx.ref("e")}`);

      const graph = session.compile();
      const graphErrors = utils.validate(graph);

      const tableNames = graph.tables.map(item => item.name);

      expect(tableNames).includes("schema.a");
      expect(tableNames).includes("schema.b");
      expect(tableNames).includes("schema.c");
      expect(tableNames).includes("schema.d");
      expect(tableNames).includes("foo.e");
      expect(tableNames).includes("schema.f");
      const errors = graphErrors.compilationErrors.map(item => item.message);
      expect(errors).includes("Action name is not specified");
      expect(graphErrors.compilationErrors.length === 1);
      expect(graphErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;
    });
  });

  describe("operate", () => {
    it("ref", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session.operate("operate-1", () => `select 1 as sample`).hasOutput(true);
      session.operate("operate-2", ctx => `select * from ${ctx.ref("operate-1")}`).hasOutput(true);

      const graph = session.compile();
      const gErrors = utils.validate(graph);

      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.empty;
      expect(gErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;
      expect(graph)
        .to.have.property("operations")
        .to.be.an("array")
        .to.have.lengthOf(2);

      expect(graph.operations[0].name).equals("schema.operate-1");
      expect(graph.operations[0].dependencies).to.be.an("array").that.is.empty;
      expect(graph.operations[0].queries).deep.equals(["select 1 as sample"]);

      expect(graph.operations[1].name).equals("schema.operate-2");
      expect(graph.operations[1].dependencies).deep.equals(["schema.operate-1"]);
      expect(graph.operations[1].queries).deep.equals(['select * from "schema"."operate-1"']);
    });
  });

  describe("graph", () => {
    it("circular_dependencies", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session.publish("a").dependencies("b");
      session.publish("b").dependencies("a");
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.not.empty;
      expect(
        gErrors.compilationErrors.filter(item => item.message.match(/Circular dependency/))
      ).to.be.an("array").that.is.not.empty;
    });

    it("missing_dependency", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session.publish("a", ctx => `select * from ${ctx.ref("b")}`);
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.not.empty;
      expect(
        gErrors.compilationErrors.filter(item => item.message.match(/Missing dependency/))
      ).to.be.an("array").that.is.not.empty;
    });

    it("duplicate_action_names", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session.publish("a").dependencies("b");
      session.publish("b");
      session.publish("a");
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);

      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.not.empty;

      const errors = gErrors.compilationErrors.filter(item =>
        item.message.match(/Duplicate action name/)
      );
      expect(errors).to.be.an("array").that.is.not.empty;
    });

    it("same action names in different schemas (ambiguity)", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session.publish("a", { schema: "foo" });
      session.publish("a", { schema: "bar" });
      session.publish("b", { schema: "foo" }).dependencies("a");
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.not.empty;
      const errors = gErrors.compilationErrors.filter(item =>
        item.message.match(/Ambiguous Action name: a. Did you mean one of: foo.a, bar.a./)
      );
      expect(errors).to.be.an("array").that.is.not.empty;
    });

    it("same action name in same schema", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session.publish("a", { schema: "schema2" }).dependencies("b");
      session.publish("a", { schema: "schema2" });
      session.publish("b");
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.not.empty;
      const errors = gErrors.compilationErrors.filter(item =>
        item.message.match(/Duplicate action name detected. Names within a schema must be unique/)
      );
      expect(errors).to.be.an("array").that.is.not.empty;
    });

    it("same action names in different schemas", () => {
      const session = new Session(path.dirname(__filename), TEST_CONFIG.REDSHIFT);
      session.publish("b");
      session.publish("a", { schema: "schema1" }).dependencies("b");
      session.publish("a", { schema: "schema2" });
      const cGraph = session.compile();
      const gErrors = utils.validate(cGraph);
      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.empty;
    });
  });

  describe("compilers", () => {
    it("extract_blocks", () => {
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
  });
});
