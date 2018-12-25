import { expect } from "chai";

import { Session } from "@dataform/core";
import * as compilers from "@dataform/core/compilers";
import * as protos from "@dataform/protos";
import * as path from "path";

const TEST_CONFIG: protos.IProjectConfig = {
  warehouse: "redshift",
  defaultSchema: "schema"
};

describe("@dataform/core", () => {
  describe("materialize", () => {
    it("config", function() {
      var df = new Session(path.dirname(__filename), TEST_CONFIG);
      var m = df
        .materialize("example", {
          type: "table",
          query: _ => "select 1 as test",
          dependencies: [],
          descriptor: {
            test: "test description"
          },
          preOps: _ => ["pre_op"],
          postOps: _ => ["post_op"]
        })
        .compile();

      expect(m.name).equals("example");
      expect(m.type).equals("table");
      expect(m.descriptor).deep.equals({
        test: "test description"
      });
      expect(m.preOps).deep.equals(["pre_op"]);
      expect(m.postOps).deep.equals(["post_op"]);
    });

    it("config_context", function() {
      var df = new Session(path.dirname(__filename), TEST_CONFIG);
      var m = df
        .materialize(
          "example",
          ctx => `
          ${ctx.type("table")}
          ${ctx.descriptor({
            test: "test description"
          })}
          ${ctx.preOps(["pre_op"])}
          ${ctx.postOps(["post_op"])}
        `
        )
        .compile();

      expect(m.name).equals("example");
      expect(m.type).equals("table");
      expect(m.descriptor).deep.equals({
        test: "test description"
      });
      expect(m.preOps).deep.equals(["pre_op"]);
      expect(m.postOps).deep.equals(["post_op"]);
    });

    it("validation_type_incremental", function() {
      const sessionSuccess = new Session(path.dirname(__filename), TEST_CONFIG);
      sessionSuccess.materialize("exampleSuccess1", { type: "incremental", where: "test1" });
      sessionSuccess.materialize(
        "exampleSuccess2",
        ctx => `
        ${ctx.where("test2")}
        ${ctx.type("incremental")}
      `
      );
      const cgSuccess = sessionSuccess.compile();

      cgSuccess.materializations.forEach(item => {
        expect(item)
          .to.have.property("validationErrors")
          .to.be.an("array").that.is.empty;
      });

      const sessionFail = new Session(path.dirname(__filename), TEST_CONFIG);
      sessionFail.materialize("exampleFail1", { type: "incremental" });
      sessionFail.materialize("exampleFail2", { type: "incremental", where: "" });
      sessionFail.materialize("exampleFail3", ctx => `${ctx.type("incremental")}`);
      const cgFail = sessionFail.compile();

      cgFail.materializations.forEach(item => {
        expect(item)
          .to.have.property("validationErrors")
          .to.be.an("array");

        const errors = item.validationErrors.filter(item => item.message.match(/"where" property is not defined/));
        expect(errors).to.be.an("array").that.is.not.empty;
      });
    });

    it("validation_type", function() {
      const dfSuccess = new Session(path.dirname(__filename), TEST_CONFIG);
      dfSuccess.materialize("exampleSuccess1", { type: "table" });
      dfSuccess.materialize("exampleSuccess2", { type: "view" });
      dfSuccess.materialize("exampleSuccess3", { type: "incremental", where: "test" });
      const cgSuccess = dfSuccess.compile();

      cgSuccess.materializations.forEach(item => {
        expect(item)
          .to.have.property("validationErrors")
          .to.be.an("array").that.is.empty;
      });

      const dfFail = new Session(path.dirname(__filename), TEST_CONFIG);
      const mFail = dfFail.materialize("exampleFail", JSON.parse('{"type": "ta ble"}')).compile();
      expect(mFail)
        .to.have.property("validationErrors")
        .to.be.an("array");

      const errors = mFail.validationErrors.filter(item => item.message.match(/Wrong type of materialization/));
      expect(errors).to.be.an("array").that.is.not.empty;
    });

    it("validation_redshift_success", function() {
      const session = new Session(path.dirname(__filename), TEST_CONFIG);
      session.materialize("example_without_dist", {
        redshift: {
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        }
      });
      session.materialize("example_without_sort", {
        redshift: {
          distKey: "column1",
          distStyle: "even"
        }
      });

      const graph = session.compile();

      expect(graph)
        .to.have.property("materializations")
        .to.be.an("array")
        .to.have.lengthOf(2);

      graph.materializations.forEach((item, index) => {
        expect(item)
          .to.have.property("validationErrors")
          .to.be.an("array").that.is.empty;
      });
    });

    it("validation_redshift_fail", function() {
      const session = new Session(path.dirname(__filename), TEST_CONFIG);
      session.materialize("example_absent_distKey", {
        redshift: {
          distStyle: "even",
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        }
      });
      session.materialize("example_absent_distStyle", {
        redshift: {
          distKey: "column1",
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        }
      });
      session.materialize("example_wrong_distStyle", {
        redshift: {
          distKey: "column1",
          distStyle: "wrong_even",
          sortKeys: ["column1", "column2"],
          sortStyle: "compound"
        }
      });
      session.materialize("example_absent_sortKeys", {
        redshift: {
          distKey: "column1",
          distStyle: "even",
          sortStyle: "compound"
        }
      });
      session.materialize("example_empty_sortKeys", {
        redshift: {
          distKey: "column1",
          distStyle: "even",
          sortKeys: [],
          sortStyle: "compound"
        }
      });
      session.materialize("example_absent_sortStyle", {
        redshift: {
          distKey: "column1",
          distStyle: "even",
          sortKeys: ["column1", "column2"]
        }
      });
      session.materialize("example_wrong_sortStyle", {
        redshift: {
          distKey: "column1",
          distStyle: "even",
          sortKeys: ["column1", "column2"],
          sortStyle: "wrong_sortStyle"
        }
      });
      session.materialize("example_empty_redshift", {
        redshift: {}
      });

      const graph = session.compile();
      const expectedMessages = [
        /Property "distKey" is not defined/,
        /Property "distStyle" is not defined/,
        /Wrong value of "distStyle" property/,
        /Property "sortKeys" is not defined/,
        /Property "sortKeys" is not defined/,
        /Property "sortStyle" is not defined/,
        /Wrong value of "sortStyle" property/,
        /Missing properties in redshift config/
      ];

      expect(graph)
        .to.have.property("materializations")
        .to.be.an("array")
        .to.have.lengthOf(8);

      graph.materializations.forEach((item, index) => {
        expect(item)
          .to.have.property("validationErrors")
          .to.be.an("array");

        const errors = item.validationErrors.filter(item => item.message.match(expectedMessages[index]));
        expect(errors).to.be.an("array").that.is.not.empty;
      });
    });
  });

  describe("operate", () => {
    it("ref", function() {
      const session = new Session(path.dirname(__filename), TEST_CONFIG);
      session.operate("operate-1", () => `select 1 as sample`).hasOutput(true);
      session.operate("operate-2", ctx => `select * from ${ctx.ref("operate-1")}`).hasOutput(true);

      const graph = session.compile();

      expect(graph)
        .to.have.property("compileErrors")
        .to.be.an("array").that.is.empty;
      expect(graph)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;
      expect(graph)
        .to.have.property("operations")
        .to.be.an("array")
        .to.have.lengthOf(2);

      expect(graph.operations[0].name).equals("operate-1");
      expect(graph.operations[0].hasOutput).equals(true);
      expect(graph.operations[0].dependencies).to.be.an("array").that.is.empty;
      expect(graph.operations[0].queries).deep.equals(["select 1 as sample"]);

      expect(graph.operations[1].name).equals("operate-2");
      expect(graph.operations[1].hasOutput).equals(true);
      expect(graph.operations[1].dependencies).deep.equals(["operate-1"]);
      expect(graph.operations[1].queries).deep.equals(['select * from "schema"."operate-1"']);
    });
  });

  describe("graph", () => {
    it("circular_dependencies", () => {
      var df = new Session(path.dirname(__filename), TEST_CONFIG);
      df.materialize("a").dependencies("b");
      df.materialize("b").dependencies("a");
      const cGraph = df.compile();

      expect(cGraph)
        .to.have.property("validationErrors")
        .to.be.an("array");
      const errors = cGraph.validationErrors.filter(item => item.message.match(/Circular dependency/));
      expect(errors).to.be.an("array").that.is.not.empty;
    });

    it("missing_dependency", () => {
      const df = new Session(path.dirname(__filename), TEST_CONFIG);
      df.materialize("a").dependencies("b");
      const cGraph = df.compile();

      expect(cGraph)
        .to.have.property("validationErrors")
        .to.be.an("array");
      const errors = cGraph.validationErrors.filter(item => item.message.match(/Missing dependency/));
      expect(errors).to.be.an("array").that.is.not.empty;
    });

    it("duplicate_node_names", () => {
      const df = new Session(path.dirname(__filename), TEST_CONFIG);
      df.materialize("a").dependencies("b");
      df.materialize("b");
      df.materialize("a");
      const cGraph = df.compile();

      expect(cGraph)
        .to.have.property("validationErrors")
        .to.be.an("array");
      const errors = cGraph.validationErrors.filter(item => item.message.match(/Duplicate node name/));
      expect(errors).to.be.an("array").that.is.not.empty;
    });
  });

  describe("compilers", () => {
    it("extract_blocks", function() {
      const TEST_SQL_FILE = `
        /*js
        var a = 1;
        */
        --js var b = 2;
        /*
        normal_multiline_comment
        */
        -- normal_single_line_comment
        select 1 as test from \`x\`
        `;
      const EXPECTED_JS = `var a = 1;\nvar b = 2;`.trim();

      const EXPECTED_SQL = `
        /*
        normal_multiline_comment
        */
        -- normal_single_line_comment
        select 1 as test from \\\`x\\\``.trim();

      var { sql, js } = compilers.extractJsBlocks(TEST_SQL_FILE);
      expect(sql).equals(EXPECTED_SQL);
      expect(js).equals(EXPECTED_JS);
    });
  });
});
