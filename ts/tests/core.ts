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

    it("should_only_use_predefined_types", function() {
      const dfSuccess = new Session(path.dirname(__filename), TEST_CONFIG);
      dfSuccess.materialize("exampleSuccess1", { type: "table" });
      dfSuccess.materialize("exampleSuccess2", { type: "view" });
      dfSuccess.materialize("exampleSuccess3", { type: "incremental" });
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
        select 1 as test
        `;
      const EXPECTED_JS = `var a = 1;\nvar b = 2;`.trim();

      const EXPECTED_SQL = `
        /*
        normal_multiline_comment
        */
        -- normal_single_line_comment
        select 1 as test`.trim();

      var { sql, js } = compilers.extractJsBlocks(TEST_SQL_FILE);
      expect(sql).equals(EXPECTED_SQL);
      expect(js).equals(EXPECTED_JS);
    });
  });
});
