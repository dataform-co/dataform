import { expect } from "chai";

import { Dataform } from "@dataform/core";
import * as protos from "@dataform/protos";
import * as path from "path";

Dataform.ROOT_DIR = path.dirname(__filename);

const TEST_CONFIG: protos.IProjectConfig = {
  warehouse: "redshift",
  defaultSchema: "schema"
};

describe("@dataform/core", () => {
  describe("materialize", () => {
    it("config", function() {
      var df = new Dataform(TEST_CONFIG);
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
      var df = new Dataform(TEST_CONFIG);
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
      const dfSuccess = new Dataform(TEST_CONFIG);
      dfSuccess.materialize("exampleSuccess1", { type: "table" });
      dfSuccess.materialize("exampleSuccess2", { type: "view" });
      dfSuccess.materialize("exampleSuccess3", { type: "incremental" });
      const cgSuccess = dfSuccess.compile();

      cgSuccess.materializations.forEach(item => {
        expect(item)
          .to.have.property("validationErrors")
          .to.be.an("array").that.is.empty;
      });

      const dfFail = new Dataform(TEST_CONFIG);
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
      var df = new Dataform(TEST_CONFIG);
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
      const df = new Dataform(TEST_CONFIG);
      df.materialize("a").dependencies("b");
      const cGraph = df.compile();

      expect(cGraph)
        .to.have.property("validationErrors")
        .to.be.an("array");
      const errors = cGraph.validationErrors.filter(item => item.message.match(/Missing dependency/));
      expect(errors).to.be.an("array").that.is.not.empty;
    });

    it("duplicate_node_names", () => {
      const df = new Dataform(TEST_CONFIG);
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
});
