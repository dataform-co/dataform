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
      dfSuccess.materialize("example1", { type: "table" });
      dfSuccess.materialize("example2", { type: "view" });
      dfSuccess.materialize("example3", { type: "incremental" });
      expect(() => dfSuccess.compile()).to.not.throw();

      expect(() => {
        const dfFail = new Dataform(TEST_CONFIG);
        dfFail.materialize("example", JSON.parse('{"type": "ta ble"}')).compile();
      }).throws(Error, /Wrong type of materialization/);
    });
  });

  describe("graph", () => {
    it("circular_dependencies", () => {
      var df = new Dataform(TEST_CONFIG);
      df.materialize("a").dependencies("b");
      df.materialize("b").dependencies("a");
      expect(() => df.compile()).throws(Error, /Circular dependency/);
    });

    it("missing_dependency", () => {
      var df = new Dataform(TEST_CONFIG);
      df.materialize("a").dependencies("b");
      expect(() => df.compile()).throws(Error, /Missing dependency/);
    });
  });
});
