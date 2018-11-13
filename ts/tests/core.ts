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

      expect(m.name).to.equal("example");
      expect(m.type).to.equal("table");
      expect(m.descriptor).to.deep.equal({
        test: "test description"
      });
      expect(m.preOps).to.deep.equal(["pre_op"]);
      expect(m.postOps).to.deep.equal(["post_op"]);
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

      expect(m.name).to.equal("example");
      expect(m.type).to.equal("table");
      expect(m.descriptor).to.deep.equal({
        test: "test description"
      });
      expect(m.preOps).to.deep.equal(["pre_op"]);
      expect(m.postOps).to.deep.equal(["post_op"]);
    });
  });

  describe("graph", () => {
    it("circular_dependencies", () => {
      var df = new Dataform(TEST_CONFIG);
      df.materialize("a").dependencies("b");
      df.materialize("b").dependencies("a");
      expect(() => df.compile()).to.throw(Error, /Circular dependency/);
    });

    it("missing_dependency", () => {
      var df = new Dataform(TEST_CONFIG);
      df.materialize("a").dependencies("b");
      expect(() => df.compile()).to.throw(Error, /Missing dependency/);
    });
  });
});
