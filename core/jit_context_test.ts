import { expect } from "chai";

import { IncrementalTableJitContext, SqlActionJitContext, TableJitContext } from "df/core/jit_context";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("jit_context", () => {
  suite("SqlActionJitContext", () => {
    const adapter = {} as dataform.DbAdapter;
    const request = dataform.JitCompilationRequest.create({
      target: dataform.Target.create({
        database: "db",
        schema: "schema",
        name: "name"
      }),
      dependencies: [
        dataform.Target.create({
          database: "db",
          schema: "schema",
          name: "dep"
        })
      ],
      filePaths: [],
    });
    const withoutDependenciesRequest = dataform.JitCompilationRequest.create({
      target: dataform.Target.create({
        database: "db",
        schema: "schema",
        name: "name"
      }),
      dependencies: [],
      filePaths: [],
    });

    test("resolve by name only", () => {
      const context = new SqlActionJitContext(adapter, request);
      expect(context.resolve("dep")).to.equal("`db.schema.dep`");
    });

    test("resolve by schema and name", () => {
      const context = new SqlActionJitContext(adapter, request);
      expect(context.resolve(["schema", "dep"])).to.equal("`db.schema.dep`");
    });

    test("resolve by database, schema and name", () => {
      const context = new SqlActionJitContext(adapter, request);
      expect(context.resolve(["db", "schema", "dep"])).to.equal("`db.schema.dep`");
    });

    test("resolve throws for undeclared dependency", () => {
      const context = new SqlActionJitContext(adapter, withoutDependenciesRequest);
      expect(() => context.resolve("dep")).to.throw("Undeclared dependency referenced: dep");
    });

    test("ref", () => {
      const context = new SqlActionJitContext(adapter, request);
      expect(context.ref("dep")).to.equal("`db.schema.dep`");
    });

    test("self", () => {
      const context = new SqlActionJitContext(adapter, withoutDependenciesRequest);
      expect(context.self()).to.equal("`db.schema.name`");
    });

    test("name", () => {
      const context = new SqlActionJitContext(adapter, withoutDependenciesRequest);
      expect(context.name()).to.equal("name");
    });

    test("schema", () => {
      const context = new SqlActionJitContext(adapter, withoutDependenciesRequest);
      expect(context.schema()).to.equal("schema");
    });

    test("database", () => {
      const context = new SqlActionJitContext(adapter, withoutDependenciesRequest);
      expect(context.database()).to.equal("db");
    });
  });

  suite("TableJitContext", () => {
    const adapter = {} as dataform.DbAdapter;
    const request = dataform.JitCompilationRequest.create({
      target: dataform.Target.create({
        database: "db",
        schema: "schema",
        name: "name"
      }),
    });

    test("when", () => {
      const context = new TableJitContext(adapter, request);
      expect(context.when(true, "true", "false")).to.equal("true");
      expect(context.when(false, "true", "false")).to.equal("false");
      expect(context.when(false, "true")).to.equal("");
    });

    test("incremental", () => {
      const context = new TableJitContext(adapter, request);
      expect(context.incremental()).to.equal(false);
    });
  });

  suite("IncrementalTableJitContext", () => {
    const adapter = {} as dataform.DbAdapter;
    const request = dataform.JitCompilationRequest.create({
      target: dataform.Target.create({
        database: "db",
        schema: "schema",
        name: "name"
      }),
    });

    test("incremental", () => {
      let context = new IncrementalTableJitContext(adapter, request, true);
      expect(context.incremental()).to.equal(true);

      context = new IncrementalTableJitContext(adapter, request, false);
      expect(context.incremental()).to.equal(false);
    });
  });
});
