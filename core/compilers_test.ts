import { expect } from "chai";

import { compile } from "df/core/compilers";
import { suite, test } from "df/testing";

suite("core/compilers", () => {
  suite("compile", () => {
    test("compiles sqlx to js", () => {
      const code = `config { type: "table" } select 1`;
      const path = "definitions/foo.sqlx";
      const result = compile(code, path);
      expect(result).to.include("dataform.sqlxAction");
      expect(result).to.include('name: "foo"');
      expect(result).to.include('{ type: "table" }');
      // The sqlx compiler will format the query.
      expect(result).to.include("select 1");
    });

    test("escapes the action name derived from the file name", () => {
      const result = compile("select 1", 'definitions/a"b.sqlx');
      expect(result).to.include('name: "a\\"b"');
    });

    test("compiles yml to js", () => {
      const code = "foo: bar";
      const path = "definitions/foo.yml";
      const result = compile(code, path);
      expect(result).to.equal(`exports.asJson = {"foo":"bar"}`);
    });

    test("compiles yaml to js", () => {
      const code = `
- item1
- item2`;
      const path = "definitions/foo.yaml";
      const result = compile(code, path);
      expect(result).to.equal(`exports.asJson = ["item1","item2"]`);
    });

    test("throws error for invalid yaml", () => {
      const code = "foo: : bar";
      const path = "definitions/foo.yaml";
      expect(() => compile(code, path)).to.throw(
        `${path} is not a valid YAML file: YAMLException: bad indentation of a mapping entry (1:6)`,
      );
    });

    test("compiles ipynb to js", () => {
      const code = '{"cells": []}';
      const path = "definitions/foo.ipynb";
      const result = compile(code, path);
      expect(result).to.equal('exports.asJson = {"cells":[]}');
    });

    test("throws error for invalid ipynb json", () => {
      const code = '{"cells": [}';
      const path = "definitions/foo.ipynb";
      expect(() => compile(code, path)).to.throw(`Error parsing ${path} as JSON:`);
    });

    test("compiles sql to js", () => {
      const code = "select 1";
      const path = "definitions/foo.sql";
      const result = compile(code, path);
      expect(result).to.equal("exports.query = `select 1`;");
    });

    test("escapes backticks in sql", () => {
      const code = "select `a` from `b`";
      const path = "definitions/foo.sql";
      const result = compile(code, path);
      expect(result).to.equal("exports.query = `select \\`a\\` from \\`b\\``;");
    });

    test("escapes backslashes in sql", () => {
      const code = "select ''";
      const path = "definitions/foo.sql";
      const result = compile(code, path);
      expect(result).to.equal("exports.query = `select ''`;");
    });

    test("escapes template literals in sql", () => {
      const code = "select ${foo}";
      const path = "definitions/foo.sql";
      const result = compile(code, path);
      expect(result).to.equal("exports.query = `select \\${foo}`;");
    });

    test("returns raw code for other file types", () => {
      const code = "const a = 1;";
      const path = "definitions/foo.js";
      const result = compile(code, path);
      expect(result).to.equal(code);
    });

    test("returns raw code for files with no extension", () => {
      const code = "const a = 1;";
      const path = "definitions/foo";
      const result = compile(code, path);
      expect(result).to.equal(code);
    });

    test("compiles md to js contents export", () => {
      const code = "# this table defines";
      const path = "definitions/foo.md";
      const result = compile(code, path);
      expect(result).to.equal("exports.contents = `# this table defines`;");
    });

    test("escapes backslashes in md", () => {
      const code = "select ''";
      const path = "definitions/foo.md";
      const result = compile(code, path);
      expect(result).to.equal("exports.contents = `select ''`;");
    });
    test("escapes newlines in md", () => {
      const code = `
- item1
- item2`;
      const path = "definitions/foo.md";
      const result = compile(code, path);
      expect(result).to.equal("exports.contents = `\n- item1\n- item2`;");
    });
    test("escapes template literals in md", () => {
      const code = "select ${foo}";
      const path = "definitions/foo.md";
      const result = compile(code, path);
      expect(result).to.equal("exports.contents = `select \\${foo}`;");
    });
    test("escapes backslashes and backticks in md", () => {
      const code = "c = '\\'";
      const path = "definitions/foo.md";
      const result = compile(code, path);
      expect(result).to.equal("exports.contents = `c = '\\\\'`;");
    });
    test("escapes backticks in md", () => {
      const code = "select `a` from `b`";
      const path = "definitions/foo.md";
      const result = compile(code, path);
      expect(result).to.equal("exports.contents = `select \\`a\\` from \\`b\\``;");
    });

    suite("sqlx jit compilation", () => {
      test("compiles sqlx with compilation_mode: 'jit' to sqlxJitAction", () => {
        const code = `config { type: "table", compilation_mode: "jit" } select 1`;
        const result = compile(code, "definitions/foo.sqlx");
        expect(result).to.include("dataform.sqlxJitAction");
        expect(result).to.include('name: "foo"');
        expect(result).to.include(
          "const executionData = jctx.executionData ? jctx.executionData : undefined;",
        );
      });

      test("escapes multiple backslashes and quotes in sqlx jit compilation", () => {
        const code = `config { type: "table", compilation_mode: "jit" }
js {
  const re = /\\d+\\.\\w+/;
  const quoted = "hello \\"world\\"";
}
SELECT REGEXP_EXTRACT('a_123', r'\\d+'), '\\n'`;
        const result = compile(code, "definitions/escaped.sqlx");
        // Evaluate the generated jitCode string literal to verify it parses back cleanly.
        const jitCodeMatch = result.match(/jitCode:\s*("(?:[^"\\]|\\.)*")/);
        expect(jitCodeMatch).to.not.equal(null);
        const parsedJitCode = JSON.parse(jitCodeMatch[1]);
        expect(parsedJitCode).to.include("const re = /\\d+\\.\\w+/;");
        expect(parsedJitCode).to.include('const quoted = "hello \\"world\\"";');
        expect(parsedJitCode).to.include("SELECT REGEXP_EXTRACT('a_123', r'\\\\d+'), '\\\\n'");
      });

      test("throws error when incremental_where is used with jit compilation", () => {
        const code = `config { type: "incremental", compilation_mode: "jit" }
SELECT 1
incremental_where {
  ts > '2024-01-01'
}`;
        expect(() => compile(code, "definitions/inc.sqlx")).to.throw(
          "incremental_where is not supported with JiT compilation.",
        );
      });

      test("throws error when multiple SQL statements are used in non-operations jit sqlx", () => {
        for (const actionType of ["table", "view", "incremental", "assertion"]) {
          const code = `config { type: "${actionType}", compilation_mode: "jit" }
SELECT 1
---
SELECT 2`;
          expect(() => compile(code, `definitions/${actionType}.sqlx`)).to.throw(
            "Actions may only contain more than one SQL statement if they are of type 'operations'.",
          );
        }
      });

      test("throws error when pre_operations or post_operations are used on non-dataset jit sqlx", () => {
        for (const actionType of ["operations", "assertion"]) {
          const preCode = `config { type: "${actionType}", compilation_mode: "jit" }
SELECT 1
pre_operations {
  SELECT 0
}`;
          expect(() => compile(preCode, `definitions/${actionType}.sqlx`)).to.throw(
            "Actions may only include pre_operations if they create a dataset.",
          );

          const postCode = `config { type: "${actionType}", compilation_mode: "jit" }
SELECT 1
post_operations {
  SELECT 2
}`;
          expect(() => compile(postCode, `definitions/${actionType}.sqlx`)).to.throw(
            "Actions may only include post_operations if they create a dataset.",
          );
        }
      });

      test("throws error when input block is used on non-test jit sqlx", () => {
        const code = `config { type: "table", compilation_mode: "jit" }
input "other" {
  SELECT 1
}
SELECT 1`;
        expect(() => compile(code, "definitions/table.sqlx")).to.throw(
          "input blocks are not supported with JiT compilation.",
        );
      });

      test("throws error for unsupported action type in jit sqlx", () => {
        for (const actionType of ["declaration", "test", "notebook", "unknown"]) {
          const code = `config { type: "${actionType}", compilation_mode: "jit" }
SELECT 1`;
          expect(() => compile(code, `definitions/${actionType}.sqlx`)).to.throw(
            `Unsupported by JiT compilation mode action type: ${actionType}`,
          );
        }
      });

      test("accepts compilation_mode: 'aot' and throws clear error for invalid compilation_mode", () => {
        const aotCode = `config { type: "table", compilation_mode: "aot" } SELECT 1`;
        const aotResult = compile(aotCode, "definitions/aot.sqlx");
        expect(aotResult).to.include("dataform.sqlxAction");

        for (const invalidMode of ["JIT", "Jit", "jti"]) {
          const badCode = `config { type: "table", compilation_mode: "${invalidMode}" } SELECT 1`;
          expect(() => compile(badCode, "definitions/bad.sqlx")).to.throw(
            `Invalid compilation_mode "${invalidMode}" in definitions/bad.sqlx; expected "jit" or "aot".`,
          );
        }
      });

      test("syntax-checks jitCode at compile time (including redeclaring const data)", () => {
        const duplicateConstData = `config { type: "table", compilation_mode: "jit" }
js { const data = ["a", "b"]; }
SELECT \${data.length} AS n`;
        expect(() => compile(duplicateConstData, "definitions/dup_data.sqlx")).to.throw(
          SyntaxError,
          /Identifier 'data' has already been declared/,
        );

        const syntaxErrorCode = `config { type: "table", compilation_mode: "jit" }
js { const x = ; }
SELECT 1`;
        expect(() => compile(syntaxErrorCode, "definitions/syntax_err.sqlx")).to.throw(SyntaxError);
      });

      test("supports non-literal config referencing includes and dataform.* in jit sqlx", () => {
        const includesConfig = `config { type: "view", compilation_mode: "jit", tags: constants.TAGS }
SELECT 1`;
        const includesResult = compile(includesConfig, "definitions/inc_cfg.sqlx");
        expect(includesResult).to.include("dataform.sqlxJitAction");
        expect(includesResult).to.include("tags: constants.TAGS");

        const dynamicTypeConfig = `config { type: dataform.projectConfig.vars.prod ? "table" : "view", compilation_mode: "jit" }
SELECT 1`;
        const dynamicTypeResult = compile(dynamicTypeConfig, "definitions/dyn_type.sqlx");
        expect(dynamicTypeResult).to.include("dataform.sqlxJitAction");
        expect(dynamicTypeResult).to.include("jitCodeByType:");
      });
    });
  });
});
