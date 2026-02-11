import { expect } from "chai";
import { suite, test } from "df/testing";
import { compile } from "df/core/compilers";

suite("core/compilers", () => {
    suite("compile", () => {
        test("compiles sqlx to js", () => {
            const code = `config { type: "table" } select 1`;
            const path = "definitions/foo.sqlx";
            const result = compile(code, path);
            expect(result).to.include("dataform.sqlxAction");
            expect(result).to.include("name: \"foo\"");
            expect(result).to.include("{ type: \"table\" }");
            // The sqlx compiler will format the query.
            expect(result).to.include("select 1");
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
            expect(() => compile(code, path)).to.throw(`${path} is not a valid YAML file: YAMLException: bad indentation of a mapping entry (1:6)`);
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
    });
});