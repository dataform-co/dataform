import { expect } from "chai";
import * as fs from "fs";
import * as path from "path";

import { suite, test } from "df/testing";
import { BUNDLED_PROTO_FILES } from "df/tools/docs/proto_extractor";
import { extractCoreExports, extractNamedReExports } from "df/tools/docs/ts_extractor";

suite("core/README.md", () => {
  test("is bundled into the @dataform/core npm package", () => {
    const sourceReadme = fs.readFileSync("core/README.md", "utf8");
    const packagedReadmePath = "core/node_modules/@dataform/core/README.md";

    expect(fs.existsSync(packagedReadmePath)).to.equal(true);
    const packagedReadme = fs.readFileSync(packagedReadmePath, "utf8");
    expect(packagedReadme).to.equal(sourceReadme);
  });

  test("uses only anchor or https:// links so links resolve on npmjs.com", () => {
    const readme = fs.readFileSync("core/README.md", "utf8");
    const markdownLinkRegex = /!?\[[^\]]*\]\(([^)]+)\)/g;
    const nonNpmSafeLinks: string[] = [];

    let match = markdownLinkRegex.exec(readme);
    while (match !== null) {
      const target = match[1].trim();
      if (!target.startsWith("#") && !target.startsWith("https://")) {
        nonNpmSafeLinks.push(target);
      }
      match = markdownLinkRegex.exec(readme);
    }

    expect(nonNpmSafeLinks).to.deep.equal([]);
  });

  test("documents exact public @dataform/core runtime and type exports without internal leaks", () => {
    const corePackage = require(path.resolve("core/node_modules/@dataform/core"));
    const runtimeExports = Object.keys(corePackage)
      .filter((exportName) => exportName !== "indexFileGenerator")
      .sort();

    const documentedExports = extractCoreExports(
      "core/index.ts",
      "packages/@dataform/core/index.ts",
    );
    const documentedRuntimeNames = documentedExports
      .filter((item) => item.typeLabel !== "interface" && item.typeLabel !== "type")
      .map((item) => item.name)
      .sort();
    expect(runtimeExports).to.deep.equal(documentedRuntimeNames);

    const packageReExports = extractNamedReExports("packages/@dataform/core/index.ts");
    expect(packageReExports.has("SQLX_CONTEXT_FUNCTIONS")).to.equal(false);
    expect(packageReExports.has("SESSION_GLOBAL_METHODS")).to.equal(false);

    const bundleDts = fs.readFileSync("core/node_modules/@dataform/core/bundle.d.ts", "utf8");
    expect(bundleDts).to.include("IDataformExtension");
    expect(bundleDts).to.include("IJitCompiler");
    expect(bundleDts).to.not.include("SQLX_CONTEXT_FUNCTIONS");
    expect(bundleDts).to.not.include("SESSION_GLOBAL_METHODS");
  });

  test("bundled proto list matches .proto files packaged into @dataform/core", () => {
    const packageDir = "core/node_modules/@dataform/core";
    const packagedProtos = fs
      .readdirSync(packageDir)
      .filter((entry) => entry.endsWith(".proto"))
      .sort();

    expect(packagedProtos).to.deep.equal([...BUNDLED_PROTO_FILES].sort());
  });
});
