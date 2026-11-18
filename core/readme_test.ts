import { expect } from "chai";
import * as fs from "fs";
import * as path from "path";

import { suite, test } from "df/testing";
import { BUNDLED_PROTO_FILES } from "df/tools/docs/proto_extractor";

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

  test("documents public @dataform/core runtime exports", () => {
    const readme = fs.readFileSync("core/README.md", "utf8");
    const exportsSectionMatch =
      /### TypeScript \/ JavaScript Exports\r?\n([\s\S]*?)(?=\r?\n### |\r?\n## |$)/.exec(readme);
    expect(exportsSectionMatch).to.not.equal(null);
    const exportsSection = exportsSectionMatch![1];

    const corePackage = require(path.resolve("core/node_modules/@dataform/core"));
    const runtimeExports = Object.keys(corePackage).filter(
      (exportName) => exportName !== "indexFileGenerator",
    );

    for (const exportName of runtimeExports) {
      expect(exportsSection).to.include(`- **\`${exportName}\`**`);
    }
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
