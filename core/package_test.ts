import { expect } from "chai";
import * as fs from "fs";

import { suite, test } from "df/testing";

const PACKAGED_MODULES: ReadonlyArray<{ name: string; dir: string }> = [
  { name: "@dataform/core", dir: "core/node_modules/@dataform/core" },
  { name: "@dataform/sample-extension", dir: "core/node_modules/@dataform/sample-extension" },
];

function extractExternalModuleSpecifiers(dtsContents: string): string[] {
  const importRegex = /^(?:import|export)\s+(?:.*?\s+from\s+)?["']([^"']+)["'];?$/gm;
  const specifiers = new Set<string>();
  let match = importRegex.exec(dtsContents);
  while (match !== null) {
    specifiers.add(match[1]);
    match = importRegex.exec(dtsContents);
  }
  return Array.from(specifiers).sort();
}

for (const pkg of PACKAGED_MODULES) {
  suite(`${pkg.name} package`, () => {
    test("package.json declares no runtime dependencies and leaks no workspace fields", () => {
      const packageJson = JSON.parse(fs.readFileSync(`${pkg.dir}/package.json`, "utf8")) as Record<
        string,
        unknown
      >;

      expect(packageJson.name).to.equal(pkg.name);
      expect(packageJson.main).to.equal("bundle.js");
      expect(packageJson.dependencies).to.deep.equal({});
      expect(packageJson).to.not.have.property("devDependencies");
      expect(packageJson).to.not.have.property("pnpm");
      expect(packageJson).to.not.have.property("packageManager");
    });

    test("bundle.d.ts only imports expected external declaration modules", () => {
      const bundleDts = fs.readFileSync(`${pkg.dir}/bundle.d.ts`, "utf8");

      expect(extractExternalModuleSpecifiers(bundleDts)).to.deep.equal(["long", "protobufjs"]);
    });
  });
}
