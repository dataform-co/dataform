import { expect } from "chai";
import * as fs from "fs";

import { suite, test } from "df/testing";

const PACKAGED_CORE_DIR = "core/node_modules/@dataform/core";

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

suite("@dataform/core package", () => {
  test("package.json declares no runtime dependencies and leaks no workspace fields", () => {
    const packageJson = JSON.parse(
      fs.readFileSync(`${PACKAGED_CORE_DIR}/package.json`, "utf8"),
    ) as Record<string, unknown>;

    expect(packageJson.name).to.equal("@dataform/core");
    expect(packageJson.main).to.equal("bundle.js");
    expect(packageJson.dependencies).to.deep.equal({});
    expect(packageJson).to.not.have.property("devDependencies");
    expect(packageJson).to.not.have.property("pnpm");
    expect(packageJson).to.not.have.property("packageManager");
  });

  test("bundle.d.ts only imports expected external declaration modules", () => {
    const bundleDts = fs.readFileSync(`${PACKAGED_CORE_DIR}/bundle.d.ts`, "utf8");

    expect(extractExternalModuleSpecifiers(bundleDts)).to.deep.equal(["long", "protobufjs"]);
  });
});
