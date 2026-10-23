import { expect } from "chai";

import { resolveLineageEnablement } from "df/cli/api/lineage/enablement";
import { suite, test } from "df/testing";

suite("resolveLineageEnablement", () => {
  const cases: Array<{
    cli: boolean | undefined;
    yaml: boolean | undefined;
    enabled: boolean;
    source: string;
  }> = [
    { cli: undefined, yaml: undefined, enabled: true, source: "default" },
    { cli: undefined, yaml: true, enabled: true, source: "workflow_settings" },
    { cli: undefined, yaml: false, enabled: false, source: "workflow_settings" },
    { cli: true, yaml: undefined, enabled: true, source: "cli_flag" },
    { cli: true, yaml: true, enabled: true, source: "cli_flag" },
    { cli: true, yaml: false, enabled: true, source: "cli_flag" },
    { cli: false, yaml: undefined, enabled: false, source: "cli_flag" },
    { cli: false, yaml: true, enabled: false, source: "cli_flag" },
    { cli: false, yaml: false, enabled: false, source: "cli_flag" },
  ];

  for (const { cli, yaml, enabled, source } of cases) {
    test(`cli=${cli} yaml=${yaml} resolves to enabled=${enabled} source=${source}`, () => {
      expect(resolveLineageEnablement(cli, yaml)).to.deep.equal({ enabled, source });
    });
  }
});
