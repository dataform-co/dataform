import { expect } from "chai";

import { workflowSettingsAsProjectConfig } from "df/core/workflow_settings";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("workflowSettingsAsProjectConfig", () => {
  test("copies lineage settings when set", () => {
    const workflowSettings = dataform.WorkflowSettings.create({
      lineage: { enabled: true },
    });

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.lineage).to.deep.equal({ enabled: true });
  });

  test("leaves lineage unset when workflow settings omit it", () => {
    const workflowSettings = dataform.WorkflowSettings.create({});

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.lineage).to.equal(null);
  });
});
