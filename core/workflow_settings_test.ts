import { expect } from "chai";

import { workflowSettingsAsProjectConfig } from "df/core/workflow_settings";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("workflowSettingsAsProjectConfig", () => {
  test("sets lineageEnabled when workflow settings enable lineage", () => {
    const workflowSettings = dataform.WorkflowSettings.create({
      lineage: { enabled: true },
    });

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.lineageEnabled).to.equal(true);
  });

  test("leaves lineageEnabled unset when workflow settings omit lineage", () => {
    const workflowSettings = dataform.WorkflowSettings.create({});

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.lineageEnabled).to.equal(null);
  });

  test("sets lineageEnabled to false when lineage.enabled is explicitly false", () => {
    const workflowSettings = dataform.WorkflowSettings.create({
      lineage: { enabled: false },
    });

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.lineageEnabled).to.equal(false);
  });

  test("maps defaultJobLabels when provided in workflow settings", () => {
    const workflowSettings = dataform.WorkflowSettings.create({
      defaultJobLabels: { env: "prod", cost_center: "123" },
    });

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.defaultJobLabels).to.deep.equal({ env: "prod", cost_center: "123" });
  });

  test("leaves defaultJobLabels empty when workflow settings omit defaultJobLabels", () => {
    const workflowSettings = dataform.WorkflowSettings.create({});

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.defaultJobLabels).to.deep.equal({});
  });

  test("maps defaultLakehouseConfig to projectConfig.defaultLakehouseConfig", () => {
    const workflowSettings = dataform.WorkflowSettings.create({
      defaultLakehouseConfig: {
        catalog: "my_catalog",
        namespace: "my_namespace",
      },
    });

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.defaultLakehouseConfig).to.deep.equal({
      catalog: "my_catalog",
      namespace: "my_namespace",
    });
  });
});
