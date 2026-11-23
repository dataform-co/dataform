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

  test("maps defaultIcebergConfig to projectConfig.defaultIcebergConfig", () => {
    const workflowSettings = dataform.WorkflowSettings.create({
      defaultIcebergConfig: {
        bucketName: "my-iceberg-bucket",
        tableFolderRoot: "warehouse",
        tableFolderSubpath: "analytics",
        connection: "my-project.us.my-connection",
      },
    });

    const projectConfig = workflowSettingsAsProjectConfig(workflowSettings);

    expect(projectConfig.defaultIcebergConfig).to.deep.equal({
      bucketName: "my-iceberg-bucket",
      tableFolderRoot: "warehouse",
      tableFolderSubpath: "analytics",
      connection: "my-project.us.my-connection",
    });
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

  test("keeps WorkflowSettings.DefaultIcebergConfig field- and wire-compatible with DefaultIcebergConfig", () => {
    const configFields = Object.keys(
      dataform.WorkflowSettings.DefaultIcebergConfig.toObject(
        new dataform.WorkflowSettings.DefaultIcebergConfig(),
        { defaults: true },
      ),
    ).sort();
    const coreFields = Object.keys(
      dataform.DefaultIcebergConfig.toObject(new dataform.DefaultIcebergConfig(), {
        defaults: true,
      }),
    ).sort();
    expect(coreFields).to.deep.equal([
      "bucketName",
      "connection",
      "tableFolderRoot",
      "tableFolderSubpath",
    ]);
    expect(configFields).to.deep.equal(coreFields);

    const sample = Object.fromEntries(
      coreFields.map((field, index) => [field, `iceberg_${field}_${index}`]),
    );
    const encodedFromConfig =
      dataform.WorkflowSettings.DefaultIcebergConfig.encode(sample).finish();
    const decodedAsCore = dataform.DefaultIcebergConfig.toObject(
      dataform.DefaultIcebergConfig.decode(encodedFromConfig),
    );
    expect(decodedAsCore).to.deep.equal(sample);

    const projectConfig = workflowSettingsAsProjectConfig(
      dataform.WorkflowSettings.create({ defaultIcebergConfig: sample }),
    );
    expect(projectConfig.defaultIcebergConfig).to.deep.equal(sample);
  });

  test("keeps WorkflowSettings.DefaultLakehouseConfig field- and wire-compatible with DefaultLakehouseConfig", () => {
    const configFields = Object.keys(
      dataform.WorkflowSettings.DefaultLakehouseConfig.toObject(
        new dataform.WorkflowSettings.DefaultLakehouseConfig(),
        { defaults: true },
      ),
    ).sort();
    const coreFields = Object.keys(
      dataform.DefaultLakehouseConfig.toObject(new dataform.DefaultLakehouseConfig(), {
        defaults: true,
      }),
    ).sort();
    expect(coreFields).to.deep.equal(["catalog", "namespace"]);
    expect(configFields).to.deep.equal(coreFields);

    const sample = Object.fromEntries(
      coreFields.map((field, index) => [field, `lakehouse_${field}_${index}`]),
    );
    const encodedFromConfig =
      dataform.WorkflowSettings.DefaultLakehouseConfig.encode(sample).finish();
    const decodedAsCore = dataform.DefaultLakehouseConfig.toObject(
      dataform.DefaultLakehouseConfig.decode(encodedFromConfig),
    );
    expect(decodedAsCore).to.deep.equal(sample);

    const projectConfig = workflowSettingsAsProjectConfig(
      dataform.WorkflowSettings.create({ defaultLakehouseConfig: sample }),
    );
    expect(projectConfig.defaultLakehouseConfig).to.deep.equal(sample);
  });
});
