import { expect } from "chai";
import Long from "long";
import { anyString, anything, capture, instance, mock, verify, when } from "ts-mockito";

import { build } from "df/cli/api/commands/build";
import { Runner } from "df/cli/api/commands/run";
import { BigQueryDbAdapter } from "df/cli/api/dbadapters/bigquery";
import { wrapWithCreateSchemaIfNotExists } from "df/cli/api/utils/query_transforms";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("Runner with 4-part Lakehouse targets", () => {
  const projectConfig: dataform.IProjectConfig = {
    warehouse: "bigquery",
    defaultDatabase: "my-gcp-project",
    defaultSchema: "default_dataset",
  };

  test("skips schemas() lookup and CREATE SCHEMA wrapper when all actions are 4-part Lakehouse targets", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    const executedStatements: string[] = [];
    when(mockedDbAdapter.execute(anyString(), anything())).thenCall(async (statement: string) => {
      executedStatements.push(statement);
      return { rows: [], metadata: {} };
    });

    const lakehouseStatement =
      "create or replace table `my-gcp-project.ecommerce_catalog.sales_dw.customer_orders` as select 1 as id";
    const opStatement =
      "insert into `my-gcp-project.ecommerce_catalog.sales_dw.audit_log` values (1)";
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        {
          type: "table",
          tableType: "table",
          target: {
            database: "my-gcp-project",
            schema: "ecommerce_catalog.sales_dw",
            name: "customer_orders",
          },
          tasks: [{ type: "statement", statement: lakehouseStatement }],
          dependencyTargets: [],
        },
        {
          type: "operation",
          hasOutput: true,
          target: {
            database: "my-gcp-project",
            schema: "ecommerce_catalog.sales_dw",
            name: "audit_log",
          },
          tasks: [{ type: "statement", statement: opStatement }],
          dependencyTargets: [],
        },
      ],
    });

    const realRunResult = await new Runner(instance(mockedDbAdapter), graph).execute().result();
    expect(realRunResult.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    const dryRunResult = await new Runner(instance(mockedDbAdapter), graph, {
      bigquery: { dryRun: true },
    })
      .execute()
      .result();
    expect(dryRunResult.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);

    verify(mockedDbAdapter.schemas(anything())).never();
    expect(executedStatements).deep.equals([
      lakehouseStatement,
      opStatement,
      lakehouseStatement,
      opStatement,
    ]);
  });

  test("wraps missing 3-part dataset with CREATE SCHEMA while leaving 4-part Lakehouse target unwrapped", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    const executedStatements: string[] = [];
    when(mockedDbAdapter.schemas("my-gcp-project")).thenResolve([]);
    when(mockedDbAdapter.execute(anyString(), anything())).thenCall(async (statement: string) => {
      executedStatements.push(statement);
      return { rows: [], metadata: {} };
    });

    const standardStatement =
      "create or replace table `my-gcp-project.new_dataset.std_table` as select 1";
    const lakehouseStatement =
      "create or replace table `my-gcp-project.my_cat.my_ns.lh_table` as select 1";
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        {
          type: "table",
          tableType: "table",
          target: { database: "my-gcp-project", schema: "new_dataset", name: "std_table" },
          tasks: [{ type: "statement", statement: standardStatement }],
          dependencyTargets: [],
        },
        {
          type: "table",
          tableType: "table",
          target: { database: "my-gcp-project", schema: "my_cat.my_ns", name: "lh_table" },
          tasks: [{ type: "statement", statement: lakehouseStatement }],
          dependencyTargets: [],
        },
      ],
    });

    const result = await new Runner(instance(mockedDbAdapter), graph).execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    verify(mockedDbAdapter.schemas("my-gcp-project")).once();
    expect(executedStatements).to.have.members([
      wrapWithCreateSchemaIfNotExists("my-gcp-project", "new_dataset", standardStatement),
      lakehouseStatement,
    ]);
  });

  test("build() fetches 4-part target warehouse state via dbadapter.table() and builds incremental DML", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    const lakehouseTarget: dataform.ITarget = {
      database: "my-gcp-project",
      schema: "ecommerce_catalog.sales_dw",
      name: "customer_orders",
    };
    when(mockedDbAdapter.table(anything())).thenResolve(
      dataform.TableMetadata.create({
        type: dataform.TableMetadata.Type.TABLE,
        target: lakehouseTarget,
        fields: [{ name: "order_id", primitive: dataform.Field.Primitive.INTEGER }],
        lastUpdatedMillis: Long.ZERO,
      }),
    );

    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig,
      dataformCoreVersion: "3.1.5",
      tables: [
        {
          type: "incremental",
          enumType: dataform.TableType.INCREMENTAL,
          target: lakehouseTarget,
          query: "SELECT 1 AS order_id",
          incrementalQuery: "SELECT 2 AS order_id",
          uniqueKey: ["order_id"],
        },
      ],
    });

    const executionGraph = await build(
      compiledGraph,
      { fullRefresh: false },
      instance(mockedDbAdapter),
    );

    expect(executionGraph.actions.length).equals(1);
    expect(executionGraph.actions[0].tasks[0].statement).to.include(
      "merge `my-gcp-project.ecommerce_catalog.sales_dw.customer_orders` DATAFORM_DEST",
    );
  });

  test("delegates setMetadata() with execution options on 4-part Lakehouse tables with descriptions", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    when(mockedDbAdapter.execute(anyString(), anything())).thenResolve({ rows: [], metadata: {} });
    when(mockedDbAdapter.setMetadata(anything(), anything())).thenResolve();

    const graph = dataform.ExecutionGraph.create({
      projectConfig: {
        ...projectConfig,
        defaultLocation: "us-central1",
        defaultReservation: "projects/my-gcp-project/locations/us-central1/reservations/default",
      },
      runConfig: {},
      warehouseState: {},
      actions: [
        {
          type: "table",
          tableType: "table",
          target: {
            database: "my-gcp-project",
            schema: "ecommerce_catalog.sales_dw",
            name: "customer_orders",
          },
          actionDescriptor: {
            description: "Lakehouse orders table",
            columns: [{ path: ["order_id"], description: "Order ID" }],
            jobLabels: { team: "analytics" },
          },
          tasks: [
            {
              type: "statement",
              statement:
                "create or replace table `my-gcp-project.ecommerce_catalog.sales_dw.customer_orders` as select 1 as order_id",
            },
          ],
          dependencyTargets: [],
        },
      ],
    });

    const result = await new Runner(instance(mockedDbAdapter), graph, {
      bigquery: {
        jobPrefix: "df-prefix",
        labels: { env: "prod" },
        actionRetryLimit: 2,
      },
    })
      .execute()
      .result();
    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    verify(mockedDbAdapter.setMetadata(anything(), anything())).once();
    const [, capturedOptions] = capture(mockedDbAdapter.setMetadata).last();
    expect(capturedOptions).deep.equals({
      bigquery: {
        dryRun: undefined,
        jobPrefix: "df-prefix",
        labels: { team: "analytics", env: "prod" },
        actionRetryLimit: 2,
        location: "us-central1",
        reservation: "projects/my-gcp-project/locations/us-central1/reservations/default",
      },
    });
  });

  test("creates missing defaultDataset (with suffixes) for incremental Lakehouse action with onSchemaChange", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    const executedStatements: string[] = [];
    const executedLocations: Array<string | undefined> = [];
    when(mockedDbAdapter.schemas("default-proj_dev")).thenResolve([]);
    when(mockedDbAdapter.execute(anyString(), anything())).thenCall(
      async (statement: string, options?: { bigquery?: { location?: string } }) => {
        executedStatements.push(statement);
        executedLocations.push(options?.bigquery?.location);
        return { rows: [], metadata: {} };
      },
    );

    const incStatement =
      "BEGIN\nSELECT 1;\nEND;\nmerge `catalog-proj.my_cat.my_ns.lh_inc` DATAFORM_DEST using (select 1) DATAFORM_SOURCE on false";
    const graph = dataform.ExecutionGraph.create({
      projectConfig: {
        warehouse: "bigquery",
        defaultDatabase: "default-proj",
        databaseSuffix: "dev",
        defaultSchema: "default_dataset",
        schemaSuffix: "staging",
        defaultLocation: "us-central1",
      },
      runConfig: {},
      warehouseState: {},
      actions: [
        {
          type: "table",
          tableType: "incremental",
          onSchemaChange: dataform.OnSchemaChange.EXTEND,
          target: { database: "catalog-proj", schema: "my_cat.my_ns", name: "lh_inc" },
          tasks: [{ type: "statement", statement: incStatement }],
          dependencyTargets: [],
        },
      ],
    });

    const result = await new Runner(instance(mockedDbAdapter), graph).execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    verify(mockedDbAdapter.schemas("default-proj_dev")).once();
    expect(executedStatements).deep.equals([
      wrapWithCreateSchemaIfNotExists("default-proj_dev", "default_dataset_staging", incStatement),
    ]);
    expect(executedLocations).deep.equals(["us-central1"]);
  });

  test("does not emit CREATE SCHEMA for incremental Lakehouse action with onSchemaChange when defaultDataset already exists", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    const executedStatements: string[] = [];
    when(mockedDbAdapter.schemas("my-gcp-project")).thenResolve(["default_dataset"]);
    when(mockedDbAdapter.execute(anyString(), anything())).thenCall(async (statement: string) => {
      executedStatements.push(statement);
      return { rows: [], metadata: {} };
    });

    const incStatement =
      "BEGIN\nSELECT 1;\nEND;\nmerge `my-gcp-project.my_cat.my_ns.lh_inc` DATAFORM_DEST using (select 1) DATAFORM_SOURCE on false";
    const graph = dataform.ExecutionGraph.create({
      projectConfig,
      runConfig: {},
      warehouseState: {},
      actions: [
        {
          type: "table",
          tableType: "incremental",
          onSchemaChange: dataform.OnSchemaChange.EXTEND,
          target: { database: "my-gcp-project", schema: "my_cat.my_ns", name: "lh_inc" },
          tasks: [{ type: "statement", statement: incStatement }],
          dependencyTargets: [],
        },
      ],
    });

    const result = await new Runner(instance(mockedDbAdapter), graph).execute().result();

    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    verify(mockedDbAdapter.schemas("my-gcp-project")).once();
    expect(executedStatements).deep.equals([incStatement]);
  });

  test("skips defaultDataset lookup and creation for incremental Lakehouse action when onSchemaChange is unset or IGNORE", async () => {
    const mockedDbAdapter = mock(BigQueryDbAdapter);
    const lakehouseTarget: dataform.ITarget = {
      database: "my-gcp-project",
      schema: "ecommerce_catalog.sales_dw",
      name: "customer_orders",
    };
    const extendTarget: dataform.ITarget = {
      database: "my-gcp-project",
      schema: "ecommerce_catalog.sales_dw",
      name: "customer_orders_extend",
    };
    when(mockedDbAdapter.table(anything())).thenResolve(null);
    const executedStatements: string[] = [];
    when(mockedDbAdapter.execute(anyString(), anything())).thenCall(async (statement: string) => {
      executedStatements.push(statement);
      return { rows: [], metadata: {} };
    });

    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig,
      dataformCoreVersion: "3.1.5",
      tables: [
        {
          type: "incremental",
          enumType: dataform.TableType.INCREMENTAL,
          target: lakehouseTarget,
          query: "SELECT 1 AS order_id",
          incrementalQuery: "SELECT 2 AS order_id",
        },
        {
          type: "incremental",
          enumType: dataform.TableType.INCREMENTAL,
          onSchemaChange: dataform.OnSchemaChange.IGNORE,
          target: {
            database: "my-gcp-project",
            schema: "ecommerce_catalog.sales_dw",
            name: "customer_orders_ignore",
          },
          query: "SELECT 1 AS order_id",
          incrementalQuery: "SELECT 2 AS order_id",
        },
      ],
    });

    const executionGraph = await build(
      compiledGraph,
      { fullRefresh: false },
      instance(mockedDbAdapter),
    );
    expect(
      executionGraph.actions.map((a) => a.onSchemaChange ?? dataform.OnSchemaChange.IGNORE),
    ).deep.equals([dataform.OnSchemaChange.IGNORE, dataform.OnSchemaChange.IGNORE]);

    const result = await new Runner(instance(mockedDbAdapter), executionGraph).execute().result();
    expect(result.status).equals(dataform.RunResult.ExecutionStatus.SUCCESSFUL);
    verify(mockedDbAdapter.schemas(anything())).never();
    expect(executedStatements).to.have.lengthOf(2);
    for (const stmt of executedStatements) {
      expect(stmt.toLowerCase()).to.not.include("create schema");
    }

    // Verify build() forwards non-IGNORE onSchemaChange onto ExecutionAction.
    const extendGraph = await build(
      dataform.CompiledGraph.create({
        projectConfig,
        dataformCoreVersion: "3.1.5",
        tables: [
          {
            type: "incremental",
            enumType: dataform.TableType.INCREMENTAL,
            onSchemaChange: dataform.OnSchemaChange.EXTEND,
            target: extendTarget,
            query: "SELECT 1 AS order_id",
            incrementalQuery: "SELECT 2 AS order_id",
          },
        ],
      }),
      { fullRefresh: false },
      instance(mockedDbAdapter),
    );
    expect(extendGraph.actions[0].onSchemaChange).equals(dataform.OnSchemaChange.EXTEND);
  });
});
