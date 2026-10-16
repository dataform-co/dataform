import type { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

export interface IResolvedLakehouseFields {
  catalog: string;
  namespace: string;
  schema?: string;
}

export function resolveLakehouseFields(
  lakehouse?: dataform.ActionConfig.ILakehouseConfig | null,
  defaultLakehouseConfig?: dataform.IDefaultLakehouseConfig | null,
): IResolvedLakehouseFields {
  const catalog = lakehouse?.catalog || defaultLakehouseConfig?.catalog || "";
  const namespace = lakehouse?.namespace || defaultLakehouseConfig?.namespace || "";
  return {
    catalog,
    namespace,
    schema: catalog && namespace ? `${catalog}.${namespace}` : undefined,
  };
}

export function resolveLakehouseSchema(
  lakehouse?: dataform.ActionConfig.ILakehouseConfig | null,
  defaultLakehouseConfig?: dataform.IDefaultLakehouseConfig | null,
  requireComplete = false,
): string | undefined {
  const { schema } = resolveLakehouseFields(lakehouse, defaultLakehouseConfig);
  if (requireComplete && !schema) {
    throw new Error(
      "Lakehouse target reference must specify both 'catalog' and 'namespace' (or set defaultLakehouseConfig in workflow_settings.yaml).",
    );
  }
  return schema;
}

export function validateLakehouseConfig(
  session: Session,
  config:
    | dataform.ActionConfig.TableConfig
    | dataform.ActionConfig.IncrementalTableConfig
    | dataform.ActionConfig.OperationConfig
    | dataform.ActionConfig.DeclarationConfig,
  fileName?: string,
  hasDatasetOrSchema?: boolean,
): boolean {
  if (!config.lakehouse) {
    return false;
  }
  const { catalog, namespace, schema } = resolveLakehouseFields(
    config.lakehouse,
    session.projectConfig?.defaultLakehouseConfig,
  );
  const errorTarget = dataform.Target.create({
    database: config.project || session.projectConfig?.defaultDatabase || undefined,
    schema: schema || config.dataset || session.projectConfig?.defaultSchema || undefined,
    name: config.name,
  });

  if (hasDatasetOrSchema || !!config.dataset) {
    session.compileError(
      new Error("Cannot specify both 'lakehouse' and 'dataset'/'schema' on the same action."),
      fileName,
      errorTarget,
    );
  }
  if ("iceberg" in config && !!config.iceberg) {
    session.compileError(
      new Error("Cannot specify both 'lakehouse' and 'iceberg' on the same action."),
      fileName,
      errorTarget,
    );
  }
  if (!catalog) {
    session.compileError(
      new Error(
        "When defining a Lakehouse target, 'catalog' must be defined in workflow_settings.yaml or the lakehouse config block.",
      ),
      fileName,
      errorTarget,
    );
  } else if (catalog.includes(".")) {
    session.compileError(
      new Error("Action target catalog cannot include '.'"),
      fileName,
      errorTarget,
    );
  }
  if (!namespace) {
    session.compileError(
      new Error(
        "When defining a Lakehouse target, 'namespace' must be defined in workflow_settings.yaml or the lakehouse config block.",
      ),
      fileName,
      errorTarget,
    );
  } else if (namespace.includes(".")) {
    session.compileError(
      new Error("Action target namespace cannot include '.'"),
      fileName,
      errorTarget,
    );
  }
  return true;
}

export function validateLakehouseColumns(
  session: Session,
  actionDescriptor: dataform.IActionDescriptor | null | undefined,
  fileName: string,
  target: dataform.ITarget,
) {
  if (
    actionDescriptor?.columns?.some(
      (column) => column.bigqueryPolicyTags && column.bigqueryPolicyTags.length > 0,
    )
  ) {
    session.compileError(
      new Error(
        "Column policy tags (bigqueryPolicyTags) are not supported for Lakehouse (4-part) targets",
      ),
      fileName,
      target,
    );
  }
}

export function validateLakehouseBigQueryOptions(
  session: Session,
  bigquery: dataform.IBigQueryOptions | null | undefined,
  fileName: string,
  target: dataform.ITarget,
) {
  if (!bigquery) {
    return;
  }
  if (bigquery.labels && Object.keys(bigquery.labels).length > 0) {
    session.compileError(
      new Error("BigQuery labels are not supported for Lakehouse (4-part) targets"),
      fileName,
      target,
    );
  }
  if (bigquery.clusterBy && bigquery.clusterBy.length > 0) {
    session.compileError(
      new Error("clusterBy is not supported for Lakehouse (4-part) targets"),
      fileName,
      target,
    );
  }
  if (
    bigquery.requirePartitionFilter ||
    bigquery.additionalOptions?.require_partition_filter !== undefined
  ) {
    session.compileError(
      new Error("requirePartitionFilter is not supported for Lakehouse (4-part) targets"),
      fileName,
      target,
    );
  }
  if (
    bigquery.partitionExpirationDays ||
    bigquery.additionalOptions?.partition_expiration_days !== undefined
  ) {
    session.compileError(
      new Error("partitionExpirationDays is not supported for Lakehouse (4-part) targets"),
      fileName,
      target,
    );
  }
}
