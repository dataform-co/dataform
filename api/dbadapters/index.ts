import { query } from "..";

import { Credentials } from "df/api/commands/credentials";
import { BigQueryDbAdapter } from "df/api/dbadapters/bigquery";
import { RedshiftDbAdapter } from "df/api/dbadapters/redshift";
import { SnowflakeDbAdapter } from "df/api/dbadapters/snowflake";
import { SQLDataWarehouseDBAdapter } from "df/api/dbadapters/sqldatawarehouse";
import { ErrorWithCause } from "df/common/errors/errors";
import { concatenateQueries } from "df/core/tasks";
import { dataform } from "df/protos/ts";

export type OnCancel = (handleCancel: () => void) => void;

export const CACHED_STATE_TABLE_TARGET: dataform.ITarget = {
  schema: "dataform_meta",
  name: "cache_state"
};

export interface IExecutionResult {
  rows: any[];
  metadata: dataform.IExecutionMetadata;
}

export interface IDbAdapter {
  execute(
    statement: string,
    options?: {
      onCancel?: OnCancel;
      interactive?: boolean;
      maxResults?: number;
    }
  ): Promise<IExecutionResult>;
  evaluate(
    queryOrAction: string | dataform.ITable | dataform.IOperation | dataform.IAssertion
  ): Promise<dataform.IQueryEvaluation[]>;
  tables(): Promise<dataform.ITarget[]>;
  table(target: dataform.ITarget): Promise<dataform.ITableMetadata>;
  preview(target: dataform.ITarget, limitRows?: number): Promise<any[]>;
  prepareSchema(database: string, schema: string): Promise<void>;
  prepareStateMetadataTable(): Promise<void>;
  persistStateMetadata(actions: dataform.IExecutionAction[]): Promise<void>;
  persistedStateMetadata(): Promise<dataform.IPersistedTableMetadata[]>;
  setMetadata(action: dataform.IExecutionAction): Promise<void>;
  deleteStateMetadata(actions: dataform.IExecutionAction[]): Promise<void>;
  close(): Promise<void>;
}

export interface IDbAdapterClass<T extends IDbAdapter> {
  create: (credentials: Credentials) => Promise<T>;
}

const registry: { [warehouseType: string]: IDbAdapterClass<IDbAdapter> } = {};

export function register(warehouseType: string, c: IDbAdapterClass<IDbAdapter>) {
  registry[warehouseType] = c;
}

export async function create(credentials: Credentials, warehouseType: string): Promise<IDbAdapter> {
  if (!registry[warehouseType]) {
    throw new Error(`Unsupported warehouse: ${warehouseType}`);
  }
  return await registry[warehouseType].create(credentials);
}

register("bigquery", BigQueryDbAdapter);
// TODO: The redshift client library happens to work well for postgres, but we should probably
// not be relying on that behaviour. At some point we should replace this with a first-class
// PostgresAdapter.
register("postgres", RedshiftDbAdapter);
register("redshift", RedshiftDbAdapter);
register("snowflake", SnowflakeDbAdapter);
register("sqldatawarehouse", SQLDataWarehouseDBAdapter);

export function collectEvaluationQueries(
  queryOrAction: string | dataform.Table | dataform.Operation | dataform.Assertion,
  concatenate: boolean,
  queryModifier: (mod: string) => string = (q: string) => q
): dataform.ValidationQuery[] {
  // TODO: The prefix method (via `queryModifier`) is a bit sketchy. For example after
  // attaching the `explain` prefix, a table or operation could look like this:
  // ```
  // explain
  // -- Delete the temporary table, if it exists (perhaps from a previous run).
  // DROP TABLE IF EXISTS "df_integration_test"."load_from_s3_temp" CASCADE;
  // ```
  // which is invalid because the `explain` is interrupted by a comment.
  const validationQueries = new Array<dataform.IValidationQuery>();
  if (typeof queryOrAction === "string") {
    validationQueries.push({ query: queryModifier(queryOrAction) });
  } else {
    try {
      if (queryOrAction instanceof dataform.Table) {
        if (queryOrAction.type === "incremental") {
          const incrementalTableQueries = queryOrAction.incrementalPreOps.concat(
            queryOrAction.incrementalQuery,
            queryOrAction.incrementalPostOps
          );
          if (concatenate) {
            validationQueries.push({
              query: concatenateQueries(incrementalTableQueries, queryModifier),
              incremental: true
            });
          } else {
            incrementalTableQueries.forEach(query =>
              validationQueries.push({ query: queryModifier(query), incremental: true })
            );
          }
        }
        const tableQueries = queryOrAction.preOps.concat(
          queryOrAction.query,
          queryOrAction.postOps
        );
        if (concatenate) {
          validationQueries.push({
            query: concatenateQueries(tableQueries, queryModifier)
          });
        } else {
          tableQueries.forEach(query => validationQueries.push({ query: queryModifier(query) }));
        }
      } else if (queryOrAction instanceof dataform.Operation) {
        if (concatenate) {
          validationQueries.push({
            query: concatenateQueries(queryOrAction.queries, queryModifier)
          });
        } else {
          queryOrAction.queries.forEach(query =>
            validationQueries.push({ query: queryModifier(query) })
          );
        }
      } else if (queryOrAction instanceof dataform.Assertion) {
        validationQueries.push({ query: queryModifier(queryOrAction.query) });
      } else {
        throw new Error("Unrecognized evaluate type.");
      }
    } catch (e) {
      throw new ErrorWithCause(`Error building tasks for evaluation. ${e.message}`, e);
    }
  }
  return validationQueries
    .map(validationQuery =>
      dataform.ValidationQuery.create({ query: validationQuery.query.trim(), ...validationQuery })
    )
    .filter(validationQuery => !!validationQuery.query);
}
