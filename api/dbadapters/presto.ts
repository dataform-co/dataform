import * as Presto from "presto-client";

import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IDbClient, IExecutionResult } from "df/api/dbadapters/index";
import { flatten } from "df/common/arrays/arrays";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

interface IPrestoExecutionResult {
  columns?: Presto.IPrestoClientColumnMetaData[];
  data?: Presto.PrestoClientColumnDatum[];
  error?: any;
  queryId?: string;
  stats?: Presto.IPrestoClientStats;
}

// TODO: In the future, the connection doesn't specify the catalog (database) and schema to allow moving data
// between tables, meaning target resolution will require both the database and schema to be required,
// so should throw if not present.
// HOWEVER, until the database and schema options are made optional in the presto client used (see IPresto proto comment),
// the integration tests will provide database and schema in the connection.
// TODO: Move this to somewhere both the API and CLI can use?
function resolveTarget(target: dataform.ITarget) {
  return `${target.database ? `${target.database}.` : ""}${
    target.schema ? `${target.schema}.` : ""
  }${target.name}`;
}

export class PrestoDbAdapter implements IDbAdapter {
  public static async create(
    credentials: Credentials,
    options?: { concurrencyLimit?: number; disableSslForTestsOnly?: boolean }
  ) {
    return new PrestoDbAdapter(credentials, options);
  }

  private prestoCredentials: dataform.IPresto;

  private client: Presto.Client;

  private constructor(credentials: Credentials, options?: { concurrencyLimit?: number }) {
    this.prestoCredentials = credentials as dataform.IPresto;
    this.client = new Presto.Client(this.prestoCredentials as Presto.IPrestoClientOptions);
  }

  public async execute(
    statement: string,
    // TODO: These execute options should actually allow any of Presto.IPrestoClientExecuteOptions.
    options: {
      rowLimit?: number;
      byteLimit?: number;
    } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
  ): Promise<IExecutionResult> {
    const prestoResult = await prestoExecute(this.client, statement);
    return { rows: prestoResult.data, metadata: {} };
  }

  public async withClientLock<T>(callback: (client: IDbClient) => Promise<T>) {
    return await callback(this);
  }

  public async evaluate(queryOrAction: QueryOrAction, projectConfig?: dataform.ProjectConfig) {
    const validationQueries = collectEvaluationQueries(
      queryOrAction,
      projectConfig?.useSingleQueryPerAction === undefined ||
        !!projectConfig?.useSingleQueryPerAction,
      (query: string) => (!!query ? `explain ${query}` : "")
    );

    return await Promise.all(
      validationQueries.map(async ({ query, incremental }) => {
        try {
          await prestoExecute(this.client);
          return dataform.QueryEvaluation.create({
            status: dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS,
            incremental,
            query
          });
        } catch (e) {
          return {
            status: dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE,
            // TODO: Parse the error with line number.
            error: e,
            incremental,
            query
          };
        }
      })
    );
  }

  public async catalogs(): Promise<string[]> {
    const result = await prestoExecute(this.client, "show catalogs");
    return flatten(result.data);
  }

  public async schemas(): Promise<string[]> {
    let schemas: string[] = [];
    (await this.catalogs()).forEach(async catalog => {
      const result = await prestoExecute(this.client, `show schemas from ${catalog}`);
      schemas = schemas.concat(flatten(result.data).map(schema => `${catalog}.${schema}`));
    });
    // TODO: Currently this returns `catalog.schema` rather than just schemas in order to
    // conform to string return type. Should this be changed?
    return schemas;
  }

  public async createSchema(catalog: string, schema: string): Promise<void> {
    await prestoExecute(this.client, `create schema if not exists ${catalog}.${schema}`);
  }

  // TODO: This should take parameters to allow for retrieving from a specific catalog/schema.
  public async tables(): Promise<dataform.ITarget[]> {
    let tables: dataform.ITarget[] = [];
    (await this.schemas()).forEach(async schemaString => {
      const [database, schema] = schemaString.split(".");
      const result = await prestoExecute(this.client, `show tables from ${schema}`);
      tables = tables.concat(flatten(result.data).map(table => ({ database, schema, table })));
    });
    return tables;
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    let columnsResult: IPrestoExecutionResult;
    try {
      columnsResult = await prestoExecute(this.client, `describe ${resolveTarget(target)}`);
    } catch (e) {
      // This probably failed because the table doesn't exist, so ignore as the information isn't necessary.
    }
    // TODO: Add primitives mapping.
    // Columns are structured [column name, type (primitive), extra, comment]. This can be
    // seen under columnsResult.columns; instead they could be dynamically populated using this info.
    const fields =
      columnsResult?.data.map(column =>
        dataform.Field.create({ name: column[0], description: column[3] })
      ) || [];
    return dataform.TableMetadata.create({
      // TODO: Add missing table metadata items.
      target,
      fields
    });
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const { rows } = await this.execute(
      `select * from ${target.database}.${target.schema}.${target.name} limit ${limitRows}`
    );
    return rows;
  }

  public async persistedStateMetadata(): Promise<dataform.IPersistedTableMetadata[]> {
    // TODO.
    return [];
  }

  public async persistStateMetadata() {
    // TODO.
  }

  public async setMetadata(action: dataform.IExecutionAction): Promise<void> {
    // TODO.
  }

  public async close() {
    // Not required, as data is transferred using the Presto REST api.
  }
}

export function prestoExecute(
  client: Presto.Client,
  statement?: string
): Promise<IPrestoExecutionResult> {
  return new Promise((resolve, reject) => {
    const result: IPrestoExecutionResult = {};
    client.execute({
      query: statement,
      cancel: () => {
        return false;
      },
      state: (error: any, queryId: string, stats: Presto.IPrestoClientStats) => {
        result.error = error;
        result.queryId = queryId;
        result.stats = stats;
      },
      columns: (error: any, columns: Presto.IPrestoClientColumnMetaData[]) => {
        result.error = error;
        result.columns = columns;
      },
      data: (
        error: any,
        data: Presto.PrestoClientColumnDatum[],
        columns: Presto.IPrestoClientColumnMetaData[],
        stats: Presto.IPrestoClientStats
      ) => {
        result.error = error;
        result.data = data;
        result.columns = columns;
        result.stats = stats;
      },
      success: (error: any, stats: Presto.IPrestoClientStats) => {
        if (!!error) {
          reject(error);
        }
        result.stats = stats;
        resolve(result);
      },
      error: (error: any) => {
        reject(error);
      }
    });
  });
}
