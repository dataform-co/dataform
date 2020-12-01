import * as Presto from "presto-client";
import * as PromisePool from "promise-pool-executor";

import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IDbClient, IExecutionResult, OnCancel } from "df/api/dbadapters/index";
import { LimitedResultSet } from "df/api/utils/results";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

// TODO: Move this to somewhere both the API and CLI can use?
function resolveTarget(target: dataform.ITarget) {
  return `${target.database ? `${target.database}.` : ""}${
    target.schema ? `${target.schema}.` : ""
  }${target.name}`;
}

export class PrestoDbAdapter implements IDbAdapter {
  public static async create(credentials: Credentials, options?: { concurrencyLimit?: number }) {
    return new PrestoDbAdapter(credentials, options);
  }

  private readonly client: Presto.Client;
  private readonly pool: PromisePool.PromisePoolExecutor;

  private constructor(credentials: Credentials, options?: { concurrencyLimit?: number }) {
    this.client = new Presto.Client(credentials as Presto.IPrestoClientOptions);
    this.pool = new PromisePool.PromisePoolExecutor({
      concurrencyLimit: options?.concurrencyLimit || 10,
      frequencyWindow: 1000,
      frequencyLimit: 10
    });
  }

  public async execute(
    statement: string,
    options: {
      rowLimit?: number;
      byteLimit?: number;
      onCancel?: OnCancel;
    } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
  ): Promise<IExecutionResult> {
    let isCancelled = false;
    options.onCancel?.(() => (isCancelled = true));
    const rows = await this.pool
      .addSingleTask({
        generator: () =>
          new Promise<any>((resolve, reject) => {
            const allRows = new LimitedResultSet({
              rowLimit: options.rowLimit,
              byteLimit: options.byteLimit
            });
            const rejectIfError = (error: Presto.IPrestoClientError) => {
              if (!!error) {
                reject(error);
                return true;
              }
            };
            this.client.execute({
              query: statement,
              cancel: () => {
                // The presto client cancels executions if this returns true.
                return isCancelled;
              },
              data: (error, data, columns, stats) => {
                if (rejectIfError(error)) {
                  return;
                }
                if (!allRows.push(data)) {
                  isCancelled = true;
                }
                resolve(allRows.rows);
              },
              success: (error, stats) => {
                if (rejectIfError(error)) {
                  return;
                }
                resolve(allRows.rows);
              },
              error: rejectIfError
            });
          })
      })
      .promise();
    return { rows, metadata: {} };
  }

  public async withClientLock<T>(callback: (client: IDbClient) => Promise<T>) {
    return await callback(this);
  }

  public async evaluate(queryOrAction: QueryOrAction, projectConfig?: dataform.ProjectConfig) {
    const validationQueries = collectEvaluationQueries(queryOrAction, true, (query: string) =>
      !!query ? `explain ${query}` : ""
    ).map((validationQuery, index) => ({ index, validationQuery }));
    const validationQueriesWithoutWrappers = collectEvaluationQueries(queryOrAction, true);

    const queryEvaluations = new Array<dataform.IQueryEvaluation>();
    for (const { index, validationQuery } of validationQueries) {
      let evaluationResponse: dataform.IQueryEvaluation = {
        status: dataform.QueryEvaluation.QueryEvaluationStatus.SUCCESS
      };
      try {
        await this.execute(validationQuery.query);
      } catch (e) {
        evaluationResponse = {
          status: dataform.QueryEvaluation.QueryEvaluationStatus.FAILURE,
          error: {
            message: e.message,
            errorLocation: {
              line: e.errorLocation?.lineNumber,
              column: e.errorLocation?.columnNumber
            }
          }
        };
      }
      queryEvaluations.push(
        dataform.QueryEvaluation.create({
          ...evaluationResponse,
          incremental: validationQuery.incremental,
          query: validationQueriesWithoutWrappers[index].query
        })
      );
    }
    return queryEvaluations;
  }

  public async databases(): Promise<string[]> {
    const result = await this.execute("show catalogs");
    return result.rows.flat();
  }

  public async schemas(databases?: string[]): Promise<dataform.ISchema[]> {
    if (databases === undefined) {
      databases = await this.databases();
    }

    const schemas = new Array<dataform.ISchema>();
    await Promise.all(
      databases.map(async database => {
        const { rows } = await this.execute(`show schemas from ${database}`);
        schemas.push(...rows.flat().map(cell => ({ database, schema: cell })));
      })
    );
    return schemas;
  }

  public async createSchema(schema: dataform.ISchema): Promise<void> {
    await this.execute(`create schema if not exists ${schema.database}.${schema.schema}`);
  }

  public async tables(): Promise<dataform.ITarget[]> {
    const targets: dataform.ITarget[] = [];
    (await this.schemas()).forEach(async schema => {
      const result = await this.execute(`show tables from ${schema.database}.${schema.schema}`);
      targets.push(
        ...result.rows.flat().map(table =>
          dataform.Target.create({
            database: schema.database,
            schema: schema.schema,
            name: table as string
          })
        )
      );
    });
    return targets;
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    let columnsResult: IExecutionResult;
    try {
      // TODO: In the future, the connection won't specify the database and schema to allow moving data between
      // tables in different databases, in which case target resolution should throw if both are not present.
      columnsResult = await this.execute(`describe ${resolveTarget(target)}`);
    } catch (e) {
      // If the table doesn't exist just return empty metadata.
      // In this circumstance the error will be of type Presto.IPrestoClientError.
      if (e.errorName === "TABLE_NOT_FOUND") {
        return null;
      }
      throw e;
    }
    // TODO: Add primitives mapping.
    // Columns are structured [column name, type (primitive), extra, comment]. This can be
    // seen under columnsResult.columns; instead they could be dynamically populated using this info.
    const fields =
      columnsResult?.rows.map(column =>
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
      `select * from ${resolveTarget(target)} limit ${limitRows}`
    );
    return rows;
  }

  public async setMetadata(action: dataform.IExecutionAction): Promise<void> {
    // Unimplemented.
  }

  public async close() {
    // Unimplemented.
  }

  public async search(
    searchText: string,
    options?: { limit: number }
  ): Promise<dataform.ITableMetadata[]> {
    // Unimplemented.
    return [];
  }
}
