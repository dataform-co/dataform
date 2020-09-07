import * as pg from "pg";

import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IDbClient } from "df/api/dbadapters/index";
import { SSHTunnelProxy } from "df/api/ssh_tunnel_proxy";
import { parseRedshiftEvalError } from "df/api/utils/error_parsing";
import { convertFieldType, PgPoolExecutor } from "df/api/utils/postgres";
import { ErrorWithCause } from "df/common/errors/errors";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

interface IRedshiftAdapterOptions {
  sshTunnel?: SSHTunnelProxy;
}

export class RedshiftDbAdapter implements IDbAdapter {
  public static async create(
    credentials: Credentials,
    options?: { concurrencyLimit?: number; disableSslForTestsOnly?: boolean }
  ) {
    const jdbcCredentials = credentials as dataform.IJDBC;
    const baseClientConfig: Partial<pg.ClientConfig> = {
      user: jdbcCredentials.username,
      password: jdbcCredentials.password,
      database: jdbcCredentials.databaseName,
      ssl: options?.disableSslForTestsOnly ? false : { rejectUnauthorized: false }
    };
    if (jdbcCredentials.sshTunnel) {
      const sshTunnel = await SSHTunnelProxy.create(jdbcCredentials.sshTunnel, {
        host: jdbcCredentials.host,
        port: jdbcCredentials.port
      });
      const queryExecutor = new PgPoolExecutor(
        {
          ...baseClientConfig,
          host: "127.0.0.1",
          port: sshTunnel.localPort
        },
        options
      );
      return new RedshiftDbAdapter(queryExecutor, { sshTunnel });
    } else {
      const clientConfig: pg.ClientConfig = {
        ...baseClientConfig,
        host: jdbcCredentials.host,
        port: jdbcCredentials.port
      };
      const queryExecutor = new PgPoolExecutor(clientConfig, options);
      return new RedshiftDbAdapter(queryExecutor, {});
    }
  }

  private constructor(
    private readonly queryExecutor: PgPoolExecutor,
    private readonly options: IRedshiftAdapterOptions
  ) {}

  public async execute(
    statement: string,
    options: {
      params?: any[];
      rowLimit?: number;
      byteLimit?: number;
      includeQueryInError?: boolean;
    } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
  ) {
    return await this.withClientLock(executor => executor.execute(statement, options));
  }

  public async withClientLock<T>(callback: (client: IDbClient) => Promise<T>) {
    return await this.queryExecutor.withClientLock(client =>
      callback({
        execute: async (
          statement: string,
          options: {
            params?: any[];
            rowLimit?: number;
            byteLimit?: number;
            includeQueryInError?: boolean;
          } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
        ) => {
          try {
            const rows = await client.execute(statement, options);
            return { rows, metadata: {} };
          } catch (e) {
            if (options.includeQueryInError) {
              throw new Error(`Error encountered while running "${statement}": ${e.message}`);
            }
            throw new ErrorWithCause(`Error executing redshift query: ${e.message}`, e);
          }
        }
      })
    );
  }

  public async evaluate(queryOrAction: QueryOrAction, projectConfig?: dataform.ProjectConfig) {
    const validationQueries = collectEvaluationQueries(queryOrAction, false, (query: string) =>
      !!query ? `explain ${query}` : ""
    ).map((validationQuery, index) => ({ index, validationQuery }));
    const validationQueriesWithoutWrappers = collectEvaluationQueries(queryOrAction, false);

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
          error: parseRedshiftEvalError(validationQuery.query, e)
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

  public async tables(): Promise<dataform.ITarget[]> {
    const queryResult = await this.execute(
      `select table_name, table_schema
     from svv_tables
     where table_schema != 'information_schema'
       and table_schema != 'pg_catalog'
       and table_schema != 'pg_internal'`,
      { rowLimit: 10000, includeQueryInError: true }
    );
    const { rows } = queryResult;
    return rows.map(row => ({
      schema: row.table_schema,
      name: row.table_name
    }));
  }

  public async search(searchText: string): Promise<dataform.ITableMetadata[]> {
    const results = await this.execute(
      `select tables.table_schema as table_schema, tables.table_name as table_name
       from svv_tables as tables
       left join svv_columns as columns on tables.table_schema = columns.table_schema and tables.table_name = columns.table_name
       where tables.table_schema like $1 or tables.table_name like $1 or tables.remarks like $1
         or columns.column_name like $1 or columns.remarks like $1
       group by 1, 2`,
      {
        params: [`%${searchText}%`],
        rowLimit: 100
      }
    );
    return await Promise.all(
      results.rows.map(row =>
        this.table({
          schema: row.table_schema,
          name: row.table_name
        })
      )
    );
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const params = [target.schema, target.name];
    const [tableResults, columnResults] = await Promise.all([
      this.execute(
        `select table_type, remarks from svv_tables where table_schema = $1 and table_name = $2`,
        { params, includeQueryInError: true }
      ),
      this.execute(
        `select column_name, data_type, is_nullable, remarks
         from svv_columns
         where table_schema = $1 and table_name = $2`,
        { params, includeQueryInError: true }
      )
    ]);
    if (tableResults.rows.length === 0) {
      return null;
    }
    return dataform.TableMetadata.create({
      target,
      typeDeprecated: tableResults.rows[0].table_type === "VIEW" ? "view" : "table",
      type:
        tableResults.rows[0].table_type === "VIEW"
          ? dataform.TableMetadata.Type.VIEW
          : dataform.TableMetadata.Type.TABLE,
      fields: columnResults.rows.map(row =>
        dataform.Field.create({
          name: row.column_name,
          primitiveDeprecated: row.data_type,
          primitive: convertFieldType(row.data_type),
          flagsDeprecated: row.is_nullable && row.is_nullable === "YES" ? ["nullable"] : [],
          description: row.remarks
        })
      ),
      description: tableResults.rows[0].remarks
    });
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const { rows } = await this.execute(
      `SELECT * FROM "${target.schema}"."${target.name}" LIMIT ${limitRows}`
    );
    return rows;
  }

  public async schemas(): Promise<string[]> {
    const schemas = await this.execute(`select nspname from pg_namespace`, {
      includeQueryInError: true
    });
    return schemas.rows.map(row => row.nspname);
  }

  public async createSchema(_: string, schema: string): Promise<void> {
    await this.execute(`create schema if not exists "${schema}"`, { includeQueryInError: true });
  }

  public async close() {
    await this.queryExecutor.close();
    if (this.options.sshTunnel) {
      await this.options.sshTunnel.close();
    }
  }

  public async persistedStateMetadata(): Promise<dataform.IPersistedTableMetadata[]> {
    return [];
  }

  public async persistStateMetadata() {
    // Unimplemented.
  }

  public async setMetadata(action: dataform.IExecutionAction): Promise<void> {
    const { target, actionDescriptor, tableType } = action;

    const actualMetadata = await this.table(target);

    const queries: Array<Promise<any>> = [];
    if (actionDescriptor.description) {
      queries.push(
        this.execute(
          `comment on ${tableType === "view" ? "view" : "table"} "${target.schema}"."${
            target.name
          }" is '${actionDescriptor.description.replace("'", "\\'")}'`
        )
      );
    }
    if (tableType !== "view" && actionDescriptor.columns?.length > 0) {
      actionDescriptor.columns
        .filter(
          column =>
            column.path.length === 1 &&
            actualMetadata.fields.some(field => field.name === column.path[0])
        )
        .forEach(column => {
          queries.push(
            this.execute(
              `comment on column "${target.schema}"."${target.name}"."${
                column.path[0]
              }" is '${column.description.replace("'", "\\'")}'`
            )
          );
        });
    }

    await Promise.all(queries);
  }
}
