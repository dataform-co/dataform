import { ConnectionPool } from "mssql";

import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IDbClient, IExecutionResult, OnCancel } from "df/api/dbadapters/index";
import { parseAzureEvaluationError } from "df/api/utils/error_parsing";
import { LimitedResultSet } from "df/api/utils/results";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

const DB_CONNECTION_TIMEOUT_MILLIS = 5 * 60 * 1000; // 5 minute connection timeout
const DB_REQUEST_TIMEOUT_MILLIS = 1 * 60 * 60 * 1000; // 1 hour request timeout
const DB_CON_LIMIT = 10; // mssql default value of 10 concurrent requests

export class SQLDataWarehouseDBAdapter implements IDbAdapter {
  public static async create(credentials: Credentials, options?: { concurrencyLimit?: number }) {
    return new SQLDataWarehouseDBAdapter(credentials, options);
  }

  private pool: Promise<ConnectionPool>;

  constructor(credentials: Credentials, options?: { concurrencyLimit?: number }) {
    const sqlDataWarehouseCredentials = credentials as dataform.ISQLDataWarehouse;
    this.pool = new Promise((resolve, reject) => {
      const conn = new ConnectionPool({
        server: sqlDataWarehouseCredentials.server,
        port: sqlDataWarehouseCredentials.port,
        user: sqlDataWarehouseCredentials.username,
        password: sqlDataWarehouseCredentials.password,
        database: sqlDataWarehouseCredentials.database,
        connectionTimeout: DB_CONNECTION_TIMEOUT_MILLIS,
        requestTimeout: DB_REQUEST_TIMEOUT_MILLIS,
        pool: {
          min: 0,
          max: options?.concurrencyLimit || DB_CON_LIMIT
        },
        options: {
          encrypt: true
        }
      }).connect();
      conn
        .then(pool => {
          pool.on("error", err => {
            throw new Error(err);
          });
          resolve(conn);
        })
        .catch(e => reject(e));
    });
  }

  public async execute(
    statement: string,
    options: {
      params?: { [name: string]: any };
      onCancel?: OnCancel;
      rowLimit?: number;
      byteLimit?: number;
    } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
  ): Promise<IExecutionResult> {
    const request = (await this.pool).request();
    options?.onCancel?.(() => request.cancel());

    return await new Promise<IExecutionResult>((resolve, reject) => {
      request.stream = true;

      const results = new LimitedResultSet({
        rowLimit: options?.rowLimit,
        byteLimit: options?.byteLimit
      });

      request
        .on("row", row => {
          if (!results.push(row)) {
            request.cancel();
            resolve({ rows: results.rows, metadata: {} });
          }
        })
        .on("error", err => reject(err))
        .on("done", () => resolve({ rows: results.rows, metadata: {} }));

      for (const [name, value] of Object.entries(options?.params || {})) {
        request.input(name, value);
      }
      // tslint:disable-next-line: no-floating-promises
      request.query(statement);
    });
  }

  public async withClientLock<T>(callback: (client: IDbClient) => Promise<T>) {
    return await callback(this);
  }

  public async evaluate(queryOrAction: QueryOrAction, projectConfig?: dataform.ProjectConfig) {
    // TODO: Using `explain` before declaring a variable is not valid in SQL Data Warehouse.
    const validationQueries = collectEvaluationQueries(
      queryOrAction,
      projectConfig?.useSingleQueryPerAction === undefined ||
        !!projectConfig?.useSingleQueryPerAction,
      (query: string) => (!!query ? `explain ${query}` : "")
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
          error: parseAzureEvaluationError(e)
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
    const { rows } = await this.execute(
      `select table_schema, table_name from information_schema.tables`,
      {
        rowLimit: 10000
      }
    );
    return rows.map(row => ({
      schema: row.table_schema,
      name: row.table_name
    }));
  }

  public async search(
    searchText: string,
    options: { limit: number } = { limit: 1000 }
  ): Promise<dataform.ITableMetadata[]> {
    const results = await this.execute(
      `select tables.table_schema as table_schema, tables.table_name as table_name
       from information_schema.tables as tables
       left join information_schema.columns as columns on tables.table_schema = columns.table_schema and tables.table_name = columns.table_name
       where tables.table_schema like @searchText or tables.table_name like @searchText or columns.column_name like @searchText
       group by tables.table_schema, tables.table_name`,
      {
        params: {
          searchText: `%${searchText}%`
        },
        rowLimit: options.limit
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
    const queryParams = {
      schema: target.schema,
      name: target.name
    };
    const [tableData, columnData] = await Promise.all([
      this.execute(
        `select table_type from information_schema.tables
         where table_schema = @schema AND table_name = @name`,
        {
          params: queryParams
        }
      ),
      this.execute(
        `select column_name, data_type, is_nullable
         from information_schema.columns
         where table_schema = @schema AND table_name = @name`,
        {
          params: queryParams
        }
      )
    ]);

    if (tableData.rows.length === 0) {
      return null;
    }

    // The table exists.
    return {
      target,
      typeDeprecated: tableData.rows[0].table_type === "VIEW" ? "view" : "table",
      type:
        tableData.rows[0].table_type === "VIEW"
          ? dataform.TableMetadata.Type.VIEW
          : dataform.TableMetadata.Type.TABLE,
      fields: columnData.rows.map(row => ({
        name: row.column_name,
        primitive: convertFieldType(row.data_type),
        flagsDeprecated: row.is_nullable && row.is_nullable === "YES" ? ["nullable"] : []
      }))
    };
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const { rows } = await this.execute(
      `SELECT TOP ${limitRows} * FROM "${target.schema}"."${target.name}"`
    );
    return rows;
  }

  public async schemas(): Promise<string[]> {
    const schemas = await this.execute(`select schema_name from information_schema.schemata`);
    return schemas.rows.map(row => row.schema_name);
  }

  public async createSchema(_: string, schema: string): Promise<void> {
    await this.execute(
      `if not exists ( select schema_name from information_schema.schemata where schema_name = '${schema}' )
            begin
              exec sp_executesql N'create schema ${schema}'
            end `
    );
  }

  public async close() {
    await (await this.pool).close();
  }

  public async persistedStateMetadata(): Promise<dataform.IPersistedTableMetadata[]> {
    return [];
  }

  public async persistStateMetadata() {
    // Unimplemented.
  }

  public async setMetadata(): Promise<void> {
    // Unimplemented.
  }
}

// See: https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-ver15
function convertFieldType(type: string) {
  switch (String(type).toUpperCase()) {
    case "FLOAT":
    case "REAL":
      return dataform.Field.Primitive.FLOAT;
    case "INT":
    case "BIGINT":
    case "SMALLINT":
    case "TINYINT":
      return dataform.Field.Primitive.INTEGER;
    case "DECIMAL":
    case "NUMERIC":
      return dataform.Field.Primitive.NUMERIC;
    case "BIT":
      return dataform.Field.Primitive.BOOLEAN;
    case "VARCHAR":
    case "CHAR":
    case "TEXT":
    case "NVARCHAR":
    case "NCHAR":
    case "NTEXT":
      return dataform.Field.Primitive.STRING;
    case "DATE":
      return dataform.Field.Primitive.DATE;
    case "DATETIME":
    case "DATETIME2":
    case "DATETIMEOFFSET":
    case "SMALLDATETIME":
      return dataform.Field.Primitive.DATETIME;
    case "TIME":
      return dataform.Field.Primitive.TIME;
    case "BINARY":
    case "VARBINARY":
    case "IMAGE":
      return dataform.Field.Primitive.BYTES;
    default:
      return dataform.Field.Primitive.UNKNOWN;
  }
}
