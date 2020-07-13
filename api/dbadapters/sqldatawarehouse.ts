import { ConnectionPool } from "mssql";

import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IExecutionResult, OnCancel } from "df/api/dbadapters/index";
import { parseAzureEvaluationError } from "df/api/utils/error_parsing";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

const INFORMATION_SCHEMA_SCHEMA_NAME = "information_schema";
const TABLE_NAME_COL_NAME = "table_name";
const TABLE_SCHEMA_COL_NAME = "table_schema";
const TABLE_TYPE_COL_NAME = "table_type";
const COLUMN_NAME_COL_NAME = "column_name";
const DATA_TYPE_COL_NAME = "data_type";
const IS_NULLABLE_COL_NAME = "is_nullable";
const DB_CONNECTION_TIMEOUT_MILLIS = 5 * 60 * 1000; // 5 minute connection timeout
const DB_REQUEST_TIMEOUT_MILLIS = 1 * 60 * 60 * 1000; // 1 hour request timeout
const DB_CON_LIMIT = 10; // mssql default value of 10 concurrent requests

export class SQLDataWarehouseDBAdapter implements IDbAdapter {
  public static async create(
    credentials: Credentials,
    _: string,
    options?: { concurrencyLimit?: number }
  ) {
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
      onCancel?: OnCancel;
      maxResults?: number;
    } = { maxResults: 1000 }
  ): Promise<IExecutionResult> {
    const request = (await this.pool).request();
    options?.onCancel?.(() => request.cancel());

    return await new Promise<IExecutionResult>((resolve, reject) => {
      request.stream = true;

      const rows: any[] = [];

      request
        .on("row", row => {
          if (options && options.maxResults && rows.length >= options.maxResults) {
            request.cancel();
            resolve({ rows, metadata: {} });
            return;
          }
          rows.push(row);
        })
        .on("error", err => reject(err))
        .on("done", () => resolve({ rows, metadata: {} }));

      // tslint:disable-next-line: no-floating-promises
      request.query(statement);
    });
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
    const {
      rows
    } = await this.execute(
      `select ${TABLE_SCHEMA_COL_NAME}, ${TABLE_NAME_COL_NAME} from ${INFORMATION_SCHEMA_SCHEMA_NAME}.tables`,
      { maxResults: 10000 }
    );
    return rows.map(row => ({
      schema: row[TABLE_SCHEMA_COL_NAME],
      name: row[TABLE_NAME_COL_NAME]
    }));
  }

  public async table(target: dataform.ITarget): Promise<dataform.ITableMetadata> {
    const [tableData, columnData] = await Promise.all([
      this.execute(
        `select ${TABLE_TYPE_COL_NAME} from ${INFORMATION_SCHEMA_SCHEMA_NAME}.tables
          where ${TABLE_SCHEMA_COL_NAME} = '${target.schema}' AND ${TABLE_NAME_COL_NAME} = '${target.name}'`
      ),
      this.execute(
        `select ${COLUMN_NAME_COL_NAME}, ${DATA_TYPE_COL_NAME}, ${IS_NULLABLE_COL_NAME}
       from ${INFORMATION_SCHEMA_SCHEMA_NAME}.columns
       where ${TABLE_SCHEMA_COL_NAME} = '${target.schema}' AND ${TABLE_NAME_COL_NAME} = '${target.name}'`
      )
    ]);

    if (tableData.rows.length === 0) {
      return null;
    }

    // The table exists.
    return {
      target,
      typeDeprecated: tableData.rows[0][TABLE_TYPE_COL_NAME] === "VIEW" ? "view" : "table",
      type:
        tableData.rows[0][TABLE_TYPE_COL_NAME] === "VIEW"
          ? dataform.TableMetadata.Type.VIEW
          : dataform.TableMetadata.Type.TABLE,
      fields: columnData.rows.map(row => ({
        name: row[COLUMN_NAME_COL_NAME],
        primitiveDeprecated: row[DATA_TYPE_COL_NAME],
        primitive: convertFieldType(row[DATA_TYPE_COL_NAME]),
        flagsDeprecated:
          row[IS_NULLABLE_COL_NAME] && row[IS_NULLABLE_COL_NAME] === "YES" ? ["nullable"] : []
      }))
    };
  }

  public async preview(target: dataform.ITarget, limitRows: number = 10): Promise<any[]> {
    const { rows } = await this.execute(
      `SELECT TOP ${limitRows} * FROM "${target.schema}"."${target.name}"`
    );
    return rows;
  }

  public async prepareSchema(database: string, schema: string): Promise<void> {
    await this.execute(
      `if not exists ( select schema_name from ${INFORMATION_SCHEMA_SCHEMA_NAME}.schemata where schema_name = '${schema}' )
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
