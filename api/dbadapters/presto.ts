import Long from "long";
import * as Presto from "presto-client";

import { BigQuery, TableField, TableMetadata } from "@google-cloud/bigquery";
import { Credentials } from "df/api/commands/credentials";
import { IDbAdapter, IDbClient, IExecutionResult, OnCancel } from "df/api/dbadapters/index";
import { parseBigqueryEvalError } from "df/api/utils/error_parsing";
import { LimitedResultSet } from "df/api/utils/results";
import {
  decodePersistedTableMetadata,
  encodePersistedTableMetadata,
  hashExecutionAction,
  IMetadataRow,
  toRowKey
} from "df/api/utils/run_cache";
import { coerceAsError } from "df/common/errors/errors";
import { StringifiedMap } from "df/common/strings/stringifier";
import { collectEvaluationQueries, QueryOrAction } from "df/core/adapters";
import { dataform } from "df/protos/ts";

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
    // TODO: Should these execut options actually be of type Presto.IPrestoClientExecuteOptions?
    options: {
      onCancel?: OnCancel;
      rowLimit?: number;
      byteLimit?: number;
    } = { rowLimit: 1000, byteLimit: 1024 * 1024 }
  ): Promise<IExecutionResult> {
    const result: IExecutionResult = {};
    // Promise<IPrestoExecutionResult>
    return new Promise((resolve, reject) => {
      this.client.execute({
        query: statement,
        // TODO: Add catalog and schema here to allow cross-database querying.
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
}
