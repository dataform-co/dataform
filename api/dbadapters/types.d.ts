declare module "snowflake-sdk";

// The types for presto-client are still in an outstanding PR:
// https://github.com/tagomoris/presto-client-node/pull/29/files
// And a discussion about it here: https://github.com/tagomoris/presto-client-node/issues/28
// Some tweaking has been done to make optional parameters actually optional.
declare module "presto-client" {
  interface IPrestoClientOptions {
    host: string;
    port: number;
    user: string;
    ssl?: {
      ca?: string;
      cert: string;
      ciphers?: string;
      key?: string;
      passphrase?: string;
      secureProtocol: string;
      pfx?: string;
      rejectUnauthorized?: boolean;
    };
    source?: string;
    catalog?: string;
    checkInterval?: number;
    basic_auth?: {
      user: string;
      password: string;
    };
    schema?: string;
    enableVerboseStateCallback?: boolean;
  }

  enum PrestoClientQueryStates {
    QUEUED = "QUEUED",
    PLANNING = "PLANNING",
    STARTING = "STARTING",
    RUNNING = "RUNNING",
    FINISHED = "FINISHED",
    CANCELED = "CANCELED",
    FAILED = "FAILED"
  }

  interface IPrestoClientStats {
    processedBytes: number;
    processedRows: number;
    wallTimeMillis: number;
    cpuTimeMillis: number;
    userTimeMillis: number;
    state: PrestoClientQueryStates;
    scheduled: boolean;
    nodes: number;
    totalSplits: number;
    queuedSplits: number;
    runningSplits: number;
    completedSplits: number;
  }

  // NOTE: Needs to be extended with missing items
  enum PrestoClientPrestoTypes {
    varchar = "varchar",
    bigint = "bigint",
    boolean = "boolean",
    char = "char",
    date = "date",
    decimal = "decimal",
    double = "double"
  }

  interface IPrestoClientColumnMetaData {
    name: string;
    type: PrestoClientPrestoTypes;
  }

  type PrestoClientColumnDatum = any[];

  interface IPrestoClientExecuteOptions {
    query: string;
    catalog?: string;
    schema?: string;
    timezone?: string;
    info?: boolean;
    cancel?: () => boolean;
    state?: (error: IPrestoClientError, query_id: string, stats: IPrestoClientStats) => void;
    columns?: (error: IPrestoClientError, columns: IPrestoClientColumnMetaData[]) => void;
    data?: (
      error: IPrestoClientError,
      data: any[],
      columns: IPrestoClientColumnMetaData[],
      stats: IPrestoClientStats
    ) => void;
    success?: (error: IPrestoClientError, stats: IPrestoClientStats) => void;
    error?: (error: IPrestoClientError) => void;
  }

  interface IPrestoErrorLocation {
    lineNumber: number;
    columnNumber: number;
  }

  interface IPrestoClientError {
    message?: string;
    errorCode?: number;
    errorName?: string;
    errorType?: string;
    errorLocation?: IPrestoErrorLocation;
    failureInfo?: {
      type?: string;
      message?: string;
      suppressed?: string[];
      stack?: string[];
      errorLocation?: IPrestoErrorLocation;
    };
  }

  class Client {
    constructor(options: IPrestoClientOptions);
    public execute(options: IPrestoClientExecuteOptions): void;
    public query(query_id: string, callback: (error: IPrestoClientError, data?: any) => void): void;
    public kill(query_id: string, callback: (error: IPrestoClientError) => void): void;
    public nodes(
      opts: null | undefined | {},
      callback: (error: IPrestoClientError, data: {}[]) => void
    ): void;
  }
}
