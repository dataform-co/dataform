export interface IBigQueryEngine {
  location?: string;
  impersonationChain?: string[];
  destinationTable?: string;
}

export interface IDataprocServerlessEngine {
  location?: string;
  resourceProfile?: Record<string, any>;
  impersonationChain?: string[];
}

export interface IDataprocOnGceEngine {
  existingCluster?: Record<string, any>;
  ephemeralCluster?: Record<string, any>;
}

export interface IEngineConfig {
  bigquery?: IBigQueryEngine;
  dataprocServerless?: IDataprocServerlessEngine;
  dataprocOnGce?: IDataprocOnGceEngine;
}

export interface IQueryConfig {
  inline?: string;
  path?: string;
}

export interface ISqlConfig {
  name: string;
  query?: IQueryConfig;
  engine?: IEngineConfig;
  dependsOn?: Array<string | any>;
  executionTimeout?: string | number;
}

export interface INotebookRequirements {
  path?: string;
}

export interface INotebookEnvironment {
  requirements?: INotebookRequirements;
}

export interface INotebookEngine {
  dataprocOnGce?: IDataprocOnGceEngine;
  dataprocServerless?: IDataprocServerlessEngine;
}

export interface INotebookConfig {
  name: string;
  dependsOn?: Array<string | any>;
  mainFilePath: string;
  stagingBucket?: string;
  engine?: INotebookEngine;
  archiveUris?: string[];
  environment?: INotebookEnvironment;
}

export interface IAction {
  sql?: ISqlConfig;
  notebook?: INotebookConfig;
  tags?: string[];
}

export interface IPipeline {
  actions?: IAction[];
}

