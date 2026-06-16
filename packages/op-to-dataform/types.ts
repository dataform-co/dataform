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

export interface IAirflowOperatorConfig {
  name: string;
  dependsOn?: Array<string | any>;
  executionTimeout?: string;
  operatorClass: string;
  params?: Record<string, any>;
  stagingBucket?: string;
}

export interface IAction {
  sql?: ISqlConfig;
  notebook?: INotebookConfig;
  airflowOperator?: IAirflowOperatorConfig;
  tags?: string[];
}

export interface IExecutionConfig {
  retries?: number;
}

export interface IDefaults {
  projectId: string;
  location: string;
  executionConfig?: IExecutionConfig;
  stagingBucket?: string;
  runtimeTemplateName?: string;
}

export interface IPipeline {
  actions?: IAction[];
  defaults?: IDefaults;
}


