# Protocol Documentation

<a name="top"></a>

## Table of Contents

- [configs.proto](#configs-proto)

  - [ActionConfig](#dataform-ActionConfig)
  - [ActionConfig.AssertionConfig](#dataform-ActionConfig-AssertionConfig)
  - [ActionConfig.AssertionConfig.JobLabelsEntry](#dataform-ActionConfig-AssertionConfig-JobLabelsEntry)
  - [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor)
  - [ActionConfig.DataPreparationConfig](#dataform-ActionConfig-DataPreparationConfig)
  - [ActionConfig.DataPreparationConfig.JobLabelsEntry](#dataform-ActionConfig-DataPreparationConfig-JobLabelsEntry)
  - [ActionConfig.DeclarationConfig](#dataform-ActionConfig-DeclarationConfig)
  - [ActionConfig.IncrementalTableConfig](#dataform-ActionConfig-IncrementalTableConfig)
  - [ActionConfig.IncrementalTableConfig.AdditionalOptionsEntry](#dataform-ActionConfig-IncrementalTableConfig-AdditionalOptionsEntry)
  - [ActionConfig.IncrementalTableConfig.JobLabelsEntry](#dataform-ActionConfig-IncrementalTableConfig-JobLabelsEntry)
  - [ActionConfig.IncrementalTableConfig.LabelsEntry](#dataform-ActionConfig-IncrementalTableConfig-LabelsEntry)
  - [ActionConfig.NotebookConfig](#dataform-ActionConfig-NotebookConfig)
  - [ActionConfig.OnSchemaChange](#dataform-ActionConfig-OnSchemaChange)
  - [ActionConfig.OperationConfig](#dataform-ActionConfig-OperationConfig)
  - [ActionConfig.OperationConfig.JobLabelsEntry](#dataform-ActionConfig-OperationConfig-JobLabelsEntry)
  - [ActionConfig.TableAssertionsConfig](#dataform-ActionConfig-TableAssertionsConfig)
  - [ActionConfig.TableAssertionsConfig.UniqueKey](#dataform-ActionConfig-TableAssertionsConfig-UniqueKey)
  - [ActionConfig.TableConfig](#dataform-ActionConfig-TableConfig)
  - [ActionConfig.TableConfig.AdditionalOptionsEntry](#dataform-ActionConfig-TableConfig-AdditionalOptionsEntry)
  - [ActionConfig.TableConfig.JobLabelsEntry](#dataform-ActionConfig-TableConfig-JobLabelsEntry)
  - [ActionConfig.TableConfig.LabelsEntry](#dataform-ActionConfig-TableConfig-LabelsEntry)
  - [ActionConfig.Target](#dataform-ActionConfig-Target)
  - [ActionConfig.ViewConfig](#dataform-ActionConfig-ViewConfig)
  - [ActionConfig.ViewConfig.AdditionalOptionsEntry](#dataform-ActionConfig-ViewConfig-AdditionalOptionsEntry)
  - [ActionConfig.ViewConfig.JobLabelsEntry](#dataform-ActionConfig-ViewConfig-JobLabelsEntry)
  - [ActionConfig.ViewConfig.LabelsEntry](#dataform-ActionConfig-ViewConfig-LabelsEntry)
  - [ActionConfigs](#dataform-ActionConfigs)
  - [LineageSettings](#dataform-LineageSettings)
  - [NotebookRuntimeOptionsConfig](#dataform-NotebookRuntimeOptionsConfig)
  - [WorkflowSettings](#dataform-WorkflowSettings)
  - [WorkflowSettings.DefaultJobLabelsEntry](#dataform-WorkflowSettings-DefaultJobLabelsEntry)
  - [WorkflowSettings.VarsEntry](#dataform-WorkflowSettings-VarsEntry)

- [property_graph_config.proto](#property_graph_config-proto)

  - [DataSourceRef](#dataform-DataSourceRef)
  - [GraphAiContextConfig](#dataform-GraphAiContextConfig)
  - [GraphCustomExtensionConfig](#dataform-GraphCustomExtensionConfig)
  - [GraphDeploymentConfig](#dataform-GraphDeploymentConfig)
  - [GraphDimensionConfig](#dataform-GraphDimensionConfig)
  - [GraphEntityConfig](#dataform-GraphEntityConfig)
  - [GraphFieldConfig](#dataform-GraphFieldConfig)
  - [GraphMetricConfig](#dataform-GraphMetricConfig)
  - [GraphRelationshipConfig](#dataform-GraphRelationshipConfig)
  - [GraphUniqueKeyList](#dataform-GraphUniqueKeyList)
  - [PropertyGraphConfig](#dataform-PropertyGraphConfig)
  - [PropertyGraphConfig.JobLabelsEntry](#dataform-PropertyGraphConfig-JobLabelsEntry)
  - [SemanticModelConfig](#dataform-SemanticModelConfig)

- [Scalar Value Types](#scalar-value-types)

<a name="configs-proto"></a>

<p align="right"><a href="#top">Top</a></p>

## configs.proto

<a name="dataform-ActionConfig"></a>

### ActionConfig

Action config defines the contents of `actions.yaml` configuration files.

| Field            | Type                                                                                 | Label | Description                                                       |
| ---------------- | ------------------------------------------------------------------------------------ | ----- | ----------------------------------------------------------------- |
| table            | [ActionConfig.TableConfig](#dataform-ActionConfig-TableConfig)                       |       |                                                                   |
| view             | [ActionConfig.ViewConfig](#dataform-ActionConfig-ViewConfig)                         |       |                                                                   |
| incrementalTable | [ActionConfig.IncrementalTableConfig](#dataform-ActionConfig-IncrementalTableConfig) |       |                                                                   |
| assertion        | [ActionConfig.AssertionConfig](#dataform-ActionConfig-AssertionConfig)               |       |                                                                   |
| operation        | [ActionConfig.OperationConfig](#dataform-ActionConfig-OperationConfig)               |       |                                                                   |
| declaration      | [ActionConfig.DeclarationConfig](#dataform-ActionConfig-DeclarationConfig)           |       |                                                                   |
| notebook         | [ActionConfig.NotebookConfig](#dataform-ActionConfig-NotebookConfig)                 |       |                                                                   |
| dataPreparation  | [ActionConfig.DataPreparationConfig](#dataform-ActionConfig-DataPreparationConfig)   |       |                                                                   |
| propertyGraph    | [PropertyGraphConfig](#dataform-PropertyGraphConfig)                                 |       | Not supported in `actions.yaml`; use `definitions/**/graph.yaml`. |

<a name="dataform-ActionConfig-AssertionConfig"></a>

### ActionConfig.AssertionConfig

| Field                        | Type                                                 | Label    | Description                                                                                                                                                                                                                     |
| ---------------------------- | ---------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                         | [string](#string)                                    |          | The name of the assertion.                                                                                                                                                                                                      |
| dataset                      | [string](#string)                                    |          | The dataset (schema) of the assertion.                                                                                                                                                                                          |
| project                      | [string](#string)                                    |          | The Google Cloud project (database) of the assertion.                                                                                                                                                                           |
| dependencyTargets            | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on.                                                                                                                                                                            |
| filename                     | [string](#string)                                    |          | Path to the source file that the contents of the action is loaded from.                                                                                                                                                         |
| tags                         | [string](#string)                                    | repeated | A list of user-defined tags with which the action should be labeled.                                                                                                                                                            |
| disabled                     | [bool](#bool)                                        |          | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions.                                                                            |
| description                  | [string](#string)                                    |          | Description of the assertion.                                                                                                                                                                                                   |
| hermetic                     | [bool](#bool)                                        |          | If true, this indicates that the action only depends on data from explicitly-declared dependencies. Otherwise if false, it indicates that the action depends on data from a source which has not been declared as a dependency. |
| dependOnDependencyAssertions | [bool](#bool)                                        |          | If true, assertions dependent upon any of the dependencies are added as dependencies as well.                                                                                                                                   |
| reservation          | [string](#string)                                    |          | Optional. The BigQuery reservation to use for execution.                                                                                                                                                                        |
| jobLabels                    | [ActionConfig.AssertionConfig.JobLabelsEntry](#dataform-ActionConfig-AssertionConfig-JobLabelsEntry) | repeated | Optional. Key-value pairs for BigQuery job labels applied to the query job(s) executed for this assertion. Overrides matching keys from `defaultJobLabels` in `workflow_settings.yaml`. See `defaultJobLabels` for the allowed keys and values. |

<a name="dataform-ActionConfig-AssertionConfig-JobLabelsEntry"></a>

### ActionConfig.AssertionConfig.JobLabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-ColumnDescriptor"></a>

### ActionConfig.ColumnDescriptor

| Field              | Type              | Label    | Description                                                             |
| ------------------ | ----------------- | -------- | ----------------------------------------------------------------------- |
| path               | [string](#string) | repeated | The identifier for the column, using multiple parts for nested records. |
| description        | [string](#string) |          | A text description of the column.                                       |
| bigqueryPolicyTags | [string](#string) | repeated | A list of BigQuery policy tags that will be applied to the column.      |
| tags               | [string](#string) | repeated | A list of tags for this column which will be applied.                   |

<a name="dataform-ActionConfig-DataPreparationConfig"></a>

### ActionConfig.DataPreparationConfig

| Field               | Type                                                 | Label    | Description                                                                                                                                          |
| ------------------- | ---------------------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                | [string](#string)                                    |          | The name of the data preparation.                                                                                                                    |
| dependencyTargets   | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on.                                                                                                 |
| filename            | [string](#string)                                    |          | Path to the source file that the contents of the action is loaded from.                                                                              |
| tags                | [string](#string)                                    | repeated | A list of user-defined tags with which the action should be labeled.                                                                                 |
| disabled            | [bool](#bool)                                        |          | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions. |
| description         | [string](#string)                                    |          | Description of the data preparation.                                                                                                                 |
| reservation | [string](#string)                                    |          | Optional. The BigQuery reservation to use for execution.                                                                                             |
| jobLabels           | [ActionConfig.DataPreparationConfig.JobLabelsEntry](#dataform-ActionConfig-DataPreparationConfig-JobLabelsEntry) | repeated | Optional. Key-value pairs for BigQuery job labels applied to the BigQuery job(s) executed for this data preparation. Overrides matching keys from `defaultJobLabels` in `workflow_settings.yaml`. See `defaultJobLabels` for the allowed keys and values. |

<a name="dataform-ActionConfig-DataPreparationConfig-JobLabelsEntry"></a>

### ActionConfig.DataPreparationConfig.JobLabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-DeclarationConfig"></a>

### ActionConfig.DeclarationConfig

| Field       | Type                                                                     | Label    | Description                                             |
| ----------- | ------------------------------------------------------------------------ | -------- | ------------------------------------------------------- |
| name        | [string](#string)                                                        |          | The name of the declaration.                            |
| dataset     | [string](#string)                                                        |          | The dataset (schema) of the declaration.                |
| project     | [string](#string)                                                        |          | The Google Cloud project (database) of the declaration. |
| description | [string](#string)                                                        |          | Description of the declaration.                         |
| columns     | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor) | repeated | Descriptions of columns within the declaration.         |

<a name="dataform-ActionConfig-IncrementalTableConfig"></a>

### ActionConfig.IncrementalTableConfig

| Field                        | Type                                                                                                                               | Label    | Description                                                                                                                                                                                                                     |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                         | [string](#string)                                                                                                                  |          | The name of the incremental table.                                                                                                                                                                                              |
| dataset                      | [string](#string)                                                                                                                  |          | The dataset (schema) of the incremental table.                                                                                                                                                                                  |
| project                      | [string](#string)                                                                                                                  |          | The Google Cloud project (database) of the incremental table.                                                                                                                                                                   |
| dependencyTargets            | [ActionConfig.Target](#dataform-ActionConfig-Target)                                                                               | repeated | Targets of actions that this action is dependent on.                                                                                                                                                                            |
| filename                     | [string](#string)                                                                                                                  |          | Path to the source file that the contents of the action is loaded from.                                                                                                                                                         |
| tags                         | [string](#string)                                                                                                                  | repeated | A list of user-defined tags with which the action should be labeled.                                                                                                                                                            |
| disabled                     | [bool](#bool)                                                                                                                      |          | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions.                                                                            |
| preOperations                | [string](#string)                                                                                                                  | repeated | Queries to run before `query`. This can be useful for granting permissions.                                                                                                                                                     |
| postOperations               | [string](#string)                                                                                                                  | repeated | Queries to run after `query`.                                                                                                                                                                                                   |
| protected                    | [bool](#bool)                                                                                                                      |          | If true, prevents the dataset from being rebuilt from scratch.                                                                                                                                                                  |
| uniqueKey                    | [string](#string)                                                                                                                  | repeated | If set, unique key represents a set of names of columns that will act as a the unique key. To enforce this, when updating the incremental table, Dataform merges rows with `uniqueKey` instead of appending them.               |
| description                  | [string](#string)                                                                                                                  |          | Description of the incremental table.                                                                                                                                                                                           |
| columns                      | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor)                                                           | repeated | Descriptions of columns within the table.                                                                                                                                                                                       |
| partitionBy                  | [string](#string)                                                                                                                  |          | The key by which to partition the table. Typically the name of a timestamp or the date column. See https://cloud.google.com/dataform/docs/partitions-clusters.                                                                  |
| partitionExpirationDays      | [int32](#int32)                                                                                                                    |          | The number of days for which BigQuery stores data in each partition. The setting applies to all partitions in a table, but is calculated independently for each partition based on the partition time.                          |
| requirePartitionFilter       | [bool](#bool)                                                                                                                      |          | Declares whether the partitioned table requires a WHERE clause predicate filter that filters the partitioning column.                                                                                                           |
| updatePartitionFilter        | [string](#string)                                                                                                                  |          | SQL-based filter for when incremental updates are applied.                                                                                                                                                                      |
| incrementalPredicates        | [string](#string)                                                                                                                  | repeated | Array of SQL-based predicates for when incremental updates are applied.                                                                                                                                                         |
| clusterBy                    | [string](#string)                                                                                                                  | repeated | The keys by which to cluster partitions by. See https://cloud.google.com/dataform/docs/partitions-clusters.                                                                                                                     |
| labels                       | [ActionConfig.IncrementalTableConfig.LabelsEntry](#dataform-ActionConfig-IncrementalTableConfig-LabelsEntry)                       | repeated | Key-value pairs for BigQuery labels.                                                                                                                                                                                            |
| additionalOptions            | [ActionConfig.IncrementalTableConfig.AdditionalOptionsEntry](#dataform-ActionConfig-IncrementalTableConfig-AdditionalOptionsEntry) | repeated | Key-value pairs of additional options to pass to the BigQuery API. Some options, for example, partitionExpirationDays, have dedicated type/validity checked fields. For such options, use the dedicated fields.                 |
| dependOnDependencyAssertions | [bool](#bool)                                                                                                                      |          | When set to true, assertions dependent upon any dependency will be add as dedpendency to this action                                                                                                                            |
| assertions                   | [ActionConfig.TableAssertionsConfig](#dataform-ActionConfig-TableAssertionsConfig)                                                 |          | Assertions to be run on the dataset. If configured, relevant assertions will automatically be created and run as a dependency of this dataset.                                                                                  |
| hermetic                     | [bool](#bool)                                                                                                                      |          | If true, this indicates that the action only depends on data from explicitly-declared dependencies. Otherwise if false, it indicates that the action depends on data from a source which has not been declared as a dependency. |
| onSchemaChange               | [ActionConfg.OnSchemaChange](#dataform-ActionConfig-OnSchemaChange)                                                                |          | Defines the action behavior if the selected columns in query doesn't match columns in the target table.                                                                                                                         |
| reservation          | [string](#string)                                                                                                                  |          | Optional. The BigQuery reservation to use for execution.                                                                                                                                                                        |
| jobLabels                    | [ActionConfig.IncrementalTableConfig.JobLabelsEntry](#dataform-ActionConfig-IncrementalTableConfig-JobLabelsEntry)                 | repeated | Optional. Key-value pairs for BigQuery job labels applied to the query job(s) executed for this incremental table and its built-in assertions. Overrides matching keys from `defaultJobLabels` in `workflow_settings.yaml`. When the table has any job labels, including defaults, the Dataform CLI doesn't also apply its `labels` to its jobs. See `defaultJobLabels` for the allowed keys and values. |

<a name="dataform-ActionConfig-IncrementalTableConfig-AdditionalOptionsEntry"></a>

### ActionConfig.IncrementalTableConfig.AdditionalOptionsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-IncrementalTableConfig-JobLabelsEntry"></a>

### ActionConfig.IncrementalTableConfig.JobLabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-IncrementalTableConfig-LabelsEntry"></a>

### ActionConfig.IncrementalTableConfig.LabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-NotebookConfig"></a>

### ActionConfig.NotebookConfig

| Field                        | Type                                                 | Label    | Description                                                                                                                                          |
| ---------------------------- | ---------------------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                         | [string](#string)                                    |          | The name of the notebook.                                                                                                                            |
| location                     | [string](#string)                                    |          | The Google Cloud location of the notebook.                                                                                                           |
| project                      | [string](#string)                                    |          | The Google Cloud project (database) of the notebook.                                                                                                 |
| dependencyTargets            | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on.                                                                                                 |
| filename                     | [string](#string)                                    |          | Path to the source file that the contents of the action is loaded from.                                                                              |
| tags                         | [string](#string)                                    | repeated | A list of user-defined tags with which the action should be labeled.                                                                                 |
| disabled                     | [bool](#bool)                                        |          | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions. |
| description                  | [string](#string)                                    |          | Description of the notebook.                                                                                                                         |
| dependOnDependencyAssertions | [bool](#bool)                                        |          | When set to true, assertions dependent upon any dependency will be add as dedpendency to this action                                                 |
| reservation          | [string](#string)                                    |          | Optional. The BigQuery reservation to use for execution.                                                                                             |

<a name="dataform-ActionConfig-OnSchemaChange"></a>

### ActionConfig.OnSchemaChange

| Value       | Description                                                                                                                           |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| IGNORE      | New columns are ignored. Fails if columns are deleted or renamed. _Default value_.                                                    |
| FAIL        | Fails if the query would result in a new column(s) being added, deleted, or renamed.                                                  |
| EXTEND      | New columns will be added to the target table. Fails if columns are deleted or renamed.                                               |
| SYNCHRONIZE | Does not block any new column(s) from being added, deleted or renamed. Partitioned or clustered columns cannot be deleted or renamed. |

<a name="dataform-ActionConfig-OperationConfig"></a>

### ActionConfig.OperationConfig

| Field                        | Type                                                                     | Label    | Description                                                                                                                                                                                                                     |
| ---------------------------- | ------------------------------------------------------------------------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                         | [string](#string)                                                        |          | The name of the operation.                                                                                                                                                                                                      |
| dataset                      | [string](#string)                                                        |          | The dataset (schema) of the operation.                                                                                                                                                                                          |
| project                      | [string](#string)                                                        |          | The Google Cloud project (database) of the operation.                                                                                                                                                                           |
| dependencyTargets            | [ActionConfig.Target](#dataform-ActionConfig-Target)                     | repeated | Targets of actions that this action is dependent on.                                                                                                                                                                            |
| filename                     | [string](#string)                                                        |          | Path to the source file that the contents of the action is loaded from.                                                                                                                                                         |
| tags                         | [string](#string)                                                        | repeated | A list of user-defined tags with which the action should be labeled.                                                                                                                                                            |
| disabled                     | [bool](#bool)                                                            |          | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions.                                                                            |
| hasOutput                    | [bool](#bool)                                                            |          | Declares that this action creates a dataset which should be referenceable as a dependency target, for example by using the `ref` function.                                                                                      |
| description                  | [string](#string)                                                        |          | Description of the operation.                                                                                                                                                                                                   |
| columns                      | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor) | repeated | Descriptions of columns within the operation. Can only be set if hasOutput is true.                                                                                                                                             |
| dependOnDependencyAssertions | [bool](#bool)                                                            |          | When set to true, assertions dependent upon any dependency will be add as dedpendency to this action                                                                                                                            |
| hermetic                     | [bool](#bool)                                                            |          | If true, this indicates that the action only depends on data from explicitly-declared dependencies. Otherwise if false, it indicates that the action depends on data from a source which has not been declared as a dependency. |
| reservation          | [string](#string)                                                        |          | Optional. The BigQuery reservation to use for execution.                                                                                                                                                                        |
| jobLabels                    | [ActionConfig.OperationConfig.JobLabelsEntry](#dataform-ActionConfig-OperationConfig-JobLabelsEntry)     | repeated | Optional. Key-value pairs for BigQuery job labels applied to the query job(s) executed for this operation. Overrides matching keys from `defaultJobLabels` in `workflow_settings.yaml`. See `defaultJobLabels` for the allowed keys and values. |

<a name="dataform-ActionConfig-OperationConfig-JobLabelsEntry"></a>

### ActionConfig.OperationConfig.JobLabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-TableAssertionsConfig"></a>

### ActionConfig.TableAssertionsConfig

Options for shorthand specifying assertions, useable for some table-based
action types.

| Field         | Type                                                                                                   | Label    | Description                                                                                                                                                                                            |
| ------------- | ------------------------------------------------------------------------------------------------------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| uniqueKey     | [string](#string)                                                                                      | repeated | Column(s) which constitute the dataset&#39;s unique key index. If set, the resulting assertion will fail if there is more than one row in the dataset with the same values for all of these column(s). |
| uniqueKeys    | [ActionConfig.TableAssertionsConfig.UniqueKey](#dataform-ActionConfig-TableAssertionsConfig-UniqueKey) | repeated |                                                                                                                                                                                                        |
| nonNull       | [string](#string)                                                                                      | repeated | Column(s) which may never be `NULL`. If set, the resulting assertion will fail if any row contains `NULL` values for these column(s).                                                                  |
| rowConditions | [string](#string)                                                                                      | repeated | General condition(s) which should hold true for all rows in the dataset. If set, the resulting assertion will fail if any row violates any of these condition(s).                                      |

<a name="dataform-ActionConfig-TableAssertionsConfig-UniqueKey"></a>

### ActionConfig.TableAssertionsConfig.UniqueKey

Combinations of column(s), each of which should constitute a unique key
index for the dataset. If set, the resulting assertion(s) will fail if
there is more than one row in the dataset with the same values for all of
the column(s) in the unique key(s).

| Field     | Type              | Label    | Description |
| --------- | ----------------- | -------- | ----------- |
| uniqueKey | [string](#string) | repeated |             |

<a name="dataform-ActionConfig-TableConfig"></a>

### ActionConfig.TableConfig

| Field                        | Type                                                                                                         | Label    | Description                                                                                                                                                                                                                     |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                         | [string](#string)                                                                                            |          | The name of the table.                                                                                                                                                                                                          |
| dataset                      | [string](#string)                                                                                            |          | The dataset (schema) of the table.                                                                                                                                                                                              |
| project                      | [string](#string)                                                                                            |          | The Google Cloud project (database) of the table.                                                                                                                                                                               |
| dependencyTargets            | [ActionConfig.Target](#dataform-ActionConfig-Target)                                                         | repeated | Targets of actions that this action is dependent on.                                                                                                                                                                            |
| filename                     | [string](#string)                                                                                            |          | Path to the source file that the contents of the action is loaded from.                                                                                                                                                         |
| tags                         | [string](#string)                                                                                            | repeated | A list of user-defined tags with which the action should be labeled.                                                                                                                                                            |
| disabled                     | [bool](#bool)                                                                                                |          | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions.                                                                            |
| preOperations                | [string](#string)                                                                                            | repeated | Queries to run before `query`. This can be useful for granting permissions.                                                                                                                                                     |
| postOperations               | [string](#string)                                                                                            | repeated | Queries to run after `query`.                                                                                                                                                                                                   |
| description                  | [string](#string)                                                                                            |          | Description of the table.                                                                                                                                                                                                       |
| columns                      | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor)                                     | repeated | Descriptions of columns within the table.                                                                                                                                                                                       |
| partitionBy                  | [string](#string)                                                                                            |          | The key by which to partition the table. Typically the name of a timestamp or the date column. See https://cloud.google.com/dataform/docs/partitions-clusters.                                                                  |
| partitionExpirationDays      | [int32](#int32)                                                                                              |          | The number of days for which BigQuery stores data in each partition. The setting applies to all partitions in a table, but is calculated independently for each partition based on the partition time.                          |
| requirePartitionFilter       | [bool](#bool)                                                                                                |          | Declares whether the partitioned table requires a WHERE clause predicate filter that filters the partitioning column.                                                                                                           |
| clusterBy                    | [string](#string)                                                                                            | repeated | The keys by which to cluster partitions by. See https://cloud.google.com/dataform/docs/partitions-clusters.                                                                                                                     |
| labels                       | [ActionConfig.TableConfig.LabelsEntry](#dataform-ActionConfig-TableConfig-LabelsEntry)                       | repeated | Key-value pairs for BigQuery labels.                                                                                                                                                                                            |
| additionalOptions            | [ActionConfig.TableConfig.AdditionalOptionsEntry](#dataform-ActionConfig-TableConfig-AdditionalOptionsEntry) | repeated | Key-value pairs of additional options to pass to the BigQuery API. Some options, for example, partitionExpirationDays, have dedicated type/validity checked fields. For such options, use the dedicated fields.                 |
| dependOnDependencyAssertions | [bool](#bool)                                                                                                |          | When set to true, assertions dependent upon any dependency will be add as dedpendency to this action                                                                                                                            |
| assertions                   | [ActionConfig.TableAssertionsConfig](#dataform-ActionConfig-TableAssertionsConfig)                           |          | Assertions to be run on the dataset. If configured, relevant assertions will automatically be created and run as a dependency of this dataset.                                                                                  |
| hermetic                     | [bool](#bool)                                                                                                |          | If true, this indicates that the action only depends on data from explicitly-declared dependencies. Otherwise if false, it indicates that the action depends on data from a source which has not been declared as a dependency. |
| reservation          | [string](#string)                                                                                            |          | Optional. The BigQuery reservation to use for execution.                                                                                                                                                                        |
| jobLabels                    | [ActionConfig.TableConfig.JobLabelsEntry](#dataform-ActionConfig-TableConfig-JobLabelsEntry)                 | repeated | Optional. Key-value pairs for BigQuery job labels applied to the query job(s) executed for this table and its built-in assertions. Overrides matching keys from `defaultJobLabels` in `workflow_settings.yaml`. When the table has any job labels, including defaults, the Dataform CLI doesn't also apply its `labels` to its jobs. See `defaultJobLabels` for the allowed keys and values. |

<a name="dataform-ActionConfig-TableConfig-AdditionalOptionsEntry"></a>

### ActionConfig.TableConfig.AdditionalOptionsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-TableConfig-JobLabelsEntry"></a>

### ActionConfig.TableConfig.JobLabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-TableConfig-LabelsEntry"></a>

### ActionConfig.TableConfig.LabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-Target"></a>

### ActionConfig.Target

Target represents a unique action identifier.

| Field                      | Type              | Label | Description                                                                               |
| -------------------------- | ----------------- | ----- | ----------------------------------------------------------------------------------------- |
| project                    | [string](#string) |       | The Google Cloud project (database) of the action.                                        |
| dataset                    | [string](#string) |       | The dataset (schema) of the action. For notebooks, this is the location.                  |
| name                       | [string](#string) |       | The name of the action.                                                                   |
| includeDependentAssertions | [bool](#bool)     |       | flag for when we want to add assertions of this dependency in dependency_targets as well. |

<a name="dataform-ActionConfig-ViewConfig"></a>

### ActionConfig.ViewConfig

| Field                        | Type                                                                                                       | Label    | Description                                                                                                                                                                                                                                                     |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name                         | [string](#string)                                                                                          |          | The name of the view.                                                                                                                                                                                                                                           |
| dataset                      | [string](#string)                                                                                          |          | The dataset (schema) of the view.                                                                                                                                                                                                                               |
| project                      | [string](#string)                                                                                          |          | The Google Cloud project (database) of the view.                                                                                                                                                                                                                |
| dependencyTargets            | [ActionConfig.Target](#dataform-ActionConfig-Target)                                                       | repeated | Targets of actions that this action is dependent on.                                                                                                                                                                                                            |
| filename                     | [string](#string)                                                                                          |          | Path to the source file that the contents of the action is loaded from.                                                                                                                                                                                         |
| tags                         | [string](#string)                                                                                          | repeated | A list of user-defined tags with which the action should be labeled.                                                                                                                                                                                            |
| disabled                     | [bool](#bool)                                                                                              |          | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions.                                                                                                            |
| preOperations                | [string](#string)                                                                                          | repeated | Queries to run before `query`. This can be useful for granting permissions.                                                                                                                                                                                     |
| postOperations               | [string](#string)                                                                                          | repeated | Queries to run after `query`.                                                                                                                                                                                                                                   |
| materialized                 | [bool](#bool)                                                                                              |          | Applies the materialized view optimization, see https://cloud.google.com/bigquery/docs/materialized-views-intro.                                                                                                                                                |
| partitionBy                  | [string](#string)                                                                                          |          | Optional. Applicable only to materialized view. The key by which to partition the materialized view. Typically the name of a timestamp or the date column. See https://cloud.google.com/bigquery/docs/materialized-views-create#partitioned_materialized_views. |
| clusterBy                    | [string](#string)                                                                                          | repeated | Optional. Applicable only to materialized view. The keys by which to cluster partitions by. See https://cloud.google.com/bigquery/docs/materialized-views-create#cluster_materialized_views.                                                                    |
| description                  | [string](#string)                                                                                          |          | Description of the view.                                                                                                                                                                                                                                        |
| columns                      | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor)                                   | repeated | Descriptions of columns within the table.                                                                                                                                                                                                                       |
| labels                       | [ActionConfig.ViewConfig.LabelsEntry](#dataform-ActionConfig-ViewConfig-LabelsEntry)                       | repeated | Key-value pairs for BigQuery labels.                                                                                                                                                                                                                            |
| additionalOptions            | [ActionConfig.ViewConfig.AdditionalOptionsEntry](#dataform-ActionConfig-ViewConfig-AdditionalOptionsEntry) | repeated | Key-value pairs of additional options to pass to the BigQuery API. Some options, for example, partitionExpirationDays, have dedicated type/validity checked fields. For such options, use the dedicated fields.                                                 |
| dependOnDependencyAssertions | [bool](#bool)                                                                                              |          | When set to true, assertions dependent upon any dependency will be add as dedpendency to this action                                                                                                                                                            |
| hermetic                     | [bool](#bool)                                                                                              |          | If true, this indicates that the action only depends on data from explicitly-declared dependencies. Otherwise if false, it indicates that the action depends on data from a source which has not been declared as a dependency.                                 |
| assertions                   | [ActionConfig.TableAssertionsConfig](#dataform-ActionConfig-TableAssertionsConfig)                         |          | Assertions to be run on the dataset. If configured, relevant assertions will automatically be created and run as a dependency of this dataset.                                                                                                                  |
| reservation          | [string](#string)                                                                                          |          | Optional. The BigQuery reservation to use for execution.                                                                                                                                                                                                        |
| jobLabels                    | [ActionConfig.ViewConfig.JobLabelsEntry](#dataform-ActionConfig-ViewConfig-JobLabelsEntry)                 | repeated | Optional. Key-value pairs for BigQuery job labels applied to the query job(s) executed for this view and its built-in assertions. Overrides matching keys from `defaultJobLabels` in `workflow_settings.yaml`. When the view has any job labels, including defaults, the Dataform CLI doesn't also apply its `labels` to its jobs. See `defaultJobLabels` for the allowed keys and values. |

<a name="dataform-ActionConfig-ViewConfig-AdditionalOptionsEntry"></a>

### ActionConfig.ViewConfig.AdditionalOptionsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-ViewConfig-JobLabelsEntry"></a>

### ActionConfig.ViewConfig.JobLabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfig-ViewConfig-LabelsEntry"></a>

### ActionConfig.ViewConfig.LabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-ActionConfigs"></a>

### ActionConfigs

Action configs defines the contents of `actions.yaml` configuration files.

| Field   | Type                                   | Label    | Description |
| ------- | -------------------------------------- | -------- | ----------- |
| actions | [ActionConfig](#dataform-ActionConfig) | repeated |             |

<a name="dataform-LineageSettings"></a>

### LineageSettings

Configuration for Knowledge Catalog lineage emission. See the [Data Lineage guide](../lineage.md).

| Field   | Type          | Label | Description                                                                                                             |
| ------- | ------------- | ----- | ----------------------------------------------------------------------------------------------------------------------- |
| enabled | [bool](#bool) |       | If true, downstream runners will emit OpenLineage events to Knowledge Catalog for actions executed from this project. |

<a name="dataform-NotebookRuntimeOptionsConfig"></a>

### NotebookRuntimeOptionsConfig

| Field        | Type              | Label | Description                                                  |
| ------------ | ----------------- | ----- | ------------------------------------------------------------ |
| outputBucket | [string](#string) |       | Storage bucket to output notebooks to after their execution. |

<a name="dataform-WorkflowSettings"></a>

### WorkflowSettings

Workflow Settings defines the contents of the `workflow_settings.yaml`
configuration file.

| Field                         | Type                                                                                       | Label    | Description                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------------------- | ------------------------------------------------------------------------------------------ | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| dataformCoreVersion           | [string](#string)                                                                          |          | The desired dataform core version to compile against. When set, the CLI installs this version into a copy of the project in a temporary directory and compiles the copy, which skips paths matched by `.gitignore` or `.dataformignore` outside `definitions/` and `includes/`. For details, see https://dataform-co.github.io/dataform/docs/stateless-install. |
| defaultProject                | [string](#string)                                                                          |          | Required. The default Google Cloud project (database).                                                                                                                                                                                                                                                                                                                                  |
| defaultDataset                | [string](#string)                                                                          |          | Required. The default dataset (schema).                                                                                                                                                                                                                                                                                                                                                 |
| defaultLocation               | [string](#string)                                                                          |          | Required. The default BigQuery location to use. For more information on BigQuery locations, see https://cloud.google.com/bigquery/docs/locations.                                                                                                                                                                                                                                       |
| defaultAssertionDataset       | [string](#string)                                                                          |          | Required. The default dataset (schema) for assertions.                                                                                                                                                                                                                                                                                                                                  |
| vars                          | [WorkflowSettings.VarsEntry](#dataform-WorkflowSettings-VarsEntry)                         | repeated | Optional. User-defined variables that are made available to project code during compilation. An object containing a list of &#34;key&#34;: value pairs.                                                                                                                                                                                                                                 |
| projectSuffix                 | [string](#string)                                                                          |          | Optional. The suffix to append to all Google Cloud project references.                                                                                                                                                                                                                                                                                                                  |
| datasetSuffix                 | [string](#string)                                                                          |          | Optional. The suffix to append to all dataset references.                                                                                                                                                                                                                                                                                                                               |
| namePrefix                    | [string](#string)                                                                          |          | Optional. The prefix to append to all action names.                                                                                                                                                                                                                                                                                                                                     |
| defaultNotebookRuntimeOptions | [NotebookRuntimeOptionsConfig](#dataform-NotebookRuntimeOptionsConfig)                     |          | Optional. Default runtime options for Notebook actions.                                                                                                                                                                                                                                                                                                                                 |
| builtinAssertionNamePrefix    | [string](#string)                                                                          |          | Optional. The prefix to append to built-in assertion names.                                                                                                                                                                                                                                                                                                                             |
| defaultReservation            | [string](#string)                                                                          |          | Optional. The default BigQuery reservation to use for execution.                                                                                                                                                                                                                                                                                                                        |
| lineage                       | [LineageSettings](#dataform-LineageSettings)                                               |          | Optional. Configuration for Knowledge Catalog lineage emission. See the [Data Lineage guide](../lineage.md).                                                                                                                                                                                                                                                                            |
| defaultJobLabels              | [WorkflowSettings.DefaultJobLabelsEntry](#dataform-WorkflowSettings-DefaultJobLabelsEntry) | repeated | Optional. Default key-value pairs for BigQuery job labels applied to BigQuery jobs executed for actions in this project. Merged key-by-key when overridden via `projectConfigOverride`. Action-level `jobLabels` override matching keys; an action can't remove a default key. Built-in assertions use the job labels of their table. When an action has any job labels, including these defaults, the Dataform CLI doesn't also apply the table's `labels` to its jobs. `dataform run --job-labels` overrides matching keys. Keys and values can contain only lowercase ASCII letters, numbers, underscores and hyphens, and can be at most 63 characters long. Keys must start with a lowercase letter; values can be empty. An action can have at most 32 job labels, including these defaults. Keys that start with `dataform_`, `dataform-`, `dataprep-`, `gcp-` or `goog-` are reserved, as are `bigquery-workflow`, `single-file-asset-type`, `origin`, `bq-dea` and `data-preparation-file-loader`. Numbers and booleans are converted to strings, so quote values that must be kept exactly as written, for example `"0042"`. |

<a name="dataform-WorkflowSettings-DefaultJobLabelsEntry"></a>

### WorkflowSettings.DefaultJobLabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-WorkflowSettings-VarsEntry"></a>

### WorkflowSettings.VarsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="property_graph_config-proto"></a>

<p align="right"><a href="#top">Top</a></p>

## property_graph_config.proto

Defines the configuration schema for BigQuery Property Graph YAML files (`definitions/**/graph.yaml`), aligned with the Google Semantic Ontology V1 specification (`version: 0.2.0.dev0/google`). At most one `graph.yaml` file is allowed per project in V1. Both `snake_case` (recommended in `graph.yaml`, e.g. `semantic_model`, `primary_key`, `from_columns`, `to_columns`, `ai_context`, `custom_extensions`, `job_labels`, `depend_on_dependency_assertions`, `include_dependent_assertions`) and `camelCase` keys are accepted.

<a name="dataform-DataSourceRef"></a>

### DataSourceRef

Reference to another Dataform action as a backing table source (`source: { ref: ... }`). In `graph.yaml`, a string shorthand `source: { ref: "<action_name>" }` is also accepted and normalized to `{ name: "<action_name>" }`.

| Field                      | Type              | Label | Description                                                                                             |
| -------------------------- | ----------------- | ----- | ------------------------------------------------------------------------------------------------------- |
| name                       | [string](#string) |       | Required. Target Dataform action name.                                                                  |
| schema                     | [string](#string) |       | Optional. Target dataset (schema) name.                                                                 |
| database                   | [string](#string) |       | Optional. Target Google Cloud project (database) ID.                                                    |
| includeDependentAssertions | [bool](#bool)     |       | Optional (`include_dependent_assertions` in YAML). If true, depends on assertions of the target action. |

<a name="dataform-GraphAiContextConfig"></a>

### GraphAiContextConfig

AI context metadata attached to a semantic model, entity, relationship, field, or metric (`ai_context` in `graph.yaml`). On entities and relationships, `DEFAULT LABEL OPTIONS(...)` is emitted before `PROPERTIES (...)`, `PROPERTIES ALL COLUMNS`, or `NO PROPERTIES`. On entities, relationships, fields, and metrics, `synonyms` map to `OPTIONS(synonyms=[...])`, while non-empty `instructions` and JSON-serialized `examples` map to `OPTIONS(extensions=[("instructions", "..."), ("examples", "[...]")])`. Model-level `ai_context` populates `PropertyGraph.description` only.

| Field        | Type                                                 | Label    | Description                                                                                                                                                                                             |
| ------------ | ---------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| synonyms     | [string](#string)                                    | repeated | Optional. Alternative names emitted as `OPTIONS(synonyms=[...])`.                                                                                                                                       |
| examples     | [string](#string)                                    | repeated | Optional. Example values or questions emitted as a JSON-serialized array in `OPTIONS(extensions=[("examples", "[...]")])` on labels, fields, and metrics (and folded into `PropertyGraph.description` on `semantic_model`). |
| instructions | [string](#string)                                    |          | Optional. Free-form AI instructions emitted as `OPTIONS(extensions=[("instructions", "...")])` on labels, fields, and metrics (and folded into `PropertyGraph.description` on `semantic_model`).       |
| custom       | [google.protobuf.Struct](#google-protobuf-Struct)    |          | Optional. Opaque mapping of custom AI context member names to arbitrary JSON/YAML values (preserving nested `snake_case` keys verbatim). Accepted for Knowledge Catalog compatibility and omitted from generated BigQuery DDL. |

<a name="dataform-GraphCustomExtensionConfig"></a>

### GraphCustomExtensionConfig

Vendor-scoped custom extension metadata block attached to a semantic model, entity, relationship, field, or metric (`custom_extensions` in `graph.yaml`). Accepted for Knowledge Catalog compatibility and omitted from generated BigQuery DDL.

| Field      | Type              | Label | Description                                                                                                                                                                                                     |
| ---------- | ----------------- | ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| vendorName | [string](#string) |       | Required (`vendor_name` in YAML). Non-empty third-party vendor identifier (`"GOOGLE"` in any casing is rejected in `0.2.0.dev0/google` documents, where Google extensions are declared as native keys).          |
| data       | [string](#string) |       | Required. Vendor-specific extension payload. In `graph.yaml`, either a serialized JSON string or a plain mapping object (normalized to a JSON string with nested keys preserved verbatim) is accepted.          |

<a name="dataform-GraphDeploymentConfig"></a>

### GraphDeploymentConfig

Deployment target mapping for a semantic model (`deployments` in `graph.yaml`). At most one deployment is supported in V1; when omitted, the property graph is deployed to the project's default database and default schema with the `semantic_model.name`.

| Field  | Type              | Label | Description                                                                                                                                                                                                                                                                                                                                     |
| ------ | ----------------- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name   | [string](#string) |       | Logical deployment profile name (for example, `default`).                                                                                                                                                                                                                                                                                       |
| target | [string](#string) |       | Required. Target BigQuery property graph resource URI (`bigquery.googleapis.com/projects/<project>/datasets/<dataset>/propertyGraphs/<graph>`). Leading `//` is rejected. Sets the base project, dataset, and graph name on the Dataform action target; Dataform session overrides (`databaseSuffix`, `schemaSuffix`, `namePrefix`) still apply when resolving the final target. |

<a name="dataform-GraphDimensionConfig"></a>

### GraphDimensionConfig

Dimension metadata for a property field on an entity (`dimension` in `graph.yaml`). In `graph.yaml`, a boolean shorthand `dimension: true` is also accepted and normalized to `{ is_time: true }`.

| Field  | Type          | Label | Description                                                                                                                    |
| ------ | ------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------ |
| isTime | [bool](#bool) |       | Optional (`is_time` in YAML). If true, marks the field as a temporal dimension and folds `"Time dimension."` into `OPTIONS(description=...)`. |

<a name="dataform-GraphEntityConfig"></a>

### GraphEntityConfig

A node entity definition in the semantic model (`entities` in `graph.yaml`). Compiles to a BigQuery `NODE TABLES` entry.

| Field            | Type                                                                 | Label    | Description                                                                                                                                                                                                                                                                                                                 |
| ---------------- | -------------------------------------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name             | [string](#string)                                                    |          | Required. Unique entity name within the semantic model (must be a valid identifier).                                                                                                                                                                                                                                        |
| sourceString     | [string](#string)                                                    |          | Backing table reference when `source` in `graph.yaml` is a string (`bigquery:<project>.<dataset>.<table>`, `<project>.<dataset>.<table>`, or `bigquery.googleapis.com/projects/<project>/datasets/<dataset>/tables/<table>`; leading `//` is rejected). Mutually exclusive with `sourceRef`.                               |
| sourceRef        | [DataSourceRef](#dataform-DataSourceRef)                             |          | Backing Dataform action reference when `source` in `graph.yaml` is `{ ref: "<name>" }` or `{ ref: { name, schema, database, include_dependent_assertions } }`. Mutually exclusive with `sourceString`.                                                                                                                      |
| primaryKey       | [string](#string)                                                    | repeated | Required (`primary_key` in YAML; accepts a single string or list of strings). Physical primary key column name(s) identifying rows of this entity, emitted in `KEY (...)`.                                                                                                                                                  |
| uniqueKeys       | [GraphUniqueKeyList](#dataform-GraphUniqueKeyList)                   | repeated | Secondary unique key column lists (`unique_keys` in YAML). Not supported in V1 because BigQuery `CREATE PROPERTY GRAPH` DDL supports only a single `KEY(...)` clause.                                                                                                                                                       |
| description      | [string](#string)                                                    |          | Optional. Human-readable description of the entity, emitted in `DEFAULT LABEL OPTIONS(description=...)`.                                                                                                                                                                                                                    |
| aiContext        | [GraphAiContextConfig](#dataform-GraphAiContextConfig)               |          | Optional (`ai_context` in YAML). AI context metadata (`instructions`, `synonyms`, `examples`, and `custom`) for the entity.                                                                                                                                                                                                 |
| fields           | [GraphFieldConfig](#dataform-GraphFieldConfig)                       | repeated | Optional. Properties exposed on the entity node table (`PROPERTIES (...)`). In `graph.yaml`, each item may be a bare column name string `"col"` or a `GraphFieldConfig` object. When omitted (and no `metrics` target the entity), `PROPERTIES ALL COLUMNS` is emitted. Required when one or more `metrics` target the entity. |
| customExtensions | [GraphCustomExtensionConfig](#dataform-GraphCustomExtensionConfig)   | repeated | Optional (`custom_extensions` in YAML). Vendor-scoped custom extension metadata blocks (`vendor_name` must not be `"GOOGLE"` in `0.2.0.dev0/google` documents). Accepted for Knowledge Catalog compatibility and omitted from generated BigQuery DDL.                                                                       |

<a name="dataform-GraphFieldConfig"></a>

### GraphFieldConfig

A property field definition on an entity (`fields` in `graph.yaml`). In `graph.yaml`, a bare string `"col"` is shorthand for `{ name: "col", expression: "col" }`.

| Field            | Type                                                               | Label    | Description                                                                                                                                                                                            |
| ---------------- | ------------------------------------------------------------------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| name             | [string](#string)                                                  |          | Required. Property name exposed on the graph element (must be a valid identifier, unique case-insensitively on the entity).                                                                            |
| expression       | [string](#string)                                                  |          | Optional. SQL expression computing the property value; defaults to `name`. Passed through as-is into BigQuery DDL (`<expression> AS <name>` when distinct from `name`).                                |
| description      | [string](#string)                                                  |          | Optional. Human-readable description of the field, folded into `OPTIONS(description=...)`.                                                                                                             |
| datatype         | [string](#string)                                                  |          | Optional. Informational data type metadata. Accepted by the parser but has no effect on the generated BigQuery DDL.                                                                                    |
| dimension        | [GraphDimensionConfig](#dataform-GraphDimensionConfig)             |          | Optional. Dimension metadata (e.g. `dimension: { is_time: true }` or `dimension: true`), folding `"Time dimension."` into `OPTIONS(description=...)` when `is_time` is true.                           |
| label            | [string](#string)                                                  |          | Optional. Human-readable display label folded into `OPTIONS(description=...)`.                                                                                                                         |
| aiContext        | [GraphAiContextConfig](#dataform-GraphAiContextConfig)             |          | Optional (`ai_context` in YAML). AI context metadata (`instructions`, `synonyms`, `examples`, and `custom`) for the field.                                                                             |
| customExtensions | [GraphCustomExtensionConfig](#dataform-GraphCustomExtensionConfig) | repeated | Optional (`custom_extensions` in YAML). Vendor-scoped custom extension metadata blocks (`vendor_name` must not be `"GOOGLE"` in `0.2.0.dev0/google` documents). Accepted for Knowledge Catalog compatibility and omitted from generated BigQuery DDL. |

<a name="dataform-GraphMetricConfig"></a>

### GraphMetricConfig

A top-level aggregate metric definition (`metrics` in `graph.yaml`), compiled as a `MEASURE(<expression>) AS <name>` property inside the target entity's `PROPERTIES (...)` list.

| Field            | Type                                                               | Label    | Description                                                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------- | ------------------------------------------------------------------ | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name             | [string](#string)                                                  |          | Required. Unique metric name within the semantic model (must be a valid identifier and must not collide case-insensitively with any declared field on the target `entity`).                                                                                                                                                                                                                                          |
| entity           | [string](#string)                                                  |          | Required. Target entity name on whose node table the `MEASURE(...)` property is emitted.                                                                                                                                                                                                                                                                                                                             |
| expression       | [string](#string)                                                  |          | Required. SQL aggregate or derived measure expression passed through as-is into `MEASURE(<expression>) AS <name>` (supporting `SUM`, `AVG`, `COUNT(<property>)`, `MIN`, `MAX`, `DISTINCT`, `CASE WHEN`, scalar arithmetic, and derived sibling measures via `AGG(<sibling_metric>)`). `COUNT(*)` and literal-only counts such as `COUNT(1)` are rejected at compile time because BigQuery requires every measure to aggregate at least one graph property. |
| description      | [string](#string)                                                  |          | Optional. Human-readable description of the metric, folded into `OPTIONS(description=...)`.                                                                                                                                                                                                                                                                                                                          |
| label            | [string](#string)                                                  |          | Optional. Human-readable display label folded into `OPTIONS(description=...)`.                                                                                                                                                                                                                                                                                                                                       |
| aiContext        | [GraphAiContextConfig](#dataform-GraphAiContextConfig)             |          | Optional (`ai_context` in YAML). AI context metadata (`instructions`, `synonyms`, `examples`, and `custom`) for the metric.                                                                                                                                                                                                                                                                                          |
| datatype         | [string](#string)                                                  |          | Optional. Informational data type metadata. Accepted by the parser but has no effect on the generated BigQuery DDL.                                                                                                                                                                                                                                                                                                  |
| customExtensions | [GraphCustomExtensionConfig](#dataform-GraphCustomExtensionConfig) | repeated | Optional (`custom_extensions` in YAML). Vendor-scoped custom extension metadata blocks (`vendor_name` must not be `"GOOGLE"` in `0.2.0.dev0/google` documents). Accepted for Knowledge Catalog compatibility and omitted from generated BigQuery DDL.                                                                                                                                                                |

<a name="dataform-GraphRelationshipConfig"></a>

### GraphRelationshipConfig

An edge relationship definition connecting two entities in the semantic model (`relationships` in `graph.yaml`). Each relationship is a 1:N foreign-key edge from `from` to `to`, backed by the `from` entity's table and `primary_key`, and compiles to a BigQuery `EDGE TABLES` entry with `NO PROPERTIES` in V1.

| Field            | Type                                                               | Label    | Description                                                                                                                                                                                            |
| ---------------- | ------------------------------------------------------------------ | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| name             | [string](#string)                                                  |          | Required. Unique relationship name within the semantic model (must be a valid identifier).                                                                                                             |
| from             | [string](#string)                                                  |          | Required. Name of the source entity (`SOURCE KEY ... REFERENCES <from>`).                                                                                                                              |
| to               | [string](#string)                                                  |          | Required. Name of the destination entity (`DESTINATION KEY ... REFERENCES <to>`).                                                                                                                      |
| fromColumns      | [string](#string)                                                  | repeated | Required (`from_columns` in YAML; accepts a single string or list of strings). Physical foreign-key column name(s) on the `from` entity table emitted in `DESTINATION KEY (<from_columns>) REFERENCES <to> (<to_columns>)`. |
| toColumns        | [string](#string)                                                  | repeated | Required (`to_columns` in YAML; accepts a single string or list of strings). Referenced key column(s) on the `to` entity table (must match `<to>.primary_key`).                                        |
| description      | [string](#string)                                                  |          | Optional. Human-readable description of the relationship, emitted in `DEFAULT LABEL OPTIONS(description=...)`.                                                                                         |
| aiContext        | [GraphAiContextConfig](#dataform-GraphAiContextConfig)             |          | Optional (`ai_context` in YAML). AI context metadata (`instructions`, `synonyms`, `examples`, and `custom`) for the relationship.                                                                      |
| customExtensions | [GraphCustomExtensionConfig](#dataform-GraphCustomExtensionConfig) | repeated | Optional (`custom_extensions` in YAML). Vendor-scoped custom extension metadata blocks (`vendor_name` must not be `"GOOGLE"` in `0.2.0.dev0/google` documents). Accepted for Knowledge Catalog compatibility and omitted from generated BigQuery DDL. |

<a name="dataform-GraphUniqueKeyList"></a>

### GraphUniqueKeyList

Wrapper for a single composite unique key column list on an entity (`unique_keys` in `graph.yaml`). Accepted by the parser so V1 validation can report an explicit unsupported error.

| Field   | Type              | Label    | Description                                  |
| ------- | ----------------- | -------- | -------------------------------------------- |
| columns | [string](#string) | repeated | Column names comprising the unique key list. |

<a name="dataform-PropertyGraphConfig"></a>

### PropertyGraphConfig

Top-level configuration for a Dataform BigQuery Property Graph YAML file (`definitions/**/graph.yaml`), aligned with the Google Semantic Ontology V1 specification (`version: 0.2.0.dev0/google`).

| Field                        | Type                                                                               | Label    | Description                                                                                                                                                                                                                                                        |
| ---------------------------- | ---------------------------------------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| version                      | [string](#string)                                                                  |          | Required. Semantic Ontology specification version (`"0.2.0.dev0/google"`).                                                                                                                                                                                         |
| semanticModel                | [SemanticModelConfig](#dataform-SemanticModelConfig)                               | repeated | Required (`semantic_model` in YAML). Semantic models declared in the file (must contain 1 model in V1).                                                                                                                                                            |
| disabled                     | [bool](#bool)                                                                      |          | Optional. If set to true, this property graph action will not be executed, though it can still be depended upon.                                                                                                                                                   |
| dependOnDependencyAssertions | [bool](#bool)                                                                      |          | Optional (`depend_on_dependency_assertions` in YAML). If true, assertions dependent upon any of the graph's referenced dependencies are added as dependencies of this property graph action.                                                                       |
| tags                         | [string](#string)                                                                  | repeated | Optional. A list of user-defined tags with which the property graph action should be labeled.                                                                                                                                                                      |
| jobLabels                    | [PropertyGraphConfig.JobLabelsEntry](#dataform-PropertyGraphConfig-JobLabelsEntry) | repeated | Optional (`job_labels` in YAML). Key-value pairs for BigQuery job labels applied to the query job(s) executed for this property graph. Overrides matching keys from `defaultJobLabels` in `workflow_settings.yaml`. See `defaultJobLabels` for the allowed keys and values. |

<a name="dataform-PropertyGraphConfig-JobLabelsEntry"></a>

### PropertyGraphConfig.JobLabelsEntry

| Field | Type              | Label | Description |
| ----- | ----------------- | ----- | ----------- |
| key   | [string](#string) |       |             |
| value | [string](#string) |       |             |

<a name="dataform-SemanticModelConfig"></a>

### SemanticModelConfig

A single semantic model definition within `semantic_model` in `definitions/**/graph.yaml`.

| Field            | Type                                                               | Label    | Description                                                                                                                                                                                                                  |
| ---------------- | ------------------------------------------------------------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name             | [string](#string)                                                  |          | Required. Name of the semantic model / BigQuery property graph (must be a valid identifier).                                                                                                                                 |
| description      | [string](#string)                                                  |          | Optional. Human-readable description of the semantic model. Populates Dataform's action metadata (`PropertyGraph.description`) only and has no effect on the generated BigQuery DDL (BigQuery `CREATE PROPERTY GRAPH` does not support graph-level `OPTIONS`). |
| aiContext        | [GraphAiContextConfig](#dataform-GraphAiContextConfig)             |          | Optional (`ai_context` in YAML). AI context metadata for the model. `instructions` and `examples` are folded into Dataform's action metadata (`PropertyGraph.description`) only; model-level `ai_context` has no effect on the generated BigQuery DDL. |
| deployments      | [GraphDeploymentConfig](#dataform-GraphDeploymentConfig)           | repeated | Optional. Deployment target(s) for the property graph (at most 1 in V1).                                                                                                                                                     |
| entities         | [GraphEntityConfig](#dataform-GraphEntityConfig)                   | repeated | Required (`entities` or `datasets` in YAML). Node entities in the semantic model (must contain at least 1 entity).                                                                                                           |
| relationships    | [GraphRelationshipConfig](#dataform-GraphRelationshipConfig)       | repeated | Optional. Edge relationships connecting entities in the semantic model.                                                                                                                                                      |
| metrics          | [GraphMetricConfig](#dataform-GraphMetricConfig)                   | repeated | Optional. Top-level aggregate metrics compiled as `MEASURE(...)` properties on target entities.                                                                                                                              |
| customExtensions | [GraphCustomExtensionConfig](#dataform-GraphCustomExtensionConfig) | repeated | Optional (`custom_extensions` in YAML). Vendor-scoped custom extension metadata blocks (`vendor_name` must not be `"GOOGLE"` in `0.2.0.dev0/google` documents). Accepted for Knowledge Catalog compatibility and omitted from generated BigQuery DDL. |

## Scalar Value Types

| .proto Type                    | Notes                                                                                                                                           | C++    | Java       | Python      | Go      | C#         | PHP            | Ruby                           |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------- | ----------- | ------- | ---------- | -------------- | ------------------------------ |
| <a name="double" /> double     |                                                                                                                                                 | double | double     | float       | float64 | double     | float          | Float                          |
| <a name="float" /> float       |                                                                                                                                                 | float  | float      | float       | float32 | float      | float          | Float                          |
| <a name="int32" /> int32       | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32  | int        | int         | int32   | int        | integer        | Bignum or Fixnum (as required) |
| <a name="int64" /> int64       | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64  | long       | int/long    | int64   | long       | integer/string | Bignum                         |
| <a name="uint32" /> uint32     | Uses variable-length encoding.                                                                                                                  | uint32 | int        | int/long    | uint32  | uint       | integer        | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64     | Uses variable-length encoding.                                                                                                                  | uint64 | long       | int/long    | uint64  | ulong      | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32     | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s.                            | int32  | int        | int         | int32   | int        | integer        | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64     | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s.                            | int64  | long       | int/long    | int64   | long       | integer/string | Bignum                         |
| <a name="fixed32" /> fixed32   | Always four bytes. More efficient than uint32 if values are often greater than 2^28.                                                            | uint32 | int        | int         | uint32  | uint       | integer        | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64   | Always eight bytes. More efficient than uint64 if values are often greater than 2^56.                                                           | uint64 | long       | int/long    | uint64  | ulong      | integer/string | Bignum                         |
| <a name="sfixed32" /> sfixed32 | Always four bytes.                                                                                                                              | int32  | int        | int         | int32   | int        | integer        | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes.                                                                                                                             | int64  | long       | int/long    | int64   | long       | integer/string | Bignum                         |
| <a name="bool" /> bool         |                                                                                                                                                 | bool   | boolean    | boolean     | bool    | bool       | boolean        | TrueClass/FalseClass           |
| <a name="string" /> string     | A string must always contain UTF-8 encoded or 7-bit ASCII text.                                                                                 | string | String     | str/unicode | string  | string     | string         | String (UTF-8)                 |
| <a name="bytes" /> bytes       | May contain any arbitrary sequence of bytes.                                                                                                    | string | ByteString | str         | []byte  | ByteString | string         | String (ASCII-8BIT)            |
