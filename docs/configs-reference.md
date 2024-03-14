# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [configs.proto](#configs-proto)
    - [ActionConfig](#dataform-ActionConfig)
    - [ActionConfig.AssertionConfig](#dataform-ActionConfig-AssertionConfig)
    - [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor)
    - [ActionConfig.DeclarationConfig](#dataform-ActionConfig-DeclarationConfig)
    - [ActionConfig.IncrementalTableConfig](#dataform-ActionConfig-IncrementalTableConfig)
    - [ActionConfig.IncrementalTableConfig.AdditionalOptionsEntry](#dataform-ActionConfig-IncrementalTableConfig-AdditionalOptionsEntry)
    - [ActionConfig.IncrementalTableConfig.LabelsEntry](#dataform-ActionConfig-IncrementalTableConfig-LabelsEntry)
    - [ActionConfig.NotebookConfig](#dataform-ActionConfig-NotebookConfig)
    - [ActionConfig.OperationConfig](#dataform-ActionConfig-OperationConfig)
    - [ActionConfig.TableConfig](#dataform-ActionConfig-TableConfig)
    - [ActionConfig.TableConfig.AdditionalOptionsEntry](#dataform-ActionConfig-TableConfig-AdditionalOptionsEntry)
    - [ActionConfig.TableConfig.LabelsEntry](#dataform-ActionConfig-TableConfig-LabelsEntry)
    - [ActionConfig.Target](#dataform-ActionConfig-Target)
    - [ActionConfig.ViewConfig](#dataform-ActionConfig-ViewConfig)
    - [ActionConfig.ViewConfig.AdditionalOptionsEntry](#dataform-ActionConfig-ViewConfig-AdditionalOptionsEntry)
    - [ActionConfig.ViewConfig.LabelsEntry](#dataform-ActionConfig-ViewConfig-LabelsEntry)
    - [ActionConfigs](#dataform-ActionConfigs)
    - [NotebookRuntimeOptionsConfig](#dataform-NotebookRuntimeOptionsConfig)
    - [WorkflowSettings](#dataform-WorkflowSettings)
    - [WorkflowSettings.VarsEntry](#dataform-WorkflowSettings-VarsEntry)
  
- [Scalar Value Types](#scalar-value-types)



<a name="configs-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## configs.proto



<a name="dataform-ActionConfig"></a>

### ActionConfig
Action config defines the configuration properties of actions.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| table | [ActionConfig.TableConfig](#dataform-ActionConfig-TableConfig) |  |  |
| view | [ActionConfig.ViewConfig](#dataform-ActionConfig-ViewConfig) |  |  |
| incremental_table | [ActionConfig.IncrementalTableConfig](#dataform-ActionConfig-IncrementalTableConfig) |  |  |
| assertion | [ActionConfig.AssertionConfig](#dataform-ActionConfig-AssertionConfig) |  |  |
| operation | [ActionConfig.OperationConfig](#dataform-ActionConfig-OperationConfig) |  |  |
| declaration | [ActionConfig.DeclarationConfig](#dataform-ActionConfig-DeclarationConfig) |  |  |
| notebook | [ActionConfig.NotebookConfig](#dataform-ActionConfig-NotebookConfig) |  |  |






<a name="dataform-ActionConfig-AssertionConfig"></a>

### ActionConfig.AssertionConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of the assertion. |
| dataset | [string](#string) |  | The dataset (schema) of the assertion. |
| project | [string](#string) |  | The Google Cloud project (database) of the assertion. |
| dependency_targets | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on. |
| filename | [string](#string) |  | Path to the source file that the contents of the action is loaded from. |
| tags | [string](#string) | repeated | A list of user-defined tags with which the action should be labeled. |
| disabled | [bool](#bool) |  | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions. |
| description | [string](#string) |  | Description of the assertion. |






<a name="dataform-ActionConfig-ColumnDescriptor"></a>

### ActionConfig.ColumnDescriptor



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [string](#string) | repeated | The identifier for the column, using multiple parts for nested records. |
| description | [string](#string) |  | A text description of the column. |
| bigquery_policy_tags | [string](#string) | repeated | A list of BigQuery policy tags that will be applied to the column. |






<a name="dataform-ActionConfig-DeclarationConfig"></a>

### ActionConfig.DeclarationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of the declaration. |
| dataset | [string](#string) |  | The dataset (schema) of the declaration. |
| project | [string](#string) |  | The Google Cloud project (database) of the declaration. |
| description | [string](#string) |  | Description of the declaration. |
| columns | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor) | repeated | Descriptions of columns within the declaration. |






<a name="dataform-ActionConfig-IncrementalTableConfig"></a>

### ActionConfig.IncrementalTableConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of the incremental table. |
| dataset | [string](#string) |  | The dataset (schema) of the incremental table. |
| project | [string](#string) |  | The Google Cloud project (database) of the incremental table. |
| dependency_targets | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on. |
| filename | [string](#string) |  | Path to the source file that the contents of the action is loaded from. |
| tags | [string](#string) | repeated | A list of user-defined tags with which the action should be labeled. |
| disabled | [bool](#bool) |  | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions. |
| pre_operations | [string](#string) | repeated | Queries to run before `query`. This can be useful for granting permissions. |
| post_operations | [string](#string) | repeated | Queries to run after `query`. |
| protected | [bool](#bool) |  | If true, prevents the dataset from being rebuilt from scratch. |
| unique_key | [string](#string) | repeated | If set, unique key represents a set of names of columns that will act as a the unique key. To enforce this, when updating the incremental table, Dataform merges rows with `uniqueKey` instead of appending them. |
| description | [string](#string) |  | Description of the incremental table. |
| columns | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor) | repeated | Descriptions of columns within the table. |
| partition_by | [string](#string) |  | The key by which to partition the table. Typically the name of a timestamp or the date column. See https://cloud.google.com/dataform/docs/partitions-clusters. |
| partition_expiration_days | [int32](#int32) |  | The number of days for which BigQuery stores data in each partition. The setting applies to all partitions in a table, but is calculated independently for each partition based on the partition time. |
| require_partition_filter | [bool](#bool) |  | Declares whether the partitioned table requires a WHERE clause predicate filter that filters the partitioning column. |
| update_partition_filter | [string](#string) |  | SQL-based filter for when incremental updates are applied. |
| cluster_by | [string](#string) | repeated | The keys by which to cluster partitions by. See https://cloud.google.com/dataform/docs/partitions-clusters. |
| labels | [ActionConfig.IncrementalTableConfig.LabelsEntry](#dataform-ActionConfig-IncrementalTableConfig-LabelsEntry) | repeated | Key-value pairs for BigQuery labels. If the label name contains special characters, e.g. hyphens, then quote its name, e.g. `labels: { &#34;label-name&#34;: &#34;value&#34; }`. |
| additional_options | [ActionConfig.IncrementalTableConfig.AdditionalOptionsEntry](#dataform-ActionConfig-IncrementalTableConfig-AdditionalOptionsEntry) | repeated | Key-value pairs of additional options to pass to the BigQuery API.

Some options, for example, partitionExpirationDays, have dedicated type/validity checked fields. For such options, use the dedicated fields.

String values must be encapsulated in double-quotes, for example: additionalOptions: {numeric_option: &#34;5&#34;, string_option: &#39;&#34;string-value&#34;&#39;}

If the option name contains special characters, encapsulate the name in quotes, for example: additionalOptions: { &#34;option-name&#34;: &#34;value&#34; }. |






<a name="dataform-ActionConfig-IncrementalTableConfig-AdditionalOptionsEntry"></a>

### ActionConfig.IncrementalTableConfig.AdditionalOptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="dataform-ActionConfig-IncrementalTableConfig-LabelsEntry"></a>

### ActionConfig.IncrementalTableConfig.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="dataform-ActionConfig-NotebookConfig"></a>

### ActionConfig.NotebookConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of the notebook. |
| location | [string](#string) |  | The Google Cloud location of the notebook. |
| project | [string](#string) |  | The Google Cloud project (database) of the notebook. |
| dependency_targets | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on. |
| filename | [string](#string) |  | Path to the source file that the contents of the action is loaded from. |
| tags | [string](#string) | repeated | A list of user-defined tags with which the action should be labeled. |
| disabled | [bool](#bool) |  | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions. |
| description | [string](#string) |  | Description of the notebook. |






<a name="dataform-ActionConfig-OperationConfig"></a>

### ActionConfig.OperationConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of the operation. |
| dataset | [string](#string) |  | The dataset (schema) of the operation. |
| project | [string](#string) |  | The Google Cloud project (database) of the operation. |
| dependency_targets | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on. |
| filename | [string](#string) |  | Path to the source file that the contents of the action is loaded from. |
| tags | [string](#string) | repeated | A list of user-defined tags with which the action should be labeled. |
| disabled | [bool](#bool) |  | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions. |
| has_output | [bool](#bool) |  | Declares that this action creates a dataset which should be referenceable as a dependency target, for example by using the `ref` function. |
| description | [string](#string) |  | Description of the operation. |
| columns | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor) | repeated | Descriptions of columns within the operation. Can only be set if hasOutput is true. |






<a name="dataform-ActionConfig-TableConfig"></a>

### ActionConfig.TableConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of the table. |
| dataset | [string](#string) |  | The dataset (schema) of the table. |
| project | [string](#string) |  | The Google Cloud project (database) of the table. |
| dependency_targets | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on. |
| filename | [string](#string) |  | Path to the source file that the contents of the action is loaded from. |
| tags | [string](#string) | repeated | A list of user-defined tags with which the action should be labeled. |
| disabled | [bool](#bool) |  | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions. |
| pre_operations | [string](#string) | repeated | Queries to run before `query`. This can be useful for granting permissions. |
| post_operations | [string](#string) | repeated | Queries to run after `query`. |
| description | [string](#string) |  | Description of the table. |
| columns | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor) | repeated | Descriptions of columns within the table. |
| partition_by | [string](#string) |  | The key by which to partition the table. Typically the name of a timestamp or the date column. See https://cloud.google.com/dataform/docs/partitions-clusters. |
| partition_expiration_days | [int32](#int32) |  | The number of days for which BigQuery stores data in each partition. The setting applies to all partitions in a table, but is calculated independently for each partition based on the partition time. |
| require_partition_filter | [bool](#bool) |  | Declares whether the partitioned table requires a WHERE clause predicate filter that filters the partitioning column. |
| cluster_by | [string](#string) | repeated | The keys by which to cluster partitions by. See https://cloud.google.com/dataform/docs/partitions-clusters. |
| labels | [ActionConfig.TableConfig.LabelsEntry](#dataform-ActionConfig-TableConfig-LabelsEntry) | repeated | Key-value pairs for BigQuery labels. If the label name contains special characters, e.g. hyphens, then quote its name, e.g. `labels: { &#34;label-name&#34;: &#34;value&#34; }`. |
| additional_options | [ActionConfig.TableConfig.AdditionalOptionsEntry](#dataform-ActionConfig-TableConfig-AdditionalOptionsEntry) | repeated | Key-value pairs of additional options to pass to the BigQuery API.

Some options, for example, partitionExpirationDays, have dedicated type/validity checked fields. For such options, use the dedicated fields.

String values must be encapsulated in double-quotes, for example: additionalOptions: {numeric_option: &#34;5&#34;, string_option: &#39;&#34;string-value&#34;&#39;}

If the option name contains special characters, encapsulate the name in quotes, for example: additionalOptions: { &#34;option-name&#34;: &#34;value&#34; }. |






<a name="dataform-ActionConfig-TableConfig-AdditionalOptionsEntry"></a>

### ActionConfig.TableConfig.AdditionalOptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="dataform-ActionConfig-TableConfig-LabelsEntry"></a>

### ActionConfig.TableConfig.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="dataform-ActionConfig-Target"></a>

### ActionConfig.Target
Target represents a unique action identifier.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| project | [string](#string) |  | The Google Cloud project (database) of the action. |
| dataset | [string](#string) |  | The dataset (schema) of the action. For notebooks, this is the location. |
| name | [string](#string) |  | The name of the action. |






<a name="dataform-ActionConfig-ViewConfig"></a>

### ActionConfig.ViewConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The name of the view. |
| dataset | [string](#string) |  | The dataset (schema) of the view. |
| project | [string](#string) |  | The Google Cloud project (database) of the view. |
| dependency_targets | [ActionConfig.Target](#dataform-ActionConfig-Target) | repeated | Targets of actions that this action is dependent on. |
| filename | [string](#string) |  | Path to the source file that the contents of the action is loaded from. |
| tags | [string](#string) | repeated | A list of user-defined tags with which the action should be labeled. |
| disabled | [bool](#bool) |  | If set to true, this action will not be executed. However, the action can still be depended upon. Useful for temporarily turning off broken actions. |
| pre_operations | [string](#string) | repeated | Queries to run before `query`. This can be useful for granting permissions. |
| post_operations | [string](#string) | repeated | Queries to run after `query`. |
| materialized | [bool](#bool) |  | Applies the materialized view optimization, see https://cloud.google.com/bigquery/docs/materialized-views-intro. |
| description | [string](#string) |  | Description of the view. |
| columns | [ActionConfig.ColumnDescriptor](#dataform-ActionConfig-ColumnDescriptor) | repeated | Descriptions of columns within the table. |
| labels | [ActionConfig.ViewConfig.LabelsEntry](#dataform-ActionConfig-ViewConfig-LabelsEntry) | repeated | Key-value pairs for BigQuery labels. If the label name contains special characters, e.g. hyphens, then quote its name, e.g. `labels: { &#34;label-name&#34;: &#34;value&#34; }`. |
| additional_options | [ActionConfig.ViewConfig.AdditionalOptionsEntry](#dataform-ActionConfig-ViewConfig-AdditionalOptionsEntry) | repeated | Key-value pairs of additional options to pass to the BigQuery API.

Some options, for example, partitionExpirationDays, have dedicated type/validity checked fields. For such options, use the dedicated fields.

String values must be encapsulated in double-quotes, for example: additionalOptions: {numeric_option: &#34;5&#34;, string_option: &#39;&#34;string-value&#34;&#39;}

If the option name contains special characters, encapsulate the name in quotes, for example: additionalOptions: { &#34;option-name&#34;: &#34;value&#34; }. |






<a name="dataform-ActionConfig-ViewConfig-AdditionalOptionsEntry"></a>

### ActionConfig.ViewConfig.AdditionalOptionsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="dataform-ActionConfig-ViewConfig-LabelsEntry"></a>

### ActionConfig.ViewConfig.LabelsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="dataform-ActionConfigs"></a>

### ActionConfigs
Action configs defines the contents of `actions.yaml` configuration files.
TODO(ekrekr): consolidate these configuration options in the JS API.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| actions | [ActionConfig](#dataform-ActionConfig) | repeated |  |






<a name="dataform-NotebookRuntimeOptionsConfig"></a>

### NotebookRuntimeOptionsConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| output_bucket | [string](#string) |  | Storage bucket to output notebooks to after their execution. |






<a name="dataform-WorkflowSettings"></a>

### WorkflowSettings
Workflow Settings defines the contents of the `workflow_settings.yaml`
configuration file.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dataform_core_version | [string](#string) |  | The desired dataform core version to compile against. |
| default_project | [string](#string) |  | Required. The default Google Cloud project (database). |
| default_dataset | [string](#string) |  | Required. The default dataset (schema). |
| default_location | [string](#string) |  | Required. The default BigQuery location to use. For more information on BigQuery locations, see https://cloud.google.com/bigquery/docs/locations. |
| default_assertion_dataset | [string](#string) |  | Required. The default dataset (schema) for assertions. |
| vars | [WorkflowSettings.VarsEntry](#dataform-WorkflowSettings-VarsEntry) | repeated | Optional. User-defined variables that are made available to project code during compilation. An object containing a list of &#34;key&#34;: value pairs. Example: `{ &#34;name&#34;: &#34;wrench&#34;, &#34;mass&#34;: &#34;1.3kg&#34;, &#34;count&#34;: &#34;3&#34; }`. |
| project_suffix | [string](#string) |  | Optional. The suffix to append to all Google Cloud project references. |
| dataset_suffix | [string](#string) |  | Optional. The suffix to append to all dataset references. |
| name_prefix | [string](#string) |  | Optional. The prefix to append to all action names. |
| default_notebook_runtime_options | [NotebookRuntimeOptionsConfig](#dataform-NotebookRuntimeOptionsConfig) |  | Optional. Default runtime options for Notebook actions. |






<a name="dataform-WorkflowSettings-VarsEntry"></a>

### WorkflowSettings.VarsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |





 

 

 

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

