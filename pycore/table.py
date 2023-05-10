from typing import Literal, Optional, List, Dict
from dataclasses import dataclass
from protos.core_pb2 import Target, Table as TableProto, TableType, ProjectConfig
from google.protobuf.any_pb2 import Any

TABLE_TYPE_NAMES = Literal["table", "incremental", "view", "inline"]


@dataclass
class TableAssertions:
    """
    A data class that represents table assertions.

    Args:
        unique_key: The column(s) which constitute the dataset's unique key index.
        unique_keys: Combinations of column(s), each of which should constitute a unique key index for the dataset.
        non_null: The column(s) which may never be `NULL`.
        row_conditions: General condition(s) which should hold true for all rows in the dataset.
    """

    unique_key: Optional[List[str]] = None  # TODO: Can be just str type too.
    unique_keys: Optional[List[List[str]]] = None
    non_null: Optional[List[str]] = None  # TODO: Can be just str type too.
    row_conditions: Optional[List[str]] = None


# TODO: Move to shared location.
@dataclass
class ActionConfig:
    """
    A dataclass that represents generic action configuration options.

    Args:
        # From target interface.
        database: The database in which the output of this action should be created.
        schema: The schema in which the output of this action should be created.

        # From record descriptor interface.
        description: A description of the struct, object or record.
        columns: A description of columns within the struct, object or record.
        displayName: A human-readable name for the column.
        dimension: The type of the column. Can be `category`, `timestamp` or `number`.
        aggregator: The type of aggregator to use for the column. Can be `sum`, `distinct` or `derived`.
        expression: The expression to use for the column.
        tags: Tags that apply to this column (experimental).
        bigqueryPolicyTags: BigQuery policy tags that should be applied to this column.

        # From action config interface.
        tags: A list of user-defined tags with which the action should be labeled.
        dependencies: Dependencies of the action.
        disabled: If set to true, this action will not be executed. However, the action may still be depended upon. Useful for temporarily turning off broken actions.
    """

    database: Optional[str] = None
    schema: Optional[str] = None

    description: Optional[str] = None
    # columns: Optional[IColumnsDescriptor] = None
    displayName: Optional[str] = None
    dimension: Optional[str] = None
    aggregator: Optional[str] = None
    expression: Optional[str] = None
    tags: Optional[List[str]] = None
    bigqueryPolicyTags: Optional[List[str]] = None

    tags: Optional[List[str]] = None
    dependencies: Optional[List[str]] = None
    disabled: Optional[bool] = None


@dataclass
class TableConfig(ActionConfig):
    """
    A data class that represents table configuration options.

    Args:
        type: The type of the dataset. For more information on how this setting works, check out some of the [guides](guides) on publishing different types of datasets with Dataform.
        protected: Optional[bool]: Only allowed when the table type is `incremental`.
            If set to true, running this action will ignore the full-refresh option.
            This is useful for tables which are built from transient data, to ensure that historical data is never lost.
        assertions: Optional[TableAssertions]: Assertions to be run on the dataset.
            If configured, relevant assertions will automatically be created and run as a dependency of this dataset.
        uniqueKey: Optional[List[str]]: Unique keys for merge criteria for incremental tables.
            If configured, records with matching unique key(s) will be updated, rather than new rows being inserted.
        materialized: Only valid when the table type is `view`.
            If set to true, will make the view materialized.
            For more information, read the [BigQuery materialized view docs](https://cloud.google.com/bigquery/docs/materialized-views-intro).

        partition_by: The key with which to partition the table. Typically the name of a timestamp or date column.
        cluster_by: The keys by which to cluster partitions by.
        update_partition_filter: SQL based filter for when incremental updates are applied.
        labels: Key-value pairs for [BigQuery labels](https://cloud.google.com/bigquery/docs/labels-intro).
        partition_expiration_days: This setting specifies how long BigQuery keeps the data in each partition.
        require_partition_filter: When you create a partitioned table, you can require that all queries on the table must include a predicate filter (a WHERE clause) that filters on the partitioning column.

        additional_options: Key-value pairs for options [table](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list), [view](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#view_option_list), [materialized view](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#materialized_view_option_list).
    """

    type: Optional[TABLE_TYPE_NAMES] = None
    protected: Optional[bool] = None
    assertions: Optional[TableAssertions] = None
    uniqueKey: Optional[List[str]] = None
    materialized: Optional[bool] = None

    # These were originally BigQuery specific options.
    partition_by: Optional[str] = None
    cluster_by: Optional[List[str]] = None
    update_partition_filter: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    partition_expiration_days: Optional[int] = None
    require_partition_filter: Optional[bool] = None

    # Additional options can be used by hookable adapters.
    additional_options: Optional[Dict[str, str]] = None


class Table:
    _proto = TableProto()
    _proto.enum_type = TableType.VIEW
    _table_config: TableConfig = None

    # TODO: These should be common across all Action types.
    file_name: str
    target: Target
    dependency_targets: List[Target]

    def __init__(
        self,
        project_config: ProjectConfig,
        name: str,
        table_type: TABLE_TYPE_NAMES,
        table_config_as_map: TableConfig,
    ):
        self._table_config = TableConfig(**table_config_as_map)
        # We're able to populate proto fields that don't require context at class initialization.
        self._populate_simple_proto_fields(table_type)
        self._populate_bigquery_fields()

        print("DATABASE FIELD:", self._table_config.database)

        target = Target()
        target.database = (
            self._table_config.database
            if self._table_config.database
            else project_config.default_database
        )
        target.schema = (
            self._table_config.schema
            if self._table_config.schema
            else project_config.default_schema
        )
        target.name = name
        self._proto.target.CopyFrom(target)

    def _populate_simple_proto_fields(self, table_type: TABLE_TYPE_NAMES):
        if table_type != None:
            if table_type == "incremental":
                self._proto.enum_type = TableType.INCREMENTAL
            elif table_type == "table":
                self._proto.enum_type = TableType.TABLE
            elif table_type == "view":
                self._proto.enum_type = TableType.VIEW
            else:
                raise Exception(f"Unrecognized table type: {self._table_config.type}")
        if self._table_config.disabled:
            self._proto.disabled = self._table_config.disabled
        if self._table_config.protected:
            self._proto.protected = self._table_config.protected

    def _populate_bigquery_fields(self):
        """These are options that were previously BigQuery specific."""
        if self._table_config.partition_by:
            self._proto.bigquery.partition_by = self._table_config.partition_by
        if self._table_config.cluster_by:
            self._proto.bigquery.cluster_by = self._table_config.cluster_by
        if self._table_config.additional_options:
            # TODO: Expand dynamically.
            self._proto.bigquery.additional_options = (
                self._table_config.additional_options
            )

    def sql(self, sql: str):
        self._proto.query = sql

    def _add_dependency(self, target: Target):
        self._proto.dependency_targets.append(target)
