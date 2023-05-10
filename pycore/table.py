from typing import Literal, Optional, List, Dict
from dataclasses import dataclass
from protos.core_pb2 import (
    Target,
    Table as TableProto,
    TableType,
)

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


@dataclass
class TableConfig:
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
    _config: TableConfig = None

    # TODO: These should be common across all Action types.
    file_name: str
    target: Target
    dependency_targets: List[Target]

    def __init__(self, table_type: TABLE_TYPE_NAMES, config: TableConfig):
        self._config = config
        # We're able to populate proto fields that don't require context at class initialization.
        self._populate_simple_proto_fields(table_type)
        self._populate_bigquery_fields()

    def _populate_simple_proto_fields(self, table_type: TABLE_TYPE_NAMES):
        if table_type != None:
            if table_type == "incremental":
                self._proto.enum_type = TableType.INCREMENTAL
            elif table_type == "inline":
                self._proto.enum_type = TableType.INLINE
            elif table_type == "table":
                self._proto.enum_type = TableType.TABLE
            elif table_type == "view":
                self._proto.enum_type = TableType.VIEW
            else:
                raise Exception(f"Unrecognized table type: {self._config.type}")
        if "disabled" in self._config:
            self._proto.disabled = self._config.disabled
        if "protected" in self._config:
            self._proto.protected = self._config.protected

    def _populate_bigquery_fields(self):
        """These are options that were previously BigQuery specific."""
        if "partition_by" in self._config:
            self._proto.bigquery.partition_by = self._config.partition_by
        if "cluster_by" in self._config:
            self._proto.bigquery.cluster_by = self._config.cluster_by
        if "additional_options" in self._config:
            # TODO: Expand dynamically.
            self._proto.bigquery.additional_options = self._config.additional_options

    def sql(self, sql: str):
        self._proto.query = sql
