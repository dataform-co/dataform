from typing import Literal, Optional, List, Dict
from dataclasses import dataclass
from protos.core_pb2 import (
    Target,
    Table as TableProto,
    TableType,
    ProjectConfig,
    ActionDescriptor,
)
from common import ActionConfig, action_target, target_to_target_representation
from pathlib import Path

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
        unique_key: Optional[List[str]]: Unique keys for merge criteria for incremental tables.
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
    unique_key: Optional[List[str]] = None
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
    def __init__(
        self,
        project_config: ProjectConfig,
        path: Path,
        table_type: TABLE_TYPE_NAMES,
        table_config_as_map: TableConfig,
    ):
        # TODO: Validate table config.
        self._proto = TableProto()
        self._table_config = TableConfig(**table_config_as_map)
        self.dependency_targets: List[Target] = []

        self._populate_proto_fields(table_type)
        self._populate_bigquery_proto_fields()

        # Canonical target is based off of the file structure, and is guaranteed to be unique.
        self._proto.canonical_target.CopyFrom(action_target(project_config, path.stem))

        # Target is the final resolved target, and can be overridden by the table config.
        self._proto.target.CopyFrom(
            action_target(
                project_config,
                path.stem,
                self._table_config.database,
                self._table_config.schema,
                self._table_config.name,
            )
        )

    def _populate_proto_fields(self, table_type: TABLE_TYPE_NAMES):
        if table_type != None:
            if table_type == "incremental":
                self._proto.enum_type = TableType.INCREMENTAL
            elif table_type == "table":
                self._proto.enum_type = TableType.TABLE
            elif table_type == "view":
                self._proto.enum_type = TableType.VIEW
            else:
                raise Exception(f"Unrecognized table type: {self._table_config.type}")
        # TODO: Propagate unique key assertions from disabled.
        self._proto.disabled = bool(self._table_config.disabled)
        if self._table_config.protected:
            self._proto.protected = self._table_config.protected
        if self._table_config.tags:
            self._proto.tags.extend(self._table_config.tags)
        # if self._table_config.description:
        #     self._proto.description = self._table_config.description
        if self._table_config.description:
            self._proto.action_descriptor.description = self._table_config.description
        # TODO: Check for columns.
        # TODO: Check for assertions.
        if self._table_config.unique_key:
            self._proto.unique_key = self._table_config.uniqueKey
        if self._table_config.materialized:
            self._proto.materialized = self._table_config.materialized

    def _populate_bigquery_proto_fields(self):
        """These are options that were previously BigQuery specific."""
        if self._table_config.partition_by:
            self._proto.bigquery.partition_by = self._table_config.partition_by
        if self._table_config.cluster_by:
            self._proto.bigquery.cluster_by = self._table_config.cluster_by
        if self._table_config.update_partition_filter:
            self._proto.bigquery.update_partition_filter = (
                self._table_config.update_partition_filter
            )
        if self._table_config.labels:
            self._proto.bigquery.labels = self._table_config.labels
        if self._table_config.partition_expiration_days:
            self._proto.bigquery.partition_expiration_days = (
                self._table_config.partition_expiration_days
            )
        if self._table_config.require_partition_filter:
            self._proto.bigquery.require_partition_filter = (
                self._table_config.require_partition_filter
            )
        if self._table_config.additional_options:
            # TODO: Expand dynamically.
            self._proto.bigquery.additional_options = (
                self._table_config.additional_options
            )
        if self._table_config.labels:
            self._proto.action_descriptor.bigquery_labels = self._table_config.labels

    def sql(self, sql: str):
        self._proto.query = sql

    def _add_dependency(self, target: Target):
        self._proto.dependency_targets.append(target)

    def target_representation(self):
        return target_to_target_representation(self._proto.target)

    def canonical_target_representation(self):
        return target_to_target_representation(self._proto.canonical_target)
