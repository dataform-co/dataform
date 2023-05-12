from __future__ import annotations

from typing import Literal, Optional, List, Dict
from dataclasses import dataclass
from protos.core_pb2 import (
    Target,
    Table as TableProto,
    TableType,
    ProjectConfig,
    ColumnDescriptor,
)
from common import (
    ActionConfig,
    action_target,
    target_to_target_representation,
    efficient_replace_string,
)
from pathlib import Path
from assertion import Assertion
import adapter
from google.protobuf import json_format


# TODO: These are all snake case, but the current open source uses camel case. We shouldn't
# necessarily convert them to camel case however.
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

    type: Optional[TableType] = None
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
        session,  # TODO: Can't import this type because cirular dependency; maybe spread session?
        table_type: TableType,
        table_config_as_map: Dict,
    ):
        self._project_config = project_config
        self._path = path
        self._session = session
        self._table_type = table_type
        self._table_config = TableConfig(**table_config_as_map)

        self._proto = TableProto()

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

        self._populate_proto_fields()
        self._add_assertions()
        self._populate_bigquery_proto_fields()

    def _populate_proto_fields(self):
        if TableType == TableType.UNKNOWN_TYPE:
            raise Exception(f"Unknown table type: {self._table_config.type}")
        else:
            self._proto.enum_type = self._table_type
        self._proto.disabled = bool(self._table_config.disabled)
        if self._table_config.protected:
            self._proto.protected = self._table_config.protected
        if self._table_config.tags:
            self._proto.tags.extend(self._table_config.tags)
        if self._table_config.description:
            self._proto.action_descriptor.description = self._table_config.description
        if self._table_config.columns:
            self._proto.action_descriptor.columns.extend(
                [
                    json_format.ParseDict(column, ColumnDescriptor())
                    for column in self._table_config.columns
                ]
            )
        if self._table_config.unique_key:
            self._proto.unique_key = self._table_config.uniqueKey
        if self._table_config.materialized:
            self._proto.materialized = self._table_config.materialized
        if self._table_config.dependencies:
            self._proto.dependency_targets.extend(
                [
                    self._session._resolve_target_representation(target_representation)
                    for target_representation in self._table_config.dependencies
                ]
            )
        self._proto.file_name = str(self._path)

    def _add_assertions(self):
        if self._table_config.assertions:
            assertions = TableAssertions(**self._table_config.assertions)
            if assertions.unique_key and assertions.unique_keys:
                raise Exception(
                    "Specify at most one of 'assertions.unique_key' and 'assertions.unique_keys'."
                )
            if assertions.unique_key:
                assertions.unique_keys = [assertions.unique_key]
            assertion_config_to_share = {
                "tags": self._proto.tags,
                "disabled": self._proto.disabled,
                "dependencies": [self.target_representation()],
            }
            for unique_key in assertions.unique_keys:
                self._session._add_action(
                    Assertion(
                        self._project_config,
                        self._path.parent
                        # TODO: Confirm this is an improvement: it differs from previous
                        # implementation, but facilitates multiple unique key assertions in the same
                        # file.
                        / f"{self._proto.target.name}_assertions_unique_key_{'_'.join(unique_key)}",
                        self._session,
                        assertion_config_to_share,
                    ).sql(
                        adapter.index_assertion(
                            self.target_representation(), unique_key
                        )
                    )
                )

            merged_row_conditions = assertions.row_conditions or []
            if assertions.non_null:
                merged_row_conditions.extend(
                    [f"{col} IS NOT NULL" for col in assertions.non_null]
                )
            if len(merged_row_conditions) > 0:
                self._session._add_action(
                    Assertion(
                        self._project_config,
                        self._path.parent
                        / f"{self._path.stem}_assertions_rowConditions",
                        self._session,
                        assertion_config_to_share,
                    ).sql(
                        adapter.row_conditions_assertion(
                            self.target_representation(), merged_row_conditions
                        )
                    )
                )

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

    def sql(self, sql: str) -> "Table":
        if self._proto.query:
            raise Exception("SQL is already defined")
        self._proto.query = sql
        if self._proto.enum_type == TableType.INCREMENTAL:
            self._proto.incremental_query = sql
        # TODO: Validate query strings (remove semi-colon at end), or no longer necessary?
        return self

    def pre_operations(self, sqls: List[str]) -> "Table":
        self._proto.pre_ops.extend(sqls)
        if self._proto.enum_type == TableType.INCREMENTAL:
            self._proto.incremental_pre_ops.extend(sqls)
        return self

    def post_operations(self, sqls: List[str]) -> "Table":
        self._proto.post_ops.extend(sqls)
        if self._proto.enum_type == TableType.INCREMENTAL:
            self._proto.incremental_post_ops.extend(sqls)
        return self

    def load_sql_file(self, sql_file_path_as_str: str) -> "Table":
        self.sql(self._session._load_sql_file(sql_file_path_as_str))
        return self

    def _add_dependency(self, target: Target):
        self._proto.dependency_targets.append(target)

    def target_representation(self):
        return target_to_target_representation(self._proto.target)

    def canonical_target_representation(self):
        return target_to_target_representation(self._proto.canonical_target)

    def clean_refs(self, refs_to_replace: Dict[str, str]):
        if not refs_to_replace:
            return
        self._proto.query = efficient_replace_string(refs_to_replace, self._proto.query)
        # TODO: Clean pre-ops and post-ops.
