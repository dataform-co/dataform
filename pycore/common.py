from dataclasses import dataclass
from typing import Literal, Optional, List, Dict
from protos.core_pb2 import Target, Table as TableProto, TableType, ProjectConfig


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
    name: Optional[str] = None

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


def action_target(
    project_config: ProjectConfig,
    name: str,
    database_override: Optional[str] = None,
    schema_override: Optional[str] = None,
    name_override: Optional[str] = None,
):
    target = Target()
    target.database = (
        database_override if database_override else project_config.default_database
    )
    target.schema = (
        schema_override if schema_override else project_config.default_schema
    )
    target.name = name_override if name_override else name
    return target


def target_to_target_representation(target: Target) -> str:
    """
    Converts a Target (proto) to a target representation (string), e.g. `database.schema.name`.
    """
    return f"{target.database}.{target.schema}.{target.name}"
