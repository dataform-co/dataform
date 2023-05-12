from __future__ import annotations

from typing import Optional, List, Dict
from dataclasses import dataclass
from protos.core_pb2 import (
    Target,
    Operation as OperationProto,
    ProjectConfig,
)
from common import (
    ActionConfig,
    action_target,
    target_to_target_representation,
    efficient_replace_string,
)
from pathlib import Path
import traceback


# TODO: Override argument descriptions for docs usage; see original assertions file.
@dataclass
class OperationConfig(ActionConfig):
    """
    A data class that represents declaration configuration options.

    Args:
        has_output: Declares that this `operations` action creates a dataset which should be referenceable using the `ref` function.
            If set to true, this action should create a dataset with its configured name, using the `self()` context function.
    """

    has_output: Optional[bool] = False


class Operation:
    def __init__(
        self,
        project_config: ProjectConfig,
        path: Path,
        session,
        operation_config_as_map: Dict,
    ):
        self._project_config = project_config
        self._path = path
        self._session = session
        self._operation_config = OperationConfig(**operation_config_as_map)

        self._proto = OperationProto()

        # Canonical target is based off of the file structure, and is guaranteed to be unique.
        self._proto.canonical_target.CopyFrom(action_target(project_config, path.stem))

        # Target is the final resolved target, and can be overridden by the table config.
        self._proto.target.CopyFrom(
            action_target(
                project_config,
                path.stem,
                self._operation_config.database,
                self._operation_config.schema,
                self._operation_config.name,
            )
        )

        self._populate_proto_fields()

    def _populate_proto_fields(self):
        if self._operation_config.dependencies:
            self._proto.dependency_targets.extend(
                [
                    self._session._resolve_target_representation(target_representation)
                    for target_representation in self._operation_config.dependencies
                ]
            )
        self._proto.disabled = bool(self._operation_config.disabled)
        if self._operation_config.tags:
            self._proto.tags.extend(self._operation_config.tags)
        if self._operation_config.description:
            self._proto.action_descriptor.description = (
                self._operation_config.description
            )
        # TODO: Handle columns.
        if self._operation_config.has_output:
            self._proto.has_output = self._operation_config.has_output
        if self._operation_config.columns:
            try:
                raise Exception()
            except Exception:
                self._session.report_compilation_error(
                    self._path,
                    self._proto.target,
                    "Actions of type 'operation' may only describe columns if they specify 'has_output: true'.",
                    # This isn't the idea way of getting a stack trace, but it does work.
                    traceback.format_exc(),
                )

    def queries(self, sqls: List[str]):
        self._proto.queries.extend(sqls)

    def target_representation(self):
        return target_to_target_representation(self._proto.target)

    def canonical_target_representation(self):
        return target_to_target_representation(self._proto.canonical_target)

    def clean_refs(self, refs_to_replace: Dict[str, str]):
        if not refs_to_replace:
            return
        cleaned_queries = [
            efficient_replace_string(refs_to_replace, query)
            for query in self._proto.queries
        ]
        del self._proto.queries[:]
        self._proto.queries.extend(cleaned_queries)
