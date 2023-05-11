from common import (
    ActionConfig,
    action_target,
    target_to_target_representation,
    efficient_replace_string,
)
from dataclasses import dataclass
from pathlib import Path
from protos.core_pb2 import (
    Target,
    Assertion as AssertionProto,
    ProjectConfig,
    ActionDescriptor,
)
from typing import Literal, Optional, List, Dict


# TODO: Override argument descriptions for docs usage; see original assertions file.
@dataclass
class AssertionConfig(ActionConfig):
    """
    A data class that represents declaration configuration options.
    """


class Assertion:
    def __init__(
        self,
        project_config: ProjectConfig,
        path: Path,
        session,
        assertion_config_as_map: Dict,
    ):
        self._project_config = project_config
        self._path = path
        self._session = session
        self._assertion_config = AssertionConfig(**assertion_config_as_map)

        self._proto = AssertionProto()

        # Canonical target is based off of the file structure, and is guaranteed to be unique.
        self._proto.canonical_target.CopyFrom(action_target(project_config, path.stem))

        # Target is the final resolved target, and can be overridden by the table config.
        self._proto.target.CopyFrom(
            action_target(
                project_config,
                path.stem,
                self._assertion_config.database,
                self._assertion_config.schema,
                self._assertion_config.name,
            )
        )

        self._populate_proto_fields()

    def sql(self, sql: str) -> "Assertion":
        self._proto.query = sql
        return self

    def _populate_proto_fields(self):
        if self._assertion_config.description:
            self._proto.action_descriptor.description = (
                self._assertion_config.description
            )
        if self._assertion_config.dependencies:
            self._proto.dependency_targets.extend(
                [
                    self._session._resolve_target_representation(target_representation)
                    for target_representation in self._assertion_config.dependencies
                ]
            )
        self._proto.disabled = bool(self._assertion_config.disabled)
        if self._assertion_config.tags:
            self._proto.tags.extend(self._assertion_config.tags)
        if self._assertion_config.description:
            self._proto.action_descriptor.description = (
                self._assertion_config.description
            )
        self._proto.file_name = str(self._path)

    def target_representation(self):
        return target_to_target_representation(self._proto.target)

    def canonical_target_representation(self):
        return target_to_target_representation(self._proto.canonical_target)

    def clean_refs(self, refs_to_replace: Dict[str, str]):
        self._proto.query = efficient_replace_string(refs_to_replace, self._proto.query)
