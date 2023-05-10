from common import ActionConfig, action_target, target_to_target_representation
from dataclasses import dataclass
from pathlib import Path
from protos.core_pb2 import (
    Target,
    Declaration as DeclarationProto,
    ProjectConfig,
    ActionDescriptor,
)
from typing import Literal, Optional, List, Dict


@dataclass
class DeclarationConfig(ActionConfig):
    """
    A data class that represents declaration configuration options.
    """


class Declaration:
    def __init__(
        self,
        project_config: ProjectConfig,
        path: Path,
        declaration_config_as_map: DeclarationConfig,
    ):
        # TODO: Validate table config.
        self._proto = DeclarationProto()
        self._table_config = DeclarationConfig(**declaration_config_as_map)

        self._populate_proto_fields()

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

    def _populate_proto_fields(self):
        self._proto.action_descriptor.description = self._table_config.description

    def target_representation(self):
        return target_to_target_representation(self._proto.target)

    def canonical_target_representation(self):
        return target_to_target_representation(self._proto.canonical_target)
