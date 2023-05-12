import importlib
import importlib.util
import sys
from pathlib import Path
from table import Table
from common import target_to_target_representation
from declaration import Declaration
from assertion import Assertion
import json
import os
from protos.core_pb2 import (
    CompileConfig,
    ProjectConfig,
    TableType,
    Target,
    GraphErrors,
    CompiledGraph,
)
import inspect
from typing import List, Dict
from google.protobuf import json_format
from tarjan import tarjan

ACTION_TYPES = Table | Assertion | Declaration


class Session:
    actions: Dict[str, ACTION_TYPES] = {}
    graph_errors: GraphErrors = GraphErrors()
    project_path = Path()
    project_config: ProjectConfig = ProjectConfig()
    _includes_functions = {}
    _current_action_context: ACTION_TYPES = None

    # This is used to store read `.sql` files in the global variables during nested `eval()` calls.
    # A queue would probably be better, but this fits all purposes currently.
    _stored_sql = ""

    def __init__(self, compile_config: CompileConfig):
        print(f"Running with Python version {sys.version}")

        self.project_path = Path(compile_config.project_dir)
        project_config_file = {}
        with open((self.project_path / "dataform.json").absolute(), "r") as f:
            project_config_file = json.load(f)
        self.project_config = json_format.ParseDict(
            project_config_file, self.project_config
        )

    def load_includes(self):
        includes_path = self.project_path / "includes"
        if not includes_path.exists():
            return
        for file in detect_files(includes_path):
            print("Loading include", file)
            module_name = "includes." + file.stem
            spec = importlib.util.spec_from_file_location(module_name, file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            for name, function in inspect.getmembers(
                module, predicate=inspect.isfunction
            ):
                self._includes_functions[name] = function

    def load_actions(self):
        definitions_files = detect_files(self.project_path / "definitions")

        for path in definitions_files:
            print("Loading definition:", path)
            code = ""
            with open(path.absolute(), "r") as f:
                code = f.read()
            exec(code, self._get_globals(path))

    def compile(self) -> CompiledGraph:
        # Before compiling, replace canonical targets with effectual targets.
        refs_to_replace: Dict[str, str] = {}
        for action in self.actions.values():
            target_representation = action.target_representation()
            canonical_target_representation = action.canonical_target_representation()
            if target_representation != canonical_target_representation:
                refs_to_replace[
                    f"`{canonical_target_representation}`"
                ] = f"`{target_representation}`"
        for action in self.actions.values():
            action.clean_refs(refs_to_replace)

        compiled_graph = CompiledGraph()
        compiled_graph.project_config.CopyFrom(self.project_config)
        compiled_graph.tables.extend(
            [i._proto for i in self.actions.values() if isinstance(i, Table)]
        )
        compiled_graph.declarations.extend(
            [i._proto for i in self.actions.values() if isinstance(i, Declaration)]
        )
        compiled_graph.assertions.extend(
            [i._proto for i in self.actions.values() if isinstance(i, Assertion)]
        )

        self._check_circularity(compiled_graph)
        return compiled_graph

    def _get_globals(self, path: Path) -> Dict:
        return {
            "session": self,
            "table": lambda config_as_map={}: self._add_action_from_definition(
                path, Table, TableType.TABLE, config_as_map
            ),
            "view": lambda config_as_map={}: self._add_action_from_definition(
                path, Table, TableType.VIEW, config_as_map
            ),
            "incremental": lambda config_as_map={}: self._add_action_from_definition(
                path, Table, TableType.INCREMENTAL, config_as_map
            ),
            "declaration": lambda config_as_map={}: self._add_action_from_definition(
                path, Declaration, config_as_map
            ),
            "assertion": lambda config_as_map={}: self._add_action_from_definition(
                path, Assertion, config_as_map
            ),
            "ref": self._ref,
            "store_sql": lambda val="": self._store_sql(val),
            **self._includes_functions,
        }

    def _store_sql(self, val: str):
        self._stored_sql = val

    def _add_action_from_definition(
        self, path: Path, action_class: ACTION_TYPES, *args
    ) -> ACTION_TYPES:
        # This remove references to directories outside of the project directory.
        path = path.relative_to(self.project_path.absolute())
        action = action_class(self.project_config, path, self, *args)
        self._current_action_context = action
        return self._add_action(action)

    def _add_action(self, action: ACTION_TYPES) -> ACTION_TYPES:
        target_representation = action.target_representation()
        if target_representation in self.actions:
            raise Exception(f"Duplicate action: {target_representation}")
        self.actions[target_representation] = action
        return action

    def _ref(self, partial_target_representation: str) -> str:
        target = self._resolve_target_representation(partial_target_representation)
        full_target_representation = target_to_target_representation(target)
        # This is a bit hacky; it's not guaranteed that current action context is set before ref.
        self._current_action_context._add_dependency(target)
        return f"`{full_target_representation}`"

    def _resolve_target_representation(self, target_representation: str) -> Target:
        if target_representation == "":
            raise Exception(f"Empty canonical target")
        target = Target()
        segments = target_representation.split(".")
        segments.reverse()
        if len(segments) > 3:
            raise Exception(
                f"Target representation {target_representation} contains too many segments"
            )
        target.database = (
            segments[2] if len(segments) >= 3 else self.project_config.default_database
        )
        target.schema = (
            segments[1] if len(segments) >= 2 else self.project_config.default_schema
        )
        target.name = segments[0]
        return target

    def _check_circularity(self, compiled_graph):
        # First transform the compiled graph into target representations, which can be used by the
        # tarjan libary.
        representations_to_dependencies: Dict[str, str] = {}
        for action in [
            *compiled_graph.tables,
            *compiled_graph.operations,
            *compiled_graph.assertions,
            # Declarations have no dependencies, so are ignored.
        ]:
            dependency_target_representations = [
                target_to_target_representation(target)
                for target in action.dependency_targets
            ]
            representations_to_dependencies[
                target_to_target_representation(action.target)
            ] = dependency_target_representations
        tarjan_groups: List[List[str]] = tarjan(representations_to_dependencies)
        cycles = [
            strongly_connected_actions
            for strongly_connected_actions in tarjan_groups
            if len(strongly_connected_actions) > 1
        ]
        if len(cycles) >= 1:
            raise Exception(f"Cycle detected. Cycle groups: {cycles}")


def detect_files(path: Path, filtered_suffixes: List[str] = [".py"]) -> List[Path]:
    files: List[Path] = []
    for subdirectory, _, filenames in os.walk(path):
        for filename in filenames:
            file_path = path / os.path.join(subdirectory, filename)
            if file_path.suffix in filtered_suffixes:
                files.append(file_path)
    return files
