import importlib
import importlib.util
import sys
from pathlib import Path
from table import Table
from common import target_to_target_representation
from declaration import Declaration
import json
from dataclasses import dataclass
import os
from protos.core_pb2 import (
    CompileConfig,
    ProjectConfig,
    Target,
    GraphErrors,
    CompiledGraph,
    CompilationError,
)
import inspect
from typing import List, Literal, Dict
from google.protobuf import json_format
from google.protobuf import text_format
from tarjan import tarjan

ACTION_TYPES = Table


class Session:
    actions: Dict[str, ACTION_TYPES] = {}
    graph_errors: GraphErrors = GraphErrors()
    project_path = Path()
    project_config: ProjectConfig = ProjectConfig()
    _includes_functions = {}
    _current_action_context: ACTION_TYPES = None

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
            _globals = {
                "session": self,
                "table": lambda config_as_map={}: self._add_action(
                    path, Table, "table", config_as_map
                ),
                "view": lambda config_as_map={}: self._add_action(
                    path, Table, "view", config_as_map
                ),
                "incremental": lambda config_as_map={}: self._add_action(
                    path, Table, "incremental", config_as_map
                ),
                "declaration": lambda config_as_map={}: self._add_action(
                    path, Declaration, config_as_map
                ),
                "ref": session._ref,
                **self._includes_functions,
            }
            code = ""
            with open(path.absolute(), "r") as f:
                code = f.read()
            exec(code, _globals)

    def compile(self) -> CompiledGraph:
        compiled_graph = CompiledGraph()
        compiled_graph.project_config.CopyFrom(self.project_config)
        compiled_graph.tables.extend(
            [i._proto for i in self.actions.values() if isinstance(i, Table)]
        )
        compiled_graph.declarations.extend(
            [i._proto for i in self.actions.values() if isinstance(i, Declaration)]
        )

        # This is a pretty hacky way to Replace all outdated refs with the updated target
        # representations.
        compiled_graph_string = json.dumps(json_format.MessageToDict(compiled_graph))
        for action in self.actions.values():
            target_representation = action.target_representation()
            canonical_target_representation = action.canonical_target_representation()
            if target_representation != canonical_target_representation:
                compiled_graph_string = compiled_graph_string.replace(
                    f"`canonical_target_representation`", f"`target_representation`"
                )
        compiled_graph = json_format.ParseDict(
            json.loads(compiled_graph_string), compiled_graph
        )

        self._check_circularity(compiled_graph)
        return compiled_graph

    def _add_action(
        self, path: Path, action_class: ACTION_TYPES, *args
    ) -> ACTION_TYPES:
        action = action_class(self.project_config, path, *args)
        target_representation = action.target_representation()
        if target_representation in self.actions:
            raise Exception(f"Duplicate action: {target_representation}")
        self._current_action_context = action
        self.actions[action.target_representation()] = action
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


if __name__ == "__main__":
    compile_config = CompileConfig()
    compile_config.project_dir = "/usr/local/google/home/eliaskassell/Documents/github/dataform/examples/stackoverflow_bigquery"
    session = Session(compile_config)
    session.load_includes()
    session.load_actions()
    compiled_graph = session.compile()
    # print("Compiled graph:", text_format.MessageToString(compiled_graph))
    compiled_graph_json = json_format.MessageToDict(compiled_graph)
    print("Compiled graph:", json.dumps(compiled_graph_json, indent=4))
    with open(
        "/usr/local/google/home/eliaskassell/Documents/sandbox/sqly-output/python.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(compiled_graph_json, f, indent=4)
