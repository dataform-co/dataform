import importlib
import importlib.util
import sys
from pathlib import Path
from table import Table
import json
from dataclasses import dataclass
import os
from protos.core_pb2 import (
    CompileConfig,
    ProjectConfig,
    Target,
    GraphErrors,
    CompilationError,
)
import inspect
from typing import List, Literal, Dict
from google.protobuf.json_format import MessageToDict
from google.protobuf import json_format

ACTION_TYPES = Table


class Session:
    actions: list[ACTION_TYPES] = []
    graph_errors: GraphErrors = GraphErrors()
    project_dir = Path()
    project_config: ProjectConfig = ProjectConfig()
    _includes_functions = {}
    _current_action_context: ACTION_TYPES = None

    def __init__(self, compile_config: CompileConfig):
        print(f"Running with Python version {sys.version}")

        self.project_dir = Path(compile_config.project_dir)
        project_config_file = {}
        with open((self.project_dir / "dataform.json").absolute(), "r") as f:
            project_config_file = json.load(f)
        self.project_config = json_format.ParseDict(
            project_config_file, self.project_config
        )

    def load_includes(self):
        for file in detect_python_files(self.project_dir / "includes"):
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
        definitions_files = detect_python_files(self.project_dir / "definitions")

        # First load all actions, populating proto properties that aren't dynamic given the context.
        # Without this as a first step, we wouldn't know what tables to reference in subsequent
        # steps.
        for path in definitions_files:
            print("Loading definition:", path)
            _globals = {
                "session": self,
                "table": lambda config_as_map: self._add_action(
                    path, Table, "table", config_as_map
                ),
                "view": lambda config_as_map: self._add_action(
                    path, Table, "view", config_as_map
                ),
                "incremental": lambda config_as_map: self._add_action(
                    path, Table, "incremental", config_as_map
                ),
                "ref": session._ref,
                **self._includes_functions,
            }
            code = ""
            with open(path.absolute(), "r") as f:
                code = f.read()
            exec(f"{code}", _globals)

        print("ACTIONS:", session.actions)
        print("ACTION 0 TARGET:", session.actions[0]._proto.target)
        print("ACTION 0 DEPS 0:", session.actions[0]._proto.dependency_targets)
        print("ACTION 1 DEPS 0:", session.actions[1]._proto.dependency_targets)
        # Then compile all SQL, now that we have access to the full context.
        # TODO: Do we actually need to do this step?

    def _add_action(
        self, path: Path, action_class: ACTION_TYPES, *args
    ) -> ACTION_TYPES:
        action = action_class(self.project_config, path.stem, *args)
        self._current_action_context = action
        self.actions.append(action)
        return action

    def _ref(self, canonical_target: str) -> str:
        target = self._resolve_canonical_target(canonical_target)
        # This is a bit hacky; it's not guaranteed that current action context is set before ref.
        return self._current_action_context._add_dependency(target)

    def _resolve_canonical_target(self, canonical_target: str) -> Target:
        if canonical_target == "":
            raise Exception(f"Empty canonical target")
        target = Target()
        segments = canonical_target.split(".")
        segments.reverse()
        if len(segments) > 3:
            raise Exception(
                f"Canonical target {canonical_target} contains too many segments"
            )
        target.database = (
            segments[2] if len(segments) >= 3 else self.project_config.default_database
        )
        target.schema = (
            segments[1] if len(segments) >= 2 else self.project_config.default_schema
        )
        target.name = segments[0]
        return target


def detect_python_files(directory: Path) -> List[Path]:
    return [
        directory / f
        for f in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, f)) and f.endswith(".py")
    ]


if __name__ == "__main__":
    compile_config = CompileConfig()
    compile_config.project_dir = (
        "/usr/local/google/home/eliaskassell/Documents/github/dataform/examples/actions"
    )
    session = Session(compile_config)
    session.load_includes()
    session.load_actions()
