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

ACTION_TYPES = Table


class Session:
    actions: list[ACTION_TYPES] = []
    graph_errors: GraphErrors = GraphErrors()
    project_dir = Path()
    project_config: ProjectConfig = {}
    _includes_functions = {}

    def __init__(self, compile_config: CompileConfig):
        print(f"Running with Python version {sys.version}")

        self.project_dir = Path(compile_config.project_dir)
        project_config = {}
        with open((self.project_dir / "dataform.json").absolute(), "r") as f:
            project_config = json.load(f)

        # Project config overrides take precedence over the `dataform.json` file.
        self.project_config = {
            **project_config,
            # TODO: Convert project config override proto to mapping to make this work.
            #     **compile_config.project_config_override,
        }

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
        for file in definitions_files:
            print("Loading definition:", file)
            _globals = {
                "session": self,
                "table": lambda config_as_map: self._add_action(
                    Table("table", config_as_map)
                ),
                "view": lambda config_as_map: self._add_action(
                    Table("view", config_as_map)
                ),
                "incremental": lambda config_as_map: self._add_action(
                    Table("incremental", config_as_map)
                ),
                "ref": session._ref,
                **self._includes_functions,
            }
            code = ""
            with open(file.absolute(), "r") as f:
                code = f.read()
            exec(f"{code}", _globals)

        print("ACTIONS:", session.actions)
        # Then compile all SQL, now that we have access to the full context.
        # TODO: Do we actually need to do this step?

    def _add_action(self, action: ACTION_TYPES) -> ACTION_TYPES:
        self.actions.append(action)
        return action

    def _ref(self, canonical_target: str) -> str:
        return "test_resolve_target"


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
