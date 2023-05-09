import importlib
import importlib.util
import sys
from pathlib import Path
from table import Table, table
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
from typing import List, Literal

ACTION_TYPES = Table


class Session:
    actions: list[ACTION_TYPES] = []
    graph_errors: GraphErrors = GraphErrors()
    project_dir = Path()
    project_config: ProjectConfig = {}

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
            sys.modules[module_name] = module
            global foo
            # Can't just assign this for some reason, have to do hacky lambda.
            foo = lambda *arg: module.foo(*arg)
            spec.loader.exec_module(module)

            # TODO: Do global variable expansion for all methods. This could look like:

        #         for member in inspect.getmembers(module, predicate=inspect.isfunction):
        #             exec(
        #                 f"""
        # global {member}
        # # Can't just assign this for some reason, have to do hacky lambda.
        # {member} = lambda *arg: module.foo(*arg)
        # spec.loader.exec_module(module)
        # print("INCLUDES TEST 1:", foo("test"))
        #             """
        #             )

    def load_actions(self):
        definitions_files = detect_python_files(self.project_dir / "definitions")

        # Attach construction methods to global Python session.
        # globals()["table"] = table

        # print("GLOBALS IN LOAD ACTION:", globals())

        # First load all actions, including proto properties that aren't dynamic given the context.
        # Without this as a first step, we wouldn't know what tables to reference in subsequent
        # steps.
        for file in definitions_files:
            print("LOADING:", file)
            # module_name = "definitions." + file.stem
            # spec = importlib.util.spec_from_file_location(module_name, file)
            # module = importlib.util.module_from_spec(spec)
            # sys.modules[module_name] = module
            # # Because functions in the `.py` are at the base level of the file, they are run.
            # spec.loader.exec_module(module)

            _locals = locals()
            _globals = {"table": table, "session": self, "result": None}
            code = ""
            with open(file.absolute(), "r") as f:
                code = f.read()
            exec(f"{code}", _globals, _locals)
            # print("TEST TMP:", _locals)
            print("ACTION:", _globals)

        # Then compile all SQL, now that we have access to the full context.
        pass

    def add_action(self, action: ACTION_TYPES):
        print("ADDING ACTION:", action)
        self.actions += action


def ref(canonical_target: str) -> str:
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
