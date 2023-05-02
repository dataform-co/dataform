import importlib
import importlib.util
import sys
from pathlib import Path
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


def main(compile_config: CompileConfig):
    print(f"Running with Python version {sys.version}")

    project_dir = Path(compile_config.project_dir)

    project_config = {}
    with open((project_dir / "dataform.json").absolute(), "r") as f:
        project_config = json.load(f)

    includes_path = project_dir / "includes"
    includes_files = [
        includes_path / f
        for f in os.listdir(includes_path)
        if os.path.isfile(os.path.join(includes_path, f)) and f.endswith(".py")
    ]
    print("Includes files found:", includes_files)
    for file in includes_files:
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

    print("INCLUDES TEST:", foo("test"))


@dataclass
class Action:
    fileName: str
    target: Target
    dependencyTargets: list[Target]


class Session:
    actions: list[Action] = []
    graph_errors: GraphErrors = GraphErrors()

    def __init__(self, root_directory: Path, project_config: ProjectConfig):
        self.root_directory = root_directory
        self.project_config = project_config


if __name__ == "__main__":
    compile_config = CompileConfig()
    compile_config.project_dir = "/usr/local/google/home/eliaskassell/Documents/github/dataform/examples/common_v2"
    main(compile_config)
