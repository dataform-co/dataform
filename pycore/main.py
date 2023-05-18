import json
from protos.core_pb2 import (
    CompileConfig,
)
from typing import List, Literal, Dict
from google.protobuf import json_format
import time
from session import Session
import sys
from pathlib import Path


if __name__ == "__main__":
    start = time.time()
    print("Compiling...")

    compile_config = CompileConfig()

    # This provides a temporary entrypoint for demoing.
    example_project_name = sys.argv[1]
    if not example_project_name:
        raise Exception("Name of example project required.")
    example_project_path = Path.cwd() / "examples" / example_project_name
    if not example_project_path.exists():
        raise Exception(
            f"Example project '{example_project_path}' does not exist. Check the examples directory."
        )

    compile_config.project_dir = str(example_project_path)

    session = Session(compile_config)
    session.load_includes()
    session.load_actions()
    compiled_graph = session.compile()

    compiled_graph_json = json_format.MessageToDict(compiled_graph)

    total_compilation_time = time.time() - start

    print("Compiled graph:")
    print(json.dumps(compiled_graph_json, indent=4))
    # with open(
    #     "/usr/local/google/home/eliaskassell/Documents/sandbox/sqly-output/python.json",
    #     "w",
    #     encoding="utf-8",
    # ) as f:
    #     json.dump(compiled_graph_json, f, indent=4)

    print("Compiled graph size:", compiled_graph.ByteSize())
    print(f"Total compilation time: {total_compilation_time}s")

    # print("All actions made:")
    # for action in [
    #     *compiled_graph.tables,
    #     *compiled_graph.operations,
    #     *compiled_graph.assertions,
    #     *compiled_graph.declarations,
    # ]:
    #     # print(target_to_target_representation(action.target))
    #     print(action.file_name)
