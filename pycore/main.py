import json
from protos.core_pb2 import (
    CompileConfig,
)
from typing import List, Literal, Dict
from google.protobuf import json_format
import time
from session import Session


if __name__ == "__main__":
    start = time.time()
    print("Compiling...")

    compile_config = CompileConfig()
    compile_config.project_dir = "/usr/local/google/home/eliaskassell/Documents/github/dataform/examples/simple_load_sql"
    session = Session(compile_config)
    session.load_includes()
    session.load_actions()
    compiled_graph = session.compile()
    print(f"Total compilation time: {time.time() - start}s")

    compiled_graph_json = json_format.MessageToDict(compiled_graph)
    print("Compiled graph size:", compiled_graph.ByteSize())

    print("Compiled graph:")
    print(json.dumps(compiled_graph_json, indent=4))
    # with open(
    #     "/usr/local/google/home/eliaskassell/Documents/sandbox/sqly-output/python.json",
    #     "w",
    #     encoding="utf-8",
    # ) as f:
    #     json.dump(compiled_graph_json, f, indent=4)

    # print("All actions made:")
    # for action in [
    #     *compiled_graph.tables,
    #     *compiled_graph.operations,
    #     *compiled_graph.assertions,
    #     *compiled_graph.declarations,
    # ]:
    #     # print(target_to_target_representation(action.target))
    #     print(action.file_name)
