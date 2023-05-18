import json
from protos.core_pb2 import (
    CompileConfig,
    CompiledGraph,
)
from common import target_to_target_representation
from typing import List, Literal, Dict
from google.protobuf import json_format
import time
from session import Session
import sys
from pathlib import Path

import argparse


# def require_args(activity, present_args, required_args):
#     for arg in required_args:
#         if arg not in present_args:
#             parser.error(f"{activity} requires the argument {arg}.")


def build_parser():
    parser = argparse.ArgumentParser(description="Dataform CLI")

    parser.add_argument("activity", choices=["compile"], help="Activity to perform.")

    # TODO: Require directory exists.
    parser.add_argument(
        "-d",
        "--project-dir",
        type=str,
        default="examples/stackoverflow_bigquery",
        help="The Dataform project directory.",
    )

    # parser.add_argument(
    #     "-f",
    #     "--full-refresh",
    #     action="store_true",
    #     help="Forces incremental tables to be rebuilt from scratch.",
    # )
    # parser.add_argument(
    #     "-a",
    #     "--actions",
    #     type=str,
    #     nargs="*",
    #     help="A list of action names or patterns to run. Can include " * " wildcards.",
    # )
    # parser.add_argument(
    #     "-t",
    #     "--tags",
    #     type=str,
    #     nargs="*",
    #     help="A list of tags to filter the actions to run.",
    # )
    # parser.add_argument(
    #     "-i",
    #     "--include-deps",
    #     action="store_true",
    #     help="If set, dependencies for selected actions will also be run.",
    # )
    # parser.add_argument(
    #     "-d",
    #     "--include-dependents",
    #     action="store_true",
    #     help="If set, dependents (downstream) for selected actions will also be run.",
    # )
    # parser.add_argument(
    #     "-s",
    #     "--schema-suffix",
    #     type=str,
    #     help="A suffix to be appended to output schema names.",
    # )
    # parser.add_argument(
    #     "-c",
    #     "--credentials",
    #     type=str,
    #     default="credentials.json",
    #     help="The location of the credentials JSON file to use.",
    # )
    # parser.add_argument(
    #     "-w",
    #     "--warehouse",
    #     type=str,
    #     choices=["BigQuery", "Snowflake", "Redshift"],
    #     help="The project's data warehouse type.",
    # )
    parser.add_argument(
        "-j",
        "--json",
        action="store_true",
        help="Outputs a JSON representation of the compiled project.",
        default=False,
    )
    # parser.add_argument(
    #     "-v",
    #     "--vars",
    #     type=str,
    #     help='Variables to inject via "--${varsOptionName}=someKey=someValue,a=b", referenced by `dataform.projectConfig.vars.someValue`.',
    # )
    # parser.add_argument(
    #     "-t",
    #     "--timeout",
    #     type=str,
    #     help='Duration to allow project compilation to complete. Examples: "1s", "10m", etc.',
    # )
    # parser.add_argument(
    #     "-p",
    #     "--job-prefix",
    #     type=str,
    #     help="Adds an additional prefix in the form of `dataform-${jobPrefix}-`. Has no effect on warehouses other than BigQuery.",
    # )

    return parser


def print_compiled_graph(compiled_graph: CompiledGraph, verbose: bool):
    """
    Prints a compiled graph to stdout.

    Args:
        graph: The compiled graph.
        verbose: If true, prints the compiled graph in JSON format.
    """
    if verbose:
        print(json.dumps(json_format.MessageToDict(compiled_graph), indent=4))
        return

    action_count = (
        len(compiled_graph.tables)
        + len(compiled_graph.assertions)
        + len(compiled_graph.operations)
    )
    print(f"Compiled {action_count} processable actions.\n")

    def print_actions(action_name: str, actions: any):
        if not len(actions):
            return
        print(len(actions), action_name + ":")
        for action in actions:
            target_representation = target_to_target_representation(action.target)
            enum_type_representation = (
                f" [{action.enum_type}]" if hasattr(action, "enum_type") else ""
            )
            disabled_representation = (
                f" [{action.disabled}]" if hasattr(action, "disabled") else ""
            )
            print(
                f"{target_representation}{enum_type_representation}{disabled_representation}"
            )
        print()

    print_actions("tables", compiled_graph.tables)
    print_actions("assertions", compiled_graph.assertions)
    print_actions("operations", compiled_graph.operations)
    print_actions("declarations", compiled_graph.declarations)


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    if args.activity == "compile":
        start = time.time()

        if not args.json:
            print("Compiling...")
        compile_config = CompileConfig()
        compile_config.project_dir = str(args.project_dir)

        session = Session(compile_config)
        session.load_includes()
        session.load_actions()
        compiled_graph = session.compile()

        total_compilation_time = time.time() - start

        print_compiled_graph(compiled_graph, args.json)

        if not args.json:
            print("Compiled graph size:", compiled_graph.ByteSize())
            print(f"Total compilation time: {total_compilation_time}s")
