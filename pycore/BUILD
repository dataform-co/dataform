load("@rules_python//python:defs.bzl", "py_binary")
load("@pip//:requirements.bzl", "requirement")

py_library(
    name = "pycore_lib",
    srcs = glob(["*.py"]),
    deps = [
        "//protos:dataform_py_proto",
        requirement("tarjan"),
    ],
)

py_binary(
    name = "main",
    srcs = [
        "main.py",
    ],
    deps = [
        ":pycore_lib",
    ],
)
