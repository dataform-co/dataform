package(default_visibility=["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "core",
    module_name = "@dataform/core",
    srcs = glob(["**/*.ts"]),
    deps = [
      "@npm//@types/node",
      "@npm//protobufjs",
      "//protos",
    ]
)
