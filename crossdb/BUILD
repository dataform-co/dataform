package(default_visibility=["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "crossdb",
    module_name = "@dataform/crossdb",
    srcs = glob(["**/*.ts"]),
    deps = [
      "@npm//@types/node",
      "//core",
    ]
)

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package")

npm_package(
    name = "pkg",
    srcs = ["package.json"],
    deps = [":crossdb"],
)
