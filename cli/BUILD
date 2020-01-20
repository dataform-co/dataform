package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "cli",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/cli",
    deps = [
        "//api",
        "//core",
        "//protos",
        "@npm//@types/glob",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/readline-sync",
        "@npm//@types/yargs",
        "@npm//chokidar",
        "@npm//glob",
        "@npm//readline-sync",
        "@npm//untildify",
        "@npm//yargs",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

nodejs_binary(
    name = "bin",
    templated_args = ["--node_options=--require=source-map-support/register"],
    data = [
        ":cli",
        "@npm//source-map-support",
    ],
    entry_point = ":index.ts",
)

load("//tools/common:copy.bzl", "copy_file")

copy_file(
    name = "readme",
    src = "//:readme.md",
    out = "readme.md",
)

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = [
        "//:common.package.json",
        "cli.package.json",
    ],
    deps = [
        ":cli",
        ":readme",
    ],
)
