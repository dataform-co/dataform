package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "api",
    srcs = glob(
        ["**/*.ts"],
        exclude = ["utils/**/*.*"],
    ),
    deps = [
        "//cli/api/utils",
        "//common:current_version",
        "//common/errors",
        "//common/flags",
        "//common/promises",
        "//common/protos",
        "//common/strings",
        "//core",
        "//core/compilation_sql",
        "//protos:ts",
        "//cli/vm:compile_loader",
        "//sqlx:lexer",
        "//cli/vm:vm",
        "@npm//@google-cloud/bigquery",
        "@npm//@types/glob",
        "@npm//@types/js-beautify",
        "@npm//@types/js-yaml",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/semver",
        "@npm//deepmerge",
        "@npm//glob",
        "@npm//js-beautify",
        "@npm//js-yaml",
        "@npm//promise-pool-executor",
        "@npm//protobufjs",
        "@npm//semver",
        "@npm//sql-formatter",
    ],
)
