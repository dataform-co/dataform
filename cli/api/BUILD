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
        "//:node_modules/@google-cloud/bigquery",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/glob",
        "//:node_modules/@types/js-beautify",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/@types/semver",
        "//:node_modules/@types/tmp",
        "//:node_modules/deepmerge",
        "//:node_modules/fs-extra",
        "//:node_modules/glob",
        "//:node_modules/google-sql-syntax-ts",
        "//:node_modules/js-beautify",
        "//:node_modules/js-yaml",
        "//:node_modules/promise-pool-executor",
        "//:node_modules/protobufjs",
        "//:node_modules/semver",
        "//:node_modules/tmp",
    ],
)
