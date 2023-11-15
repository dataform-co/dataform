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
        "//protos:ts",
<<<<<<< HEAD:cli/api/BUILD
        "//cli/vm:compile_loader",
        "//sqlx:lexer",
        "//cli/vm:vm",
=======
        "//sandbox/vm:compile_loader",
        "//sqlx:lexer",
        "//sandbox/vm:vm",
>>>>>>> main:api/BUILD
        "@npm//@google-cloud/bigquery",
        "@npm//@types/glob",
        "@npm//@types/js-beautify",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/semver",
        "@npm//deepmerge",
        "@npm//glob",
        "@npm//js-beautify",
        "@npm//promise-pool-executor",
        "@npm//protobufjs",
        "@npm//semver",
        "@npm//sql-formatter",
    ],
)
