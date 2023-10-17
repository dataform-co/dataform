package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "api",
    srcs = glob(
        ["**/*.ts"],
        exclude = ["utils/**/*.*"],
    ),
    deps = [
        "//api/utils",
        "//common/errors",
        "//common/flags",
        "//common/promises",
        "//common/protos",
        "//common/strings",
        "//core",
        "//protos:ts",
        "//sandbox/vm:compile_loader",
        "//sqlx",
        "//sandbox/vm:vm",
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
