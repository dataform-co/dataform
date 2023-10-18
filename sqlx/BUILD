package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "sqlx",
    srcs = glob(
        ["**/*.ts"],
        exclude = ["format.ts"],
    ),
    deps = [
        "//:modules-fix",
        "@npm//@types/moo",
        "@npm//moo",
    ],
)

ts_library(
    name = "format",
    srcs = glob(
        ["format.ts"],
    ),
    deps = [
        "//:modules-fix",
        "//common/errors",
        "//sqlx:sqlx",
        "@npm//@types/js-beautify",
        "@npm//@types/node",
        "@npm//js-beautify",
        "@npm//sql-formatter",
        "@npm//typeid-js",
    ],
)
