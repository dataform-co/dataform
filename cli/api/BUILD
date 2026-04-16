load("//testing:index.bzl", "ts_test_suite")
load("//tools:node_modules.bzl", "node_modules")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "api",
    srcs = glob(
        ["**/*.ts"],
        exclude = [
            "utils/**/*.*",
            "**/*_test.ts",
        ],
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
        "@npm//@google-cloud/bigquery",
        "@npm//@types/fs-extra",
        "@npm//@types/glob",
        "@npm//@types/js-beautify",
        "@npm//@types/js-yaml",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/semver",
        "@npm//@types/tmp",
        "@npm//deepmerge",
        "@npm//fs-extra",
        "@npm//glob",
        "@npm//google-sql-syntax-ts",
        "@npm//js-beautify",
        "@npm//js-yaml",
        "@npm//promise-pool-executor",
        "@npm//protobufjs",
        "@npm//semver",
        "@npm//tmp",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/core:package_tar",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "utils_test.ts",
        "execution_sql_test.ts",
    ],
    data = [
        ":node_modules",
        "//packages/@dataform/core:package_tar",
        "//test_credentials:bigquery.json",
        "@nodejs//:node",
        "@nodejs//:npm",
    ],
    deps = [
        "//cli/api",
        "//core",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/js-yaml",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
        "@npm//js-yaml",
        "@npm//ts-mockito",
    ],
)
