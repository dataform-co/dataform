load("//testing:index.bzl", "ts_test_suite")
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
        "//cli/api/utils",
        "//cli/vm",
        "//cli/vm:compile_loader",
        "//common/errors",
        "//common/flags",
        "//common/promises",
        "//common/protos",
        "//common/strings",
        "//core",
        "//core/compilation_sql",
        "//protos:ts",
        "//sqlx:lexer",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "commands/jit/rpc_test.ts",
        "dbadapters/bigquery_test.ts",
        "execution_sql_test.ts",
        "utils_test.ts",
    ],
    data = [
        "//packages/@dataform/core:package_tar",
        "//test_credentials:bigquery.json",
    ] + glob(["goldens/**"]),
    deps = [
        ":api",
        "//:node_modules/@google-cloud/bigquery",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/ts-mockito",
        "//core",
        "//protos:ts",
        "//testing",
    ],
)
