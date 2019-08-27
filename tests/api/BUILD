package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "lib",
    srcs = glob(["**/*.ts"]),
    data = [
        "//api:vm/compile",
    ],
    deps = [
        "//api",
        "//core",
        "//protos",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/mocha",
        "@npm//@types/node",
        "@npm//@types/stack-trace",
        "@npm//rimraf",
        "@npm//stack-trace",
        "@npm//ts-mockito",
    ],
)

load("//tools/mocha:mocha_node_test.bzl", "mocha_node_test")

mocha_node_test(
    name = "api",
    srcs = [
        ":lib",
    ],
    data = [
        "//examples/common_v1:files",
        "//examples/common_v1:node_modules",
        "//examples/bigquery_backwards_compatibility:files",
        "//examples/bigquery_backwards_compatibility:node_modules",
        "//examples/bigquery_language_v2:files",
        "//examples/bigquery_language_v2:node_modules",
        "//examples/bigquery_with_errors:files",
        "//examples/bigquery_with_errors:node_modules",
        "//examples/init",
        "//examples/init:node_modules",
        "//examples/redshift_never_finishes_compiling:files",
        "//examples/redshift_never_finishes_compiling:node_modules",
        "//examples/redshift_operations:files",
        "//examples/redshift_operations:node_modules",
        "//examples/dataform_json_checks/invalid_warehouse:files",
        "//examples/dataform_json_checks/invalid_warehouse:node_modules",
        "//examples/dataform_json_checks/missing_warehouse:files",
        "//examples/dataform_json_checks/missing_warehouse:node_modules",
        "//examples/dataform_json_checks/invalid_defaultschema:files",
        "//examples/dataform_json_checks/invalid_defaultschema:node_modules",
        "//examples/dataform_json_checks/valid_dataform_json:files",
        "//examples/dataform_json_checks/valid_dataform_json:node_modules",
        "@npm//chai",
        "@npm//mocha",
    ],
)
