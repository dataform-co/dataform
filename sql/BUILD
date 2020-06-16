load("//tools:ts_library.bzl", "ts_library")
load("//testing:index.bzl", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "sql",
    srcs = glob(
        [
            "**/*.ts",
        ],
        exclude = ["**/*.spec.ts"],
    ),
    deps = [
        "//:modules-fix",
        "@npm//@types/long",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.spec.ts"]),
    data = [
        "//test_credentials:bigquery.json",
        "//test_credentials:snowflake.json",
        "//test_credentials:redshift.json",
        "@npm//source-map-support",
    ],
    templated_args = ["--node_options=--require=source-map-support/register"],
    deps = [
        ":sql",
        "//api",
        "//testing",
        "@npm//@types/chai",
        "@npm//chai",
    ],
)
