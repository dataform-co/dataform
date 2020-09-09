package(default_visibility = ["//visibility:public"])

load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(
        ["*.ts"],
        # SQL Data warehouse integration tests are currently disabled while we sort out a new testing server.
        exclude = ["sqldatawarehouse.spec.ts"],
    ),
    data = [
        "//test_credentials:bigquery.json",
        "//test_credentials:redshift.json",
        "//test_credentials:snowflake.json",
        "//test_credentials:sqldatawarehouse.json",
        "//tests/integration/bigquery_project:files",
        "//tests/integration/bigquery_project:node_modules",
        "//tests/integration/postgres_project:files",
        "//tests/integration/postgres_project:node_modules",
        "//tests/integration/redshift_project:files",
        "//tests/integration/redshift_project:node_modules",
        "//tests/integration/snowflake_project:files",
        "//tests/integration/snowflake_project:node_modules",
        "//tests/integration/sqldatawarehouse_project:files",
        "//tests/integration/sqldatawarehouse_project:node_modules",
    ],
    deps = [
        "//api",
        "//api/utils",
        "//common/promises",
        "//core",
        "//protos:ts",
        "//testing",
        "//tests/utils",
        "//tools/postgres",
        "//tools/presto",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
