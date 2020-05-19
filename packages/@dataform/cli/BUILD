load("@npm_bazel_typescript//:index.bzl", "ts_library")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "pkg_bundle", "pkg_json", "pkg_npm_tar")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "cli",
    srcs = glob(["*.ts"]),
    deps = [
        "//api",
        "//cli",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

nodejs_binary(
    name = "bin",
    data = [
        ":cli",
        "@npm//source-map-support",
    ],
    entry_point = ":index.ts",
    templated_args = ["--node_options=--require=source-map-support/register"],
)

externals = [
    "@google-cloud/bigquery",
    "protobufjs",
    "tarjan-graph",
    "semver",
    "moo",
    "chokidar",
    "analytics-node",
    "uuid",
    "readline-sync",
    "untildify",
    "yargs",
    "glob",
    "cron-parser",
    "get-port",
    "glob",
    "js-beautify",
    "lodash",
    "mssql",
    "object-hash",
    "pg",
    "pg-cursor",
    "pretty-ms",
    "promise-pool-executor",
    "snowflake-sdk",
    "sql-formatter",
    "ssh2",
    "vm2",
]

pkg_json(
    name = "json",
    package_name = "@dataform/cli",
    description = "Dataform command line interface.",
    external_deps = externals,
    layers = [
        "//:package.json",
        "//packages/@dataform:package.layer.json",
        "//packages/@dataform/cli:package.layer.json",
    ],
    main = "bundle.js",
    version = DF_VERSION,
)

pkg_bundle(
    name = "bundle",
    args = ["--banner='#!/usr/bin/env node'"],
    entry_point = "index.ts",
    externals = externals,
    deps = [
        ":cli",
    ],
)

pkg_bundle(
    name = "worker_bundle",
    args = ["--banner='#!/usr/bin/env node'"],
    entry_point = "worker.ts",
    externals = externals,
    deps = [
        ":cli",
    ],
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":bundle",
        ":package.json",
        ":worker_bundle",
    ],
)
