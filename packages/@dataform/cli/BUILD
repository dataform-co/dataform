load("//tools:ts_library.bzl", "ts_library")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "pkg_bundle", "pkg_json", "pkg_npm_tar", "add_license_header_to_file")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "cli",
    srcs = glob(["*.ts"]),
    deps = [
        "//cli",
        "//cli/vm:compile_loader",
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
    "deepmerge",
    "glob",
    "js-beautify",
    "object-sizeof",
    "parse-duration",
    "promise-pool-executor",
    "sql-formatter",
    "typeid-js",
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
    name = "bundle_no_license",
    args = ["--banner='#!/usr/bin/env node'"],
    entry_point = "index.ts",
    allow_node_builtins = True,
    externals = externals,
    deps = [
        ":cli",
    ],
)

pkg_bundle(
    name = "worker_bundle_no_license",
    args = ["--banner='#!/usr/bin/env node'"],
    entry_point = "worker.ts",
    allow_node_builtins = True,
    externals = externals,
    deps = [
        ":cli",
    ],
)

add_license_header_to_file(
    name="bundle_with_license",
    from_file=":bundle_no_license",
    to_file=":bundle.js",
)

add_license_header_to_file(
    name="worker_bundle_with_license",
    from_file=":worker_bundle_no_license",
    to_file=":worker_bundle.js",
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":bundle_with_license",
        ":package.json",
        ":worker_bundle_with_license",
    ],
)
