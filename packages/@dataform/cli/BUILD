load("//tools:ts_library.bzl", "ts_library")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "LICENSE_HEADER", "add_license_header_to_file", "pkg_bundle", "pkg_json", "pkg_npm_tar")
load("@build_bazel_rules_nodejs//:index.bzl", "nodejs_binary")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "cli",
    srcs = glob(["*.ts"]),
    deps = [
        "//cli",
        "//cli/vm:compile_loader",
    ],
)

nodejs_binary(
    name = "bin",
    data = [
        ":cli",
        "@npm//source-map-support",
    ],
    entry_point = ":index.ts",
    templated_args = [
        "--node_options=--require=source-map-support/register",
        "--bazel_patch_module_resolver",
    ],
)

externals = [
    "@google-cloud/bigquery",
    "chokidar",
    "deepmerge",
    "fs-extra",
    "glob",
    "google-sql-syntax-ts",
    "js-beautify",
    "js-yaml",
    "moo",
    "object-sizeof",
    "parse-duration",
    "promise-pool-executor",
    "protobufjs",
    "readline-sync",
    "semver",
    "tarjan-graph",
    "tmp",
    "typeid-js",
    "untildify",
    "vm2",
    "yargs",
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
    allow_node_builtins = True,
    args = ["--banner='#!/usr/bin/env node\n" + LICENSE_HEADER + "'"],
    entry_point = "index.ts",
    externals = externals,
    deps = [
        ":cli",
    ],
)

pkg_bundle(
    name = "worker_bundle",
    allow_node_builtins = True,
    args = ["--banner='#!/usr/bin/env node\n" + LICENSE_HEADER + "'"],
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
