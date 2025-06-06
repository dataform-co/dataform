load("//tools:ts_library.bzl", "ts_library")
load("//tools/common:copy.bzl", "copy_file")
load("//:version.bzl", "DF_VERSION")
load("//testing:index.bzl", "ts_test_suite")
load("//tools:node_modules.bzl", "node_modules")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "cli",
    srcs = [
        "console.ts",
        "credentials.ts",
        "index.ts",
        "util.ts",
        "yargswrapper.ts",
    ],
    deps = [
        "//cli/api",
        "//cli/vm",
        "//core",
        "//protos:ts",
        "//sqlx:format",
        "@npm//@types/glob",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/readline-sync",
        "@npm//@types/yargs",
        "@npm//chokidar",
        "@npm//glob",
        "@npm//parse-duration",
        "@npm//readline-sync",
        "@npm//untildify",
        "@npm//yargs",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/cli:package_tar",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "index_test.ts",
        "util_test.ts"
    ],
    data = [
        ":node_modules",
        "//packages/@dataform/core:package_tar",
        "//test_credentials:bigquery.json",
        "@nodejs//:node",
        "@nodejs//:npm",
    ],
    deps = [
        "//cli",
        "//core",
        "//protos:ts",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/js-yaml",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
        "@npm//js-yaml",
    ],
)

copy_file(
    name = "readme",
    src = "//:readme.md",
    out = "readme.md",
)
