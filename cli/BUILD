load("//tools:ts_library.bzl", "ts_library", "ts_test_suite")
load("//tools/common:copy.bzl", "copy_file")
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
        "//:node_modules/@types/glob",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/@types/readline-sync",
        "//:node_modules/@types/yargs",
        "//:node_modules/chokidar",
        "//:node_modules/glob",
        "//:node_modules/parse-duration",
        "//:node_modules/readline-sync",
        "//:node_modules/untildify",
        "//:node_modules/yargs",
    ],
)

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/cli:package_tar",
    ],
)

ts_test_suite(
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
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
    ],
)

copy_file(
    name = "readme",
    src = "//:readme.md",
    out = "readme.md",
)
