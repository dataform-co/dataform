load("//tools:ts_library.bzl", "ts_library")
load("//tools/common:copy.bzl", "copy_file")
load("//testing:index.bzl", "ts_test_suite")

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


ts_library(
    name = "index_test_base",
    srcs = ["index_test_base.ts"],
    deps = [
        "//core",
        "//protos:ts",
        "//testing",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/node",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "index_help_test.ts",
        "index_init_test.ts",
        "index_project_test.ts",
        "index_compile_test.ts",
        "index_run_e2e_test.ts",
        "util_test.ts"
    ],
    data = [
        "//packages/@dataform/cli:bundle",
        "//packages/@dataform/cli:json",
        "//packages/@dataform/cli:worker_bundle",
        "//packages/@dataform/core:package_tar",
        "//test_credentials:bigquery.json",
        "@nodejs//:node",
        "@nodejs//:npm",
        "@nodejs//:node_files",
        "@nodejs//:npm_files",
    ],
    no_copy_to_bin = [
        "@nodejs//:node",
        "@nodejs//:npm",
        "@nodejs//:node_files",
        "@nodejs//:npm_files",
    ],
    deps = [
        ":index_test_base",
        "//cli",
        "//cli/api",
        "//core",
        "//protos:ts",
        "//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/ts-mockito",
    ],
)

copy_file(
    name = "readme",
    src = "//:readme.md",
    out = "readme.md",
)
