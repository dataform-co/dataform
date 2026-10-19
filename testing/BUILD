load("@aspect_rules_js//js:defs.bzl", "js_library")
load("//testing:index.bzl", "ts_test_suite")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

exports_files([
    "build_test.sh",
])

js_library(
    name = "resolver-patch",
    srcs = ["resolver-patch.js"],
    visibility = ["//visibility:public"],
)

ts_library(
    name = "testing",
    srcs = [
        "bigquery.ts",
        "child_process.ts",
        "credentials.ts",
        "fixtures.ts",
        "hook.ts",
        "index.ts",
        "runner.ts",
        "suite.ts",
        "test.ts",
    ],
    deps = [
        "//:node_modules/@types/diff",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/js-yaml",
        "//:node_modules/@types/json-stable-stringify",
        "//:node_modules/@types/node",
        "//:node_modules/chalk",
        "//:node_modules/diff",
        "//:node_modules/fs-extra",
        "//:node_modules/js-yaml",
        "//:node_modules/json-stable-stringify",
        "//protos:ts",
    ],
)

ts_library(
    name = "run_core",
    srcs = [
        "run_core.ts",
    ],
    deps = [
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/node",
        "//:node_modules/fs-extra",
        "//common/protos",
        "//common/vm:vm_runner",
        "//core",
        "//protos:ts",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = [
        "credentials_test.ts",
        "index_test.ts",
    ],
    deps = [
        ":testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/diff",
        "//:node_modules/@types/fs-extra",
        "//:node_modules/@types/json-stable-stringify",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//:node_modules/diff",
        "//:node_modules/fs-extra",
        "//:node_modules/json-stable-stringify",
    ],
)
