load("//tools:ts_library.bzl", "ts_library", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

exports_files(["build_test.sh"])

ts_library(
    name = "testing",
    srcs = [
        "child_process.ts",
        "fixtures.ts",
        "hook.ts",
        "index.ts",
        "runner.ts",
        "suite.ts",
        "test.ts",
    ],
    validate = False,
    deps = [
        "//:node_modules/@types/diff",
        "//:node_modules/@types/json-stable-stringify",
        "//:node_modules/@types/node",
        "//:node_modules/@types/rimraf",
        "//:node_modules/chalk",
        "//:node_modules/diff",
        "//:node_modules/json-stable-stringify",
        "//:node_modules/rimraf",
    ],
)

ts_test_suite(
    srcs = ["index_test.ts"],
    deps = [
        ":testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/diff",
        "//:node_modules/@types/json-stable-stringify",
        "//:node_modules/chai",
        "//:node_modules/diff",
        "//:node_modules/json-stable-stringify",
    ],
)
