load("//tools:ts_library.bzl", "ts_library")
load("//testing:index.bzl", "ts_test_suite")

package(default_visibility = ["//visibility:public"])

exports_files(["build_test.sh"])

ts_library(
    name = "testing",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
    deps = [
        "//:node_modules/@types/diff",
        "//:node_modules/@types/json-stable-stringify",
        "//:node_modules/@types/node",
        "//:node_modules/chalk",
        "//:node_modules/diff",
        "//:node_modules/json-stable-stringify",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["*.spec.ts"]),
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
