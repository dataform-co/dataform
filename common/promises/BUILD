load("@df//testing:index.bzl", "ts_test_suite")
load("@df//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "promises",
    srcs = glob(
        ["*.ts"],
        exclude = ["*.spec.ts"],
    ),
    esm = True,
    deps = [
        "//:node_modules/@types/node",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["*.spec.ts"]),
    deps = [
        ":promises",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "@df//testing",
    ],
)
