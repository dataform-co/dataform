load("@df//testing:index.bzl", "ts_test_suite")
load("//testing:build_test.bzl", "build_test")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "protos",
    srcs = glob(
        ["*.ts"],
        exclude = [
            "*_test.ts",
            "*.spec.ts",
        ],
    ),
    esm = True,
    deps = [
        "//:node_modules/@types/node",
        "//:node_modules/protobufjs",
        "//common/strings",
        "//protos:ts",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["*_test.ts"]),
    deps = [
        ":protos",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
        "//protos:ts",
        "@df//testing",
    ],
)

build_test(
    name = "build_test",
    deps = [":protos"],
)
