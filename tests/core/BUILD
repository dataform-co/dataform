load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.ts"]),
    data = glob(["*.js.test"]),
    deps = [
        "//core",
        "//protos:ts",
        "//testing",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
    ],
)
