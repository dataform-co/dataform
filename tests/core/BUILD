load("//tools:ts_library.bzl", "ts_test_suite")

ts_test_suite(
    srcs = glob(["**/*.ts"]),
    deps = [
        "//core",
        "//protos:ts",
        "//testing",
        "//:node_modules/@types/chai",
        "//:node_modules/@types/long",
        "//:node_modules/@types/node",
        "//:node_modules/chai",
    ],
)
