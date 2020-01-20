load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//core",
        "//protos",
        "//testing",
        "//tests/utils",
        "@npm//@types/chai",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
