load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//sqlx",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
