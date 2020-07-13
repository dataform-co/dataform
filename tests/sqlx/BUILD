load("//testing:index.bzl", "ts_test_suite")

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.ts"]),
    data = [
        "//examples/formatter:files",
        "//examples/formatter:node_modules",
    ],
    deps = [
        "//sqlx",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/node",
        "@npm//chai",
    ],
)
