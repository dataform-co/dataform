load("//testing:index.bzl", "ts_test_suite")
load("//tools:node_modules.bzl", "node_modules")

node_modules(
    name = "node_modules",
    deps = [
        "//packages/@dataform/cli:package_tar",
    ],
)

ts_test_suite(
    name = "tests",
    srcs = glob(["**/*.spec.ts"]),
    data = [
        ":node_modules",
        "//packages/@dataform/core:package_tar",
        "//test_credentials:redshift.json",
        "@nodejs//:node",
        "@nodejs//:npm",
    ],
    deps = [
        "//core",
        "//testing",
        "@npm//@types/chai",
        "@npm//@types/fs-extra",
        "@npm//@types/node",
        "@npm//chai",
        "@npm//fs-extra",
    ],
)
