package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

ts_library(
    name = "bootstrap",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/packages/bootstrap",
    deps = [
        "@npm//alea",
    ],
)

# We want to use these functions in UDFs, rollup is like webpack and helps us create self contained libraries.
load("@build_bazel_rules_nodejs//:defs.bzl", "rollup_bundle")

rollup_bundle(
        name = "rollup",
        entry_point = ":index.ts",
        deps = [
            ":bootstrap",
            "@npm//alea",
        ],
    )

load("//tools/npm:package.bzl", "dataform_npm_package")

dataform_npm_package(
    name = "package",
    package_layers = [
        "//:common.package.json",
        "bootstrap.package.json",
    ],
    deps = [":bootstrap"],
)
