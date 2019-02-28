package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "core",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/core",
    deps = [
        "//protos",
        "@npm//@types/node",
        "@npm//protobufjs",
    ],
)

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package")

npm_package(
    name = "package",
    srcs = ["package.json"],
    deps = [":core"],
)

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

pkg_tar(
    name = "package_tar",
    extension = "tgz",
    srcs = [
        ":package",
    ],
)
