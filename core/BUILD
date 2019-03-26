package(default_visibility = ["//visibility:public"])

load("@npm_bazel_typescript//:index.bzl", "ts_library")

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

genrule(
    name = "package_tar",
    srcs = [":package"],
    outs = ["package.tgz"],
    cmd = "tar -cvzf $(location package.tgz) -C $(location :package)/.. --dereference package",
)
