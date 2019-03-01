package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "cli",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/cli",
    deps = [
        "//api",
        "//core",
        "//protos",
        "@npm//@types/long",
        "@npm//@types/node",
        "@npm//@types/yargs",
        "@npm//chokidar",
        "@npm//yargs",
    ],
)

load("@build_bazel_rules_nodejs//:defs.bzl", "nodejs_binary")

nodejs_binary(
    name = "bin",
    bootstrap = ["source-map-support"],
    data = [
        ":cli",
        "@npm//source-map-support",
    ],
    entry_point = "df/cli/index.js",
)

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package")

npm_package(
    name = "package",
    srcs = ["package.json"],
    deps = [":cli"],
)

load("@bazel_tools//tools/build_defs/pkg:pkg.bzl", "pkg_tar")

pkg_tar(
    name = "package_tar",
<<<<<<< HEAD
    extension = "tgz",
    srcs = [
        ":package",
    ],
=======
    srcs = [":package"],
    outs = ["package.tgz"],
    cmd = "tar -cvzf $(location package.tgz) -C $(location :package)/.. --dereference package",
>>>>>>> master
)
