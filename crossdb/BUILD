package(default_visibility = ["//visibility:public"])

load("@build_bazel_rules_typescript//:defs.bzl", "ts_library")

ts_library(
    name = "crossdb",
    srcs = glob(["**/*.ts"]),
    module_name = "@dataform/crossdb",
    deps = [
        "//core",
        "@npm//@types/node",
    ],
)

load("@build_bazel_rules_nodejs//:defs.bzl", "npm_package")

npm_package(
    name = "package",
    srcs = ["package.json"],
    deps = [":crossdb"],
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
