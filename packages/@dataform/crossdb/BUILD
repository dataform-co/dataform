load("@npm_bazel_typescript//:index.bzl", "ts_library")
load("//packages:index.bzl", "pkg_json", "pkg_npm_tar")
load("//:version.bzl", "DF_VERSION")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "crossdb",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@types/node",
    ],
)

pkg_json(
    name = "json",
    package_name = "@dataform/crossdb",
    description = "Cross database utilities for dataform.",
    layers = [
        "//:package.json",
        "//packages/@dataform:package.layer.json",
    ],
    version = DF_VERSION,
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":crossdb",
        ":package.json",
    ],
)
