load("@npm_bazel_typescript//:index.bzl", "ts_library")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "pkg_json", "pkg_npm_tar")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "assertion_utils",
    srcs = glob(["**/*.ts"]),
    deps = [
        "@npm//@types/node",
    ],
)

pkg_json(
    name = "json",
    package_name = "@dataform/assertion_utils",
    description = "Assertion utilities for Dataform.",
    external_deps = [],
    layers = [
        "//:package.json",
        "//packages/@dataform:package.layer.json",
    ],
    main = "index.js",
    types = "index.d.js",
    version = DF_VERSION,
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":assertion_utils",
        ":package.json",
    ],
)
