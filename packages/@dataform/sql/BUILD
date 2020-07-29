load("//tools:ts_library.bzl", "ts_library")
load("//packages:index.bzl", "pkg_bundle", "pkg_bundle_dts", "pkg_json", "pkg_npm_tar")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "sql",
    srcs = glob(["**/*.ts"]),
    deps = [
        "//sql",
        "@npm//@types/node",
    ],
)

pkg_json(
    name = "json",
    package_name = "@dataform/sql",
    description = "General purpose SQL helper functions for building queries in JS across dialects.",
    layers = [
        "//:package.json",
        "//packages/@dataform:package.layer.json",
    ],
    main = "bundle.js",
    types = "bundle.d.ts",
    version = "0.1.0",
)

pkg_bundle(
    name = "bundle",
    entry_point = "index.ts",
    externals = [],
    deps = [
        ":sql",
    ],
)

pkg_bundle_dts(
    name = "bundle.d",
    entry_point = "index.ts",
    externals = [],
    deps = [
        ":sql",
    ],
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":bundle",
        ":bundle.d",
        ":package.json",
    ],
)
