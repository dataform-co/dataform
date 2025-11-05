load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")
load("@npm//webpack:index.bzl", "webpack")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "add_license_header_to_file", "pkg_bundle_dts", "pkg_json", "pkg_npm_tar")
load("//tools:ts_library.bzl", "ts_library")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "sample",
    srcs = glob(["*.ts"]),
    deps = [
        "//core",
        "//protos:ts",
    ],
)

webpack(
    name = "bundler",
    data = [
        ":sample",
        ":webpack.config.js",
        "@npm//webpack-cli",
    ],
)

npm_package_bin(
    name = "bundle",
    outs = [
        "bundle_no_license.js",
    ],
    args = [
        "--config=$(location webpack.config.js)",
        "--output-path=$(RULEDIR)",
        "--output-filename=bundle_no_license.js",
        "--mode=production",
    ],
    data = [
        ":webpack.config.js",
    ],
    tool = ":bundler",
)

pkg_json(
    name = "json",
    package_name = "@dataform/sample-extension",
    description = "Sample extension.",
    external_deps = [],
    layers = [
        "//:package.json",
        "//packages/@dataform:package.layer.json",
    ],
    main = "bundle.js",
    version = DF_VERSION,
)

add_license_header_to_file(
    name = "bundle_with_license",
    from_file = "bundle",
    to_file = "bundle.js",
)

pkg_bundle_dts(
    name = "bundle.d",
    entry_point = "index.ts",
    externals = [
        "protobufjs",
        "tarjan-graph",
        "semver",
        "moo",
        "long",
    ],
    deps = [
        ":sample",
    ],
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":bundle.d",
        ":bundle_with_license",
        ":package.json",
        "//:version.bzl",
    ],
)
