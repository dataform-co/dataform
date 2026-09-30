load("@aspect_rules_webpack//webpack:defs.bzl", "webpack_bundle")
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

webpack_bundle(
    name = "bundle_no_license",
    args = [
        "--mode=production",
    ],
    data = [
        ":webpack.config.js",
        "//:node_modules/webpack-cli",
    ],
    entry_point = "index.js",
    node_modules = "//:node_modules",
    webpack_config = ":webpack.config.js",
    deps = [
        ":sample",
        "//core",
    ],
)

pkg_json(
    name = "json",
    package_name = "@dataform/sample-extension",
    description = "Sample extension.",
    external_deps = [],
    layers = [
        "//:package_json",
        "//packages/@dataform:package_layer",
    ],
    main = "bundle.js",
    version = DF_VERSION,
)

add_license_header_to_file(
    name = "bundle_with_license",
    from_file = ":bundle_no_license",
    to_file = "bundle.js",
)

pkg_bundle_dts(
    name = "bundle.d",
    entry_point = "index.d.ts",
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
