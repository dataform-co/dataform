load("//tools:ts_library.bzl", "ts_library")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "pkg_json", "pkg_npm_tar", "add_license_header_to_file")
load("//tools/common:copy.bzl", "copy_file")

package(default_visibility = ["//visibility:public"])

ts_library(
    name = "core",
    srcs = glob(["*.ts"]),
    deps = [
        "//core",
    ],
)

load("@npm//webpack:index.bzl", "webpack")

webpack(
    name = "bundler",
    data = [
        ":webpack.config.js",
        ":core",
        "@npm//webpack-cli",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")

npm_package_bin(
    name = "bundle",
    outs = [
        "bundle_no_license.js",
    ],
    args = [
        "--config=$(location webpack.config.js)",
        "--output=$(location bundle_no_license.js)",
        "--mode=production"
    ],
    data = [
        ":webpack.config.js",
    ],
    tool = ":bundler",
)

pkg_json(
    name = "json",
    package_name = "@dataform/core",
    description = "Dataform core API.",
    external_deps = [],
    layers = [
        "//:package.json",
        "//packages/@dataform:package.layer.json",
    ],
    main = "bundle.js.no_license",
    version = DF_VERSION,
)

add_license_header_to_file(
    name="core_proto_with_license",
    from_file="//protos:core.proto",
    to_file=":core.proto",
)

add_license_header_to_file(
    name="bundle_with_license",
    from_file=":bundle",
    to_file=":bundle.js",
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":bundle_with_license",
        ":package.json",
        ":core_proto_with_license",
    ],
)
