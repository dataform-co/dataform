load("//tools:ts_library.bzl", "ts_library")
load("//:version.bzl", "DF_VERSION")
load("//packages:index.bzl", "pkg_bundle", "pkg_bundle_dts", "pkg_json", "pkg_npm_tar")
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
        "bundle.js",
    ],
    args = [
        "--config=$(location webpack.config.js)",
        "--output=$(location bundle.js)",
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
    main = "bundle.js",
    version = DF_VERSION,
)

copy_file(
    name = "core_proto",
    src = "//protos:core.proto",
    out = "core.proto",
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
        ":core",
    ],
)

pkg_npm_tar(
    name = "package",
    deps = [
        ":bundle",
        ":bundle.d",
        ":package.json",
        ":core.proto",
    ],
)
