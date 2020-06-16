package(default_visibility = ["//visibility:public"])

load("//tools:css_typings.bzl", "css_typings")

filegroup(
    name = "assets",
    srcs = glob([
        "**/*.html",
        "public/**/*.*",
    ]),
)

load("@io_bazel_rules_sass//:defs.bzl", "multi_sass_binary")

multi_sass_binary(
    name = "sass",
    srcs = glob(["**/*.scss"]),
    output_style = "expanded",
    sourcemap = False,
)

css_typings(
    name = "css_typings",
    srcs = [
        ":sass",
    ],
)

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "app",
    srcs = glob(
        [
            "**/*.ts",
            "**/*.tsx",
        ],
        exclude = ["webpack.ts"],
    ),
    data = [":sass"],
    deps = [
        ":css_typings",
        "//protos:ts",
        "//tools/protobufjs:grpc_web_rpc_impl",
        "@npm//@blueprintjs/core",
        "@npm//@types/node",
        "@npm//@types/react",
        "@npm//@types/react-dom",
        "@npm//@types/react-router",
        "@npm//protobufjs",
        "@npm//react",
        "@npm//react-dom",
        "@npm//react-router",
    ],
)

load("@npm//webpack-dev-server:index.bzl", "webpack_dev_server")

webpack_dev_server(
    name = "devserver",
    args = [
        "--config=app/webpack.config.js",
        "--output=./app.bundle.js",
        "--history-api-fallback",
    ],
    data = [
        ":app",
        ":assets",
        ":sass",
        ":webpack.config.js",
        "@npm//webpack-cli",
    ],
    tags = [
        # "ibazel_notify_changes",
    ],
)

load("@npm//webpack:index.bzl", "webpack")

webpack(
    name = "bundler",
    data = [
        ":app",
        ":assets",
        ":sass",
        ":webpack.config.js",
        "@npm//webpack-cli",
    ],
)

load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")

npm_package_bin(
    name = "bundle",
    outs = [
        "app.bundle.js",
    ],
    args = [
        "--config=app/webpack.config.js",
        "--output=$(location app.bundle.js)",
    ],
    data = [
        ":webpack.config.js",
        "@npm//css-loader",
        "@npm//style-loader",
        "@npm//umd-compat-loader",
    ],
    tool = ":bundler",
)
