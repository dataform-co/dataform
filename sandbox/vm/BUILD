package(default_visibility = ["//visibility:public"])

load("//tools:ts_library.bzl", "ts_library")

ts_library(
    name = "vm",
    srcs = glob(
        ["**/*.ts"],
        exclude = ["utils/**/*.*"],
    ),
    deps = [
        "//common/protos",
        "//core",
        "//protos:ts",
        "@npm//@types/glob",
        "@npm//@types/node",
        "@npm//glob",
        "@npm//vm2",
    ],
)

load("@npm//webpack:index.bzl", "webpack")

webpack(
    name = "bundler",
    data = [
        ":vm",
        ":webpack.config.js",
        "@npm//webpack-cli",
    ],
)

# # Copy the vm2 package to the worker bundle, as it can't be bundled by webpack
# # directly.
# genrule(
#     name = "copy_vm2",
#     srcs = [":package.json"],
#     outs = ["worker_bundle.js/package.json"],
#     cmd = "cp $< $@",
#     output_to_bindir = 1,
# )

load("@build_bazel_rules_nodejs//:index.bzl", "npm_package_bin")

npm_package_bin(
    name = "worker_bundle",
    outs = [
        "worker_bundle.js",
    ],
    args = [
        "--config=sandbox/vm/webpack.config.js",
        "--output-path=$(location worker_bundle.js)",
    ],
    data = [
        ":webpack.config.js",
    ],
    tool = ":bundler",
)
